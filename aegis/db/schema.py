"""
db/schema.py
============
Database schema creation, migrations, and session management utilities.
"""

from datetime import datetime, timezone, timedelta
from db.connection import get_conn
from config import DB_PATH


PHT = timezone(timedelta(hours=8))


def utc_now_iso() -> str:
    return datetime.now(PHT).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def init_db() -> None:
    """
    Create all tables and indexes on a fresh database.
    Safe to call on an existing database — all statements use IF NOT EXISTS.
    WAL mode is set here and persists in the database file.
    """
    conn = get_conn()
    cur  = conn.cursor()

    conn.execute("PRAGMA journal_mode=WAL")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS enrollment_registry (
            node_id                  TEXT PRIMARY KEY,
            expected_sampling_ms     INTEGER NOT NULL DEFAULT 1000,
            delay_threshold_ms       INTEGER NOT NULL DEFAULT 3000,
            min_value                REAL,
            max_value                REAL,
            max_rate_of_change       REAL,
            density_min_interval_ms  INTEGER NOT NULL DEFAULT 250,
            status                   TEXT NOT NULL DEFAULT 'active',
            created_at               TEXT NOT NULL,
            updated_at               TEXT NOT NULL
        )""")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS session_state (
            node_id                  TEXT PRIMARY KEY,
            last_seq_num             INTEGER,
            last_sensor_value        REAL,
            last_sensor_timestamp_ms INTEGER,
            last_gateway_received_ms INTEGER,
            mqtt_status              TEXT NOT NULL DEFAULT 'CONNECTED',
            continuity_allowed       INTEGER NOT NULL DEFAULT 1,
            last_disconnect_at       TEXT,
            last_connect_at          TEXT,
            last_reset_at            TEXT,
            updated_at               TEXT NOT NULL
        )""")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS verification_log (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id                  TEXT    NOT NULL,
            seq_num                  INTEGER NOT NULL,
            timestamp_value          INTEGER NOT NULL,
            sensor_value             REAL    NOT NULL,
            mqtt_topic               TEXT    NOT NULL,
            gateway_received_ms      INTEGER NOT NULL,
            delay_ms                 INTEGER NOT NULL,
            enforcement_latency_ms   REAL    NOT NULL DEFAULT 0,
            anomaly_range_flag       INTEGER NOT NULL DEFAULT 0,
            anomaly_roc_flag         INTEGER NOT NULL DEFAULT 0,
            anomaly_density_flag     INTEGER NOT NULL DEFAULT 0,
            enforcement_status       TEXT    NOT NULL,
            prev_hash                TEXT,
            curr_hash                TEXT    NOT NULL,
            merkle_batch_id          INTEGER,
            received_at              TEXT    NOT NULL
        )""")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rejection_log (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id                  TEXT,
            seq_num                  INTEGER,
            timestamp_value          INTEGER,
            sensor_value             REAL,
            mqtt_topic               TEXT,
            gateway_received_ms      INTEGER NOT NULL,
            enforcement_latency_ms   REAL    NOT NULL DEFAULT 0,
            rejection_reason         TEXT    NOT NULL,
            details_json             TEXT,
            received_at              TEXT    NOT NULL
        )""")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id       TEXT NOT NULL,
            event_type    TEXT NOT NULL,
            event_details TEXT,
            created_at    TEXT NOT NULL,
            prev_hash     TEXT,
            curr_hash     TEXT NOT NULL
        )""")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS merkle_batches (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            start_log_id      INTEGER NOT NULL,
            end_log_id        INTEGER NOT NULL,
            record_count      INTEGER NOT NULL,
            merkle_root       TEXT    NOT NULL,
            anchor_status     TEXT    NOT NULL DEFAULT 'LOCAL_ONLY',
            anchor_ref        TEXT,
            created_at        TEXT    NOT NULL,
            interval_start_ms INTEGER,
            interval_end_ms   INTEGER,
            retry_count       INTEGER NOT NULL DEFAULT 0
        )""")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS recording_sessions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at     TEXT    NOT NULL,
            label          TEXT,
            vlog_start_id  INTEGER NOT NULL DEFAULT 0,
            rlog_start_id  INTEGER NOT NULL DEFAULT 0,
            batch_start_id INTEGER NOT NULL DEFAULT 0
        )""")

    # Migrate existing databases: add columns introduced after initial schema
    for table, col, default in [
        ("merkle_batches",   "interval_start_ms",       "NULL"),
        ("merkle_batches",   "interval_end_ms",         "NULL"),
        ("merkle_batches",   "retry_count",             "0"),
        ("verification_log", "enforcement_latency_ms",  "0"),
        ("rejection_log",    "enforcement_latency_ms",  "0"),
    ]:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} REAL NOT NULL DEFAULT {default}")
        except Exception:
            pass

    # Indexes
    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_vlog_node     ON verification_log(node_id)",
        "CREATE INDEX IF NOT EXISTS idx_vlog_received ON verification_log(received_at)",
        "CREATE INDEX IF NOT EXISTS idx_vlog_batch    ON verification_log(merkle_batch_id)",
        "CREATE INDEX IF NOT EXISTS idx_rlog_node     ON rejection_log(node_id)",
        "CREATE INDEX IF NOT EXISTS idx_rlog_reason   ON rejection_log(rejection_reason)",
        "CREATE INDEX IF NOT EXISTS idx_node_time     ON verification_log(node_id, gateway_received_ms)",
        "CREATE INDEX IF NOT EXISTS idx_node_seq      ON verification_log(node_id, seq_num)",
    ]:
        cur.execute(stmt)

    conn.commit()
    conn.close()


def seed_default_nodes() -> None:
    """
    Seeds one example node on a brand-new installation only.
    If any node already exists in enrollment_registry, this is a no-op.
    Prevents deleted nodes from reappearing on gateway restart.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM enrollment_registry")
    if cur.fetchone()[0] > 0:
        conn.close()
        return
    now = utc_now_iso()
    cur.execute("""
        INSERT INTO enrollment_registry (
            node_id, expected_sampling_ms, delay_threshold_ms, min_value, max_value,
            max_rate_of_change, density_min_interval_ms, status, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,'active',?,?)""",
        ("example_node", 5000, 3000, 0, 100, 10, 250, now, now))
    cur.execute("""
        INSERT OR IGNORE INTO session_state (
            node_id, last_seq_num, last_sensor_value, last_sensor_timestamp_ms,
            last_gateway_received_ms, mqtt_status, continuity_allowed,
            last_connect_at, updated_at
        ) VALUES (?,NULL,NULL,NULL,NULL,'CONNECTED',1,?,?)""",
        ("example_node", now, now))
    conn.commit()
    conn.close()
    print("[SEED] Fresh DB detected — seeded example_node. Enroll your real nodes via the UI.")


def get_session_bounds() -> dict:
    """
    Returns the start IDs for the current recording session.
    Dashboard queries use WHERE id > start_id so counts begin at zero.
    Returns zeros for all bounds if no session has been started.
    """
    from db.connection import get_conn
    conn = get_conn()
    cur  = conn.cursor()
    row  = cur.execute("""
        SELECT vlog_start_id, rlog_start_id, batch_start_id, started_at, label
        FROM recording_sessions ORDER BY id DESC LIMIT 1
    """).fetchone()
    conn.close()
    if row:
        return {
            "vlog_start_id":  row["vlog_start_id"],
            "rlog_start_id":  row["rlog_start_id"],
            "batch_start_id": row["batch_start_id"],
            "started_at":     row["started_at"],
            "label":          row["label"] or "",
        }
    return {"vlog_start_id": 0, "rlog_start_id": 0, "batch_start_id": 0,
            "started_at": None, "label": ""}


def start_new_recording(label: str = "") -> dict:
    """
    Marks a new recording session boundary.
    All historical data remains in the database; the dashboard simply filters
    by id > session_start_id so counters reset to zero.
    """
    from core.admission import write_admin_event
    conn     = get_conn()
    cur      = conn.cursor()
    vlog_max  = cur.execute("SELECT COALESCE(MAX(id),0) FROM verification_log").fetchone()[0]
    rlog_max  = cur.execute("SELECT COALESCE(MAX(id),0) FROM rejection_log").fetchone()[0]
    batch_max = cur.execute("SELECT COALESCE(MAX(id),0) FROM merkle_batches").fetchone()[0]
    now       = utc_now_iso()
    cur.execute("""
        INSERT INTO recording_sessions (started_at, label, vlog_start_id, rlog_start_id, batch_start_id)
        VALUES (?,?,?,?,?)""", (now, label, vlog_max, rlog_max, batch_max))
    conn.commit()
    conn.close()
    write_admin_event("SYSTEM", "NEW_RECORDING_SESSION", {
        "started_at": now, "label": label,
        "vlog_start_id": vlog_max, "rlog_start_id": rlog_max, "batch_start_id": batch_max,
    })
    print(f"[AEGIS] New recording session started — vlog>{vlog_max} rlog>{rlog_max} batch>{batch_max}")
    return get_session_bounds()


def get_export_bounds(session_id: str = None, from_dt: str = None, to_dt: str = None) -> dict:
    """
    Computes WHERE clause bounds for scoped export queries.

    session_id: when provided, scopes results to that session's id range.
                The lower bound is that session's start IDs; the upper bound
                is the next session's start IDs (or unbounded for the latest).
    from_dt / to_dt: optional ISO datetime strings for received_at filtering.
    """
    b   = get_session_bounds()
    v_lo = b["vlog_start_id"];  v_hi = None
    r_lo = b["rlog_start_id"];  r_hi = None
    b_lo = b["batch_start_id"]; b_hi = None

    if session_id:
        try:
            sid  = int(session_id)
            conn = get_conn()
            cur  = conn.cursor()
            row  = cur.execute("SELECT * FROM recording_sessions WHERE id=?", (sid,)).fetchone()
            if row:
                v_lo = row["vlog_start_id"]
                r_lo = row["rlog_start_id"]
                b_lo = row["batch_start_id"]
                nxt  = cur.execute(
                    "SELECT vlog_start_id, rlog_start_id, batch_start_id "
                    "FROM recording_sessions WHERE id>? ORDER BY id ASC LIMIT 1", (sid,)
                ).fetchone()
                if nxt:
                    v_hi = nxt["vlog_start_id"]
                    r_hi = nxt["rlog_start_id"]
                    b_hi = nxt["batch_start_id"]
            conn.close()
        except (ValueError, TypeError):
            pass

    dt_parts  = []
    dt_params = []
    if from_dt:
        dt_parts.append("received_at >= ?")
        dt_params.append(from_dt)
    if to_dt:
        dt_parts.append("received_at <= ?")
        dt_params.append(to_dt)
    dt_clause = (" AND " + " AND ".join(dt_parts)) if dt_parts else ""

    return {
        "v_lo": v_lo, "v_hi": v_hi,
        "r_lo": r_lo, "r_hi": r_hi,
        "b_lo": b_lo, "b_hi": b_hi,
        "dt_clause": dt_clause, "dt_params": dt_params,
    }
