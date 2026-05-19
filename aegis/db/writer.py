"""
db/writer.py
============
Asynchronous database write queue.

The MQTT callback thread handles packet evaluation and hash computation but
must never block on disk I/O. All writes are deposited into this queue and
committed by a single background thread that holds a persistent connection.

Multiple queued items are batched into one commit under load, reducing fsync
overhead on the USB SSD.
"""

import threading
import queue as _queue
import sqlite3

from config import DB_PATH


_db_write_queue: _queue.Queue = _queue.Queue()
_db_writer_stop: threading.Event = threading.Event()


def _db_writer_loop() -> None:
    """Background writer thread. Opens its own persistent DB connection."""

    def _open_conn():
        c = sqlite3.connect(DB_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA cache_size=-16000")
        c.execute("PRAGMA temp_store=MEMORY")
        return c

    conn = None

    def reconnect():
        nonlocal conn
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        try:
            conn = _open_conn()
        except Exception as e:
            print(f"[WRITER] DB reconnect failed: {e}")
            conn = None

    reconnect()

    while not _db_writer_stop.is_set() or not _db_write_queue.empty():
        items = []
        try:
            items.append(_db_write_queue.get(timeout=0.5))
            while True:
                try:
                    items.append(_db_write_queue.get_nowait())
                except _queue.Empty:
                    break
        except _queue.Empty:
            continue

        if conn is None:
            reconnect()
        if conn is None:
            for item in items:
                print(f"[WRITER] Dropped — no DB conn: {item.get('type')}")
            continue

        try:
            for item in items:
                t = item.get("type")
                if t == "admission":
                    conn.execute("""
                        INSERT INTO verification_log (
                            node_id, seq_num, timestamp_value, sensor_value, mqtt_topic,
                            gateway_received_ms, delay_ms, enforcement_latency_ms,
                            anomaly_range_flag, anomaly_roc_flag, anomaly_density_flag,
                            enforcement_status, prev_hash, curr_hash, received_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'ADMITTED',?,?,?)""",
                        item["args"])
                    conn.execute("""
                        UPDATE session_state
                        SET last_seq_num=?, last_sensor_value=?, last_sensor_timestamp_ms=?,
                            last_gateway_received_ms=?, mqtt_status='CONNECTED', updated_at=?
                        WHERE node_id=?""",
                        item["session_args"])
                elif t == "rejection":
                    conn.execute("""
                        INSERT INTO rejection_log (
                            node_id, seq_num, timestamp_value, sensor_value, mqtt_topic,
                            gateway_received_ms, enforcement_latency_ms, rejection_reason,
                            details_json, received_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        item["args"])
                elif t == "admin_event":
                    conn.execute("""
                        INSERT INTO admin_events
                            (node_id, event_type, event_details, created_at, prev_hash, curr_hash)
                        VALUES (?,?,?,?,?,?)""",
                        item["args"])
                elif t == "raw_sql":
                    conn.execute(item["sql"], item.get("params", ()))
            conn.commit()
        except Exception as e:
            print(f"[WRITER] Write error ({len(items)} items): {e}")
            reconnect()

    if conn:
        try:
            conn.close()
        except Exception:
            pass


def start_db_writer() -> None:
    """Start the background writer thread. Call once at startup."""
    t = threading.Thread(target=_db_writer_loop, daemon=True, name="aegis-db-writer")
    t.start()
    print("[AEGIS] Async DB writer thread started.")


def queue_raw_sql(sql: str, params: tuple = ()) -> None:
    """Enqueue an arbitrary SQL statement for async execution."""
    _db_write_queue.put({"type": "raw_sql", "sql": sql, "params": params})
