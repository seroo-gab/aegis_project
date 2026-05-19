"""
api/dashboard.py
================
Read-only dashboard data routes — statistics, node status, logs, trends,
latency, live data, and system health.
"""

import math
import time
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify

from config import DB_PATH, ANCHOR_INTERVAL_MIN
from db.connection import get_conn, query_rows
from db.schema import get_session_bounds, utc_now_iso
from core.admission import get_packet_rate
from core.cache import _cache_lock, _enroll_cache, _session_cache

dashboard_bp = Blueprint("dashboard", __name__)

PHT = timezone(timedelta(hours=8))


def get_per_node_stats() -> list:
    """Aggregate per-node counts from the database with live pkt/s from cache."""
    b   = get_session_bounds()
    v   = b["vlog_start_id"]
    r   = b["rlog_start_id"]
    rows = query_rows(f"""
        SELECT e.node_id, e.status, e.expected_sampling_ms, e.delay_threshold_ms,
               e.min_value, e.max_value, e.max_rate_of_change,
               s.mqtt_status, s.continuity_allowed, s.last_seq_num,
               s.last_sensor_value, s.updated_at,
               COALESCE(a.admitted,0)   AS admitted,
               COALESCE(rej.rejected,0) AS rejected,
               COALESCE(an.anomalies,0) AS anomalies
        FROM enrollment_registry e
        LEFT JOIN session_state s ON e.node_id=s.node_id
        LEFT JOIN (SELECT node_id, COUNT(*) AS admitted
                   FROM verification_log WHERE id>{v} GROUP BY node_id) a
               ON e.node_id=a.node_id
        LEFT JOIN (SELECT node_id, COUNT(*) AS rejected
                   FROM rejection_log WHERE id>{r} GROUP BY node_id) rej
               ON e.node_id=rej.node_id
        LEFT JOIN (SELECT node_id, COUNT(*) AS anomalies
                   FROM verification_log
                   WHERE id>{v} AND (anomaly_range_flag=1 OR anomaly_roc_flag=1 OR anomaly_density_flag=1)
                   GROUP BY node_id) an
               ON e.node_id=an.node_id
        ORDER BY e.node_id""")
    result = []
    for row in rows:
        d = dict(row)
        d["packets_per_second"] = get_packet_rate(d["node_id"])
        result.append(d)
    return result


@dashboard_bp.route("/api/stats")
def api_stats():
    b    = get_session_bounds()
    v    = b["vlog_start_id"]
    r    = b["rlog_start_id"]
    ba   = b["batch_start_id"]
    conn = get_conn()
    cur  = conn.cursor()
    admitted  = cur.execute(f"SELECT COUNT(*) FROM verification_log WHERE id>{v}").fetchone()[0]
    rejected  = cur.execute(f"SELECT COUNT(*) FROM rejection_log WHERE id>{r}").fetchone()[0]
    batches   = cur.execute(f"SELECT COUNT(*) FROM merkle_batches WHERE id>{ba}").fetchone()[0]
    anomalies = cur.execute(
        f"SELECT COUNT(*) FROM verification_log WHERE id>{v} "
        f"AND (anomaly_range_flag=1 OR anomaly_roc_flag=1 OR anomaly_density_flag=1)"
    ).fetchone()[0]
    avg_delay = cur.execute(
        f"SELECT AVG(delay_ms) FROM verification_log WHERE id>{v}"
    ).fetchone()[0]
    active    = cur.execute(
        "SELECT COUNT(*) FROM enrollment_registry WHERE status='active'"
    ).fetchone()[0]
    conn.close()
    return jsonify({
        "total_admitted":    admitted,
        "total_rejected":    rejected,
        "total_batches":     batches,
        "total_anomalies":   anomalies,
        "avg_delay_ms":      round(avg_delay, 2) if avg_delay else 0,
        "active_nodes":      active,
        "db_path":           DB_PATH,
        "session_started_at": b["started_at"],
        "session_label":      b["label"],
    })


@dashboard_bp.route("/api/nodes")
def api_nodes():
    return jsonify([dict(r) for r in get_per_node_stats()])


@dashboard_bp.route("/api/logs")
def api_logs():
    b     = get_session_bounds()
    v     = b["vlog_start_id"]
    node  = request.args.get("node")
    limit = min(int(request.args.get("limit", 100)), 100000)
    if node:
        rows = query_rows(
            f"SELECT * FROM verification_log WHERE id>{v} AND node_id=? "
            f"ORDER BY id DESC LIMIT ?", (node, limit))
    else:
        rows = query_rows(
            f"SELECT * FROM verification_log WHERE id>{v} ORDER BY id DESC LIMIT ?",
            (limit,))
    return jsonify([dict(r) for r in rows])


@dashboard_bp.route("/api/rejections")
def api_rejections():
    b     = get_session_bounds()
    r     = b["rlog_start_id"]
    node  = request.args.get("node")
    limit = min(int(request.args.get("limit", 100)), 100000)
    if node:
        rows = query_rows(
            f"SELECT * FROM rejection_log WHERE id>{r} AND node_id=? "
            f"ORDER BY id DESC LIMIT ?", (node, limit))
    else:
        rows = query_rows(
            f"SELECT * FROM rejection_log WHERE id>{r} ORDER BY id DESC LIMIT ?",
            (limit,))
    return jsonify([dict(r) for r in rows])


@dashboard_bp.route("/api/enforcement-stats")
def api_enforcement_stats():
    """Per-reason rejection counts for the current session."""
    b     = get_session_bounds()
    r     = b["rlog_start_id"]
    rows  = query_rows(
        f"SELECT rejection_reason, COUNT(*) as cnt FROM rejection_log "
        f"WHERE id>{r} GROUP BY rejection_reason ORDER BY cnt DESC")
    total = query_rows(
        f"SELECT COUNT(*) as total FROM rejection_log WHERE id>{r}"
    )[0]["total"]
    return jsonify({
        "total_rejected": total,
        "by_reason": [{"reason": row["rejection_reason"], "count": row["cnt"]} for row in rows],
    })


@dashboard_bp.route("/api/batches")
def api_batches():
    """Returns all Merkle batches. Batch records are permanent blockchain data."""
    return jsonify([dict(r) for r in query_rows(
        "SELECT * FROM merkle_batches ORDER BY id DESC LIMIT 200")])


@dashboard_bp.route("/api/events")
def api_events():
    return jsonify([dict(r) for r in query_rows(
        "SELECT * FROM admin_events ORDER BY id DESC LIMIT 100")])


@dashboard_bp.route("/api/anchor-status")
def api_anchor_status():
    return jsonify([dict(r) for r in query_rows(
        "SELECT id, record_count, merkle_root, anchor_status, anchor_ref, created_at "
        "FROM merkle_batches ORDER BY id DESC LIMIT 50")])


@dashboard_bp.route("/api/wallet-balance")
def api_wallet_balance():
    from config import SOLANA_PRIVATE_KEY, SOLANA_RPC_URL
    if not SOLANA_PRIVATE_KEY:
        return jsonify({"balance_sol": None, "error": "SOLANA_PRIVATE_KEY not set"})
    try:
        from solders.keypair import Keypair
        from solana.rpc.api  import Client
        kp  = Keypair.from_base58_string(SOLANA_PRIVATE_KEY)
        c   = Client(SOLANA_RPC_URL)
        bal = c.get_balance(kp.pubkey())
        return jsonify({"balance_sol": bal.value / 1e9, "pubkey": str(kp.pubkey())})
    except Exception as e:
        return jsonify({"balance_sol": None, "error": str(e)[:200]})


@dashboard_bp.route("/api/dashboard-stats")
def api_dashboard_stats():
    b     = get_session_bounds()
    v     = b["vlog_start_id"]
    r_id  = b["rlog_start_id"]
    conn  = get_conn()
    cur   = conn.cursor()
    admitted  = cur.execute(f"SELECT COUNT(*) FROM verification_log WHERE id>{v}").fetchone()[0]
    rejected  = cur.execute(f"SELECT COUNT(*) FROM rejection_log WHERE id>{r_id}").fetchone()[0]
    anomalies = cur.execute(
        f"SELECT COUNT(*) FROM verification_log WHERE id>{v} "
        f"AND (anomaly_range_flag=1 OR anomaly_roc_flag=1 OR anomaly_density_flag=1)"
    ).fetchone()[0]
    avg_delay = cur.execute(
        f"SELECT AVG(delay_ms) FROM verification_log WHERE id>{v}"
    ).fetchone()[0]
    avg_lat   = cur.execute(
        f"SELECT AVG(enforcement_latency_ms) FROM verification_log WHERE id>{v}"
    ).fetchone()[0]
    conn.close()
    return jsonify({
        "admitted":          admitted,
        "rejected":          rejected,
        "anomalies":         anomalies,
        "avg_delay_ms":      round(avg_delay, 2) if avg_delay else 0,
        "avg_latency_ms":    round(avg_lat,   3) if avg_lat   else 0,
        "session_started_at": b["started_at"],
        "session_label":      b["label"],
    })


@dashboard_bp.route("/api/system-health")
def api_system_health():
    from mqtt.handler import mqtt_started
    conn = get_conn()
    cur  = conn.cursor()
    vlog_count = cur.execute("SELECT COUNT(*) FROM verification_log").fetchone()[0]
    rlog_count = cur.execute("SELECT COUNT(*) FROM rejection_log").fetchone()[0]
    conn.close()
    return jsonify({
        "mqtt_connected":   mqtt_started,
        "db_path":          DB_PATH,
        "vlog_total":       vlog_count,
        "rlog_total":       rlog_count,
        "server_time":      utc_now_iso(),
    })


@dashboard_bp.route("/api/scheduler-health")
def api_scheduler_health():
    """Returns whether APScheduler is running and whether batches are on schedule."""
    conn = get_conn()
    cur  = conn.cursor()
    latest = cur.execute(
        "SELECT created_at, anchor_status FROM merkle_batches ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    now_iso            = utc_now_iso()
    batch_ok           = False
    minutes_since_last = None
    if latest:
        try:
            last_dt            = datetime.fromisoformat(latest["created_at"].replace("+08:00", ""))
            now_dt             = datetime.fromisoformat(now_iso.replace("+08:00", ""))
            minutes_since_last = round((now_dt - last_dt).total_seconds() / 60, 1)
            batch_ok           = minutes_since_last <= (ANCHOR_INTERVAL_MIN * 2)
        except Exception:
            pass
    from mqtt.handler import mqtt_started
    return jsonify({
        "scheduler_started":   mqtt_started,
        "last_batch_at":       latest["created_at"] if latest else None,
        "last_batch_status":   latest["anchor_status"] if latest else None,
        "minutes_since_batch": minutes_since_last,
        "batch_interval_min":  ANCHOR_INTERVAL_MIN,
        "batch_on_schedule":   batch_ok,
    })


@dashboard_bp.route("/api/timeseries/<node_id>")
def api_timeseries(node_id: str):
    b      = get_session_bounds()
    v      = b["vlog_start_id"]
    window = int(request.args.get("window", 3600))
    limit  = 300
    since  = int(time.time() * 1000) - (window * 1000)
    rows   = query_rows(
        f"SELECT gateway_received_ms, sensor_value, delay_ms "
        f"FROM verification_log WHERE id>{v} AND node_id=? AND gateway_received_ms>=? "
        f"ORDER BY gateway_received_ms ASC",
        (node_id, since))
    data = [dict(r) for r in rows]
    if len(data) > limit:
        stride = len(data) // limit
        data   = data[::stride][:limit]
    return jsonify(data)


@dashboard_bp.route("/api/latency")
def api_latency():
    b    = get_session_bounds()
    v    = b["vlog_start_id"]
    conn = get_conn()
    cur  = conn.cursor()
    rows = cur.execute(
        f"SELECT enforcement_latency_ms FROM verification_log "
        f"WHERE id>{v} AND enforcement_latency_ms IS NOT NULL ORDER BY id"
    ).fetchall()
    latencies = [r[0] for r in rows if r[0] is not None]
    conn.close()
    if not latencies:
        return jsonify({"count": 0, "mean": 0, "min": 0, "max": 0, "p95": 0, "histogram": []})
    latencies.sort()
    n    = len(latencies)
    mean = sum(latencies) / n
    p95  = latencies[int(n * 0.95)]
    buckets = [0, 1, 2, 5, 10, 20, 50, 100, 200, float("inf")]
    hist    = [0] * (len(buckets) - 1)
    for val in latencies:
        for i in range(len(buckets) - 1):
            if buckets[i] <= val < buckets[i + 1]:
                hist[i] += 1
                break
    labels = [f"{buckets[i]}-{buckets[i+1]}ms" if buckets[i+1] != float("inf")
              else f"{buckets[i]}+ms" for i in range(len(hist))]
    return jsonify({
        "count": n, "mean": round(mean, 3),
        "min": round(latencies[0], 3), "max": round(latencies[-1], 3),
        "p95": round(p95, 3),
        "histogram": [{"label": labels[i], "count": hist[i]} for i in range(len(hist))],
    })


@dashboard_bp.route("/api/live_data")
def api_live_data():
    b      = get_session_bounds()
    v      = b["vlog_start_id"]
    node   = request.args.get("node")
    window = int(request.args.get("window", 300))
    since  = int(time.time() * 1000) - (window * 1000)
    if node:
        rows = query_rows(
            f"SELECT id, node_id, seq_num, sensor_value, gateway_received_ms, delay_ms, "
            f"anomaly_range_flag, anomaly_roc_flag, anomaly_density_flag, received_at "
            f"FROM verification_log WHERE id>{v} AND node_id=? AND gateway_received_ms>=? "
            f"ORDER BY gateway_received_ms ASC LIMIT 500",
            (node, since))
    else:
        rows = query_rows(
            f"SELECT id, node_id, seq_num, sensor_value, gateway_received_ms, delay_ms, "
            f"anomaly_range_flag, anomaly_roc_flag, anomaly_density_flag, received_at "
            f"FROM verification_log WHERE id>{v} AND gateway_received_ms>=? "
            f"ORDER BY gateway_received_ms ASC LIMIT 500",
            (since,))
    return jsonify([dict(r) for r in rows])


@dashboard_bp.route("/api/historical_data")
def api_historical_data():
    b      = get_session_bounds()
    v      = b["vlog_start_id"]
    node   = request.args.get("node")
    window = int(request.args.get("window", 86400))
    bucket = max(30, window // 200)
    since  = int(time.time() * 1000) - (window * 1000)
    if node:
        rows = query_rows(
            f"SELECT gateway_received_ms, sensor_value FROM verification_log "
            f"WHERE id>{v} AND node_id=? AND gateway_received_ms>=? "
            f"ORDER BY gateway_received_ms ASC",
            (node, since))
    else:
        rows = query_rows(
            f"SELECT gateway_received_ms, sensor_value FROM verification_log "
            f"WHERE id>{v} AND gateway_received_ms>=? ORDER BY gateway_received_ms ASC",
            (since,))
    data = [dict(r) for r in rows]
    if not data:
        return jsonify([])
    buckets: dict = {}
    for rec in data:
        t = (rec["gateway_received_ms"] // (bucket * 1000)) * (bucket * 1000)
        if t not in buckets:
            buckets[t] = []
        buckets[t].append(rec["sensor_value"])
    result = [{"time_ms": t, "avg_sensor_value": round(sum(v_) / len(v_), 2)}
              for t, v_ in sorted(buckets.items())]
    return jsonify(result)


@dashboard_bp.route("/api/verify-chain")
def api_verify_chain():
    """Full hash chain walk — returns pass/fail and count of broken links."""
    conn    = get_conn()
    cur     = conn.cursor()
    rows    = cur.execute(
        "SELECT id, prev_hash, curr_hash FROM verification_log ORDER BY id ASC"
    ).fetchall()
    aevents = cur.execute(
        "SELECT id, prev_hash, curr_hash FROM admin_events ORDER BY id ASC"
    ).fetchall()
    conn.close()
    chain   = sorted(list(rows) + list(aevents), key=lambda r: r["curr_hash"])
    breaks  = 0
    checked = 0
    for i in range(1, len(chain)):
        prev = chain[i - 1]["curr_hash"]
        curr = chain[i]["prev_hash"]
        checked += 1
        if prev and curr and prev != curr:
            breaks += 1
    return jsonify({
        "chain_intact": breaks == 0,
        "pairs_checked": checked,
        "breaks": breaks,
    })
