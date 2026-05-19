"""
api/export.py
=============
Data export routes. All exports support session scoping and optional date/time
range filtering via ?session_id=, ?from_dt=, and ?to_dt= query parameters.

Timestamp columns are exported as ISO 8601 strings to prevent Excel scientific
notation on 13-digit Unix millisecond values.
"""

import io
import csv as _csv
import json as _json
import math
from flask import Blueprint, request, jsonify, Response

from db.connection import get_conn, query_rows
from db.schema import get_session_bounds, get_export_bounds, utc_now_iso
from utils.csv_helpers import csv_rows, _CSV_FLOAT_DP

export_bp = Blueprint("export", __name__)


def _ts() -> str:
    return utc_now_iso().replace(":", "-").replace("+", "").replace(" ", "T")[:19]


@export_bp.route("/api/export/verification-log")
def api_export_verification_log():
    """Export verification log. Supports ?node=, ?session_id=, ?from_dt=, ?to_dt=, ?fmt=, ?limit=."""
    node       = request.args.get("node",       "").strip() or None
    limit      = min(int(request.args.get("limit", 100000)), 100000)
    fmt        = request.args.get("fmt",        "json")
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None
    eb         = get_export_bounds(session_id, from_dt, to_dt)

    where  = f"id>{eb['v_lo']}"
    params = []
    if eb["v_hi"] is not None:
        where += f" AND id<={eb['v_hi']}"
    if node:
        where += " AND node_id=?"
        params.append(node)
    where += eb["dt_clause"]
    params.extend(eb["dt_params"])

    rows = query_rows(
        f"SELECT * FROM verification_log WHERE {where} ORDER BY id ASC LIMIT ?",
        params + [limit])
    ts   = _ts()
    data = [dict(r) for r in rows]

    if fmt == "csv":
        if not data:
            return Response("id,node_id,seq_num\n", mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename=aegis_verification_log_{ts}.csv"})
        return Response(csv_rows(list(data[0].keys()), data), mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=aegis_verification_log_{ts}.csv"})
    payload = _json.dumps({"exported_at": ts, "record_count": len(data), "records": data}, indent=2)
    return Response(payload, mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename=aegis_verification_log_{ts}.json"})


@export_bp.route("/api/export/rejection-log")
def api_export_rejection_log():
    """Export rejection log. Supports ?node=, ?session_id=, ?from_dt=, ?to_dt=, ?fmt=."""
    fmt        = request.args.get("fmt",        "json")
    node       = request.args.get("node",       "").strip() or None
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None
    eb         = get_export_bounds(session_id, from_dt, to_dt)

    where  = f"id>{eb['r_lo']}"
    params = []
    if eb["r_hi"] is not None:
        where += f" AND id<={eb['r_hi']}"
    if node:
        where += " AND node_id=?"
        params.append(node)
    where += eb["dt_clause"]
    params.extend(eb["dt_params"])

    rows = query_rows(f"SELECT * FROM rejection_log WHERE {where} ORDER BY id ASC", params)
    ts   = _ts()
    data = [dict(r) for r in rows]

    if fmt == "csv":
        headers = list(data[0].keys()) if data else ["id"]
        return Response(csv_rows(headers, data), mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=aegis_rejection_log_{ts}.csv"})
    payload = _json.dumps({"exported_at": ts, "record_count": len(data), "records": data}, indent=2)
    return Response(payload, mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename=aegis_rejection_log_{ts}.json"})


@export_bp.route("/api/export/merkle-batches")
def api_export_merkle_batches():
    """Export Merkle batches. Supports ?status=, ?session_id=, ?from_dt=, ?to_dt=, ?fmt=."""
    status     = request.args.get("status",     "").strip() or None
    fmt        = request.args.get("fmt",        "json")
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None
    eb         = get_export_bounds(session_id, from_dt, to_dt)

    where  = f"id>{eb['b_lo']}"
    params = []
    if eb["b_hi"] is not None:
        where += f" AND id<={eb['b_hi']}"
    if status:
        where += " AND anchor_status=?"
        params.append(status)
    if from_dt:
        where += " AND created_at >= ?"
        params.append(from_dt)
    if to_dt:
        where += " AND created_at <= ?"
        params.append(to_dt)

    rows = query_rows(f"SELECT * FROM merkle_batches WHERE {where} ORDER BY id ASC", params)
    ts   = _ts()
    data = [dict(r) for r in rows]

    if fmt == "csv":
        headers = list(data[0].keys()) if data else ["id"]
        return Response(csv_rows(headers, data), mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=aegis_merkle_batches_{ts}.csv"})
    payload = _json.dumps({"exported_at": ts, "batch_count": len(data), "batches": data}, indent=2)
    return Response(payload, mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename=aegis_merkle_batches_{ts}.json"})


@export_bp.route("/api/export/admin-events")
def api_export_admin_events():
    """Export admin events. Supports ?session_id=, ?from_dt=, ?to_dt=, ?fmt=."""
    fmt        = request.args.get("fmt",        "json")
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None

    where  = "1=1"
    params = []
    if session_id:
        try:
            sid  = int(session_id)
            conn = get_conn()
            cur  = conn.cursor()
            row  = cur.execute("SELECT started_at FROM recording_sessions WHERE id=?", (sid,)).fetchone()
            nxt  = cur.execute(
                "SELECT started_at FROM recording_sessions WHERE id>? ORDER BY id ASC LIMIT 1",
                (sid,)).fetchone()
            conn.close()
            if row:
                where += " AND created_at >= ?"
                params.append(row["started_at"])
            if nxt:
                where += " AND created_at < ?"
                params.append(nxt["started_at"])
        except (ValueError, TypeError):
            pass
    if from_dt:
        where += " AND created_at >= ?"
        params.append(from_dt)
    if to_dt:
        where += " AND created_at <= ?"
        params.append(to_dt)

    rows = query_rows(f"SELECT * FROM admin_events WHERE {where} ORDER BY id ASC", params)
    ts   = _ts()
    data = [dict(r) for r in rows]

    if fmt == "csv":
        headers = list(data[0].keys()) if data else ["id"]
        return Response(csv_rows(headers, data), mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=aegis_admin_events_{ts}.csv"})
    payload = _json.dumps({"exported_at": ts, "event_count": len(data), "events": data}, indent=2)
    return Response(payload, mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename=aegis_admin_events_{ts}.json"})


@export_bp.route("/api/export/transmission-delay")
def api_export_transmission_delay():
    """Export delay_ms per admitted packet. Supports ?node=, ?session_id=, ?from_dt=, ?to_dt=, ?fmt=."""
    node       = request.args.get("node",       "").strip() or None
    limit      = min(int(request.args.get("limit", 50000)), 200000)
    fmt        = request.args.get("fmt",        "csv")
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None
    eb         = get_export_bounds(session_id, from_dt, to_dt)

    where  = f"id>{eb['v_lo']}"
    params = []
    if eb["v_hi"] is not None:
        where += f" AND id<={eb['v_hi']}"
    if node:
        where += " AND node_id=?"
        params.append(node)
    where += eb["dt_clause"]
    params.extend(eb["dt_params"])

    rows = query_rows(
        f"SELECT id, node_id, seq_num, received_at, timestamp_value, "
        f"gateway_received_ms, delay_ms FROM verification_log "
        f"WHERE {where} ORDER BY id LIMIT {limit}", params)

    if fmt == "json":
        return jsonify([dict(r) for r in rows])

    buf = io.StringIO()
    w   = _csv.writer(buf)
    w.writerow(["id", "node_id", "seq_num", "received_at",
                "timestamp_value_ms", "gateway_received_ms", "delay_ms"])
    for r in rows:
        w.writerow([r["id"], r["node_id"], r["seq_num"], r["received_at"],
                    r["timestamp_value"], r["gateway_received_ms"], r["delay_ms"]])
    fn = f"aegis_transmission_delay_{'_'.join(node.split()) if node else 'all'}.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fn}"})


@export_bp.route("/api/export/processing-time")
def api_export_processing_time():
    """Export enforcement_latency_ms for admitted and rejected packets. Supports ?node=, ?session_id=, ?from_dt=, ?to_dt=, ?fmt=."""
    node       = request.args.get("node",       "").strip() or None
    limit      = min(int(request.args.get("limit", 50000)), 200000)
    fmt        = request.args.get("fmt",        "csv")
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None
    eb         = get_export_bounds(session_id, from_dt, to_dt)

    vw = f"id>{eb['v_lo']}"
    rw = f"id>{eb['r_lo']}"
    params = []
    if eb["v_hi"] is not None:
        vw += f" AND id<={eb['v_hi']}"
    if eb["r_hi"] is not None:
        rw += f" AND id<={eb['r_hi']}"
    if node:
        vw += " AND node_id=?"
        rw += " AND node_id=?"
        params.append(node)
    vw += eb["dt_clause"]
    rw += eb["dt_clause"]

    admitted = query_rows(
        f"SELECT id, node_id, seq_num, received_at, enforcement_latency_ms, 'ADMITTED' AS outcome "
        f"FROM verification_log WHERE {vw} AND enforcement_latency_ms IS NOT NULL "
        f"ORDER BY id LIMIT {limit}",
        params + eb["dt_params"])
    rejected = query_rows(
        f"SELECT id, node_id, seq_num, received_at, enforcement_latency_ms, "
        f"rejection_reason AS outcome FROM rejection_log "
        f"WHERE {rw} AND enforcement_latency_ms IS NOT NULL ORDER BY id LIMIT {limit}",
        params + eb["dt_params"])
    all_rows = sorted([dict(x) for x in admitted] + [dict(x) for x in rejected],
                      key=lambda x: x["id"])

    if fmt == "json":
        return jsonify(all_rows)

    buf = io.StringIO()
    w   = _csv.writer(buf)
    w.writerow(["id", "node_id", "seq_num", "received_at", "enforcement_latency_ms", "outcome"])
    for row in all_rows:
        w.writerow([row["id"], row["node_id"], row["seq_num"], row["received_at"],
                    row["enforcement_latency_ms"], row["outcome"]])
    fn = f"aegis_processing_time_{'_'.join(node.split()) if node else 'all'}.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fn}"})


@export_bp.route("/api/export/run-summary")
def api_export_run_summary():
    """
    Per-node summary: admitted/rejected counts, rejection breakdown by enforcement
    layer, and latency statistics (mean, min, max, P95).
    Supports ?fmt=csv|json, ?session_id=, ?from_dt=, ?to_dt=.
    """
    session_id = request.args.get("session_id", "").strip() or None
    from_dt    = request.args.get("from_dt",    "").strip() or None
    to_dt      = request.args.get("to_dt",      "").strip() or None
    eb         = get_export_bounds(session_id, from_dt, to_dt)

    v   = eb["v_lo"]; v_hi = eb["v_hi"]
    r   = eb["r_lo"]; r_hi = eb["r_hi"]
    blo = eb["b_lo"]; b_hi = eb["b_hi"]

    v_scope = f"id>{v}" + (f" AND id<={v_hi}" if v_hi else "")
    r_scope = f"id>{r}" + (f" AND id<={r_hi}" if r_hi else "")
    b_scope = f"id>{blo}" + (f" AND id<={b_hi}" if b_hi else "")
    v_scope += eb["dt_clause"]
    r_scope += eb["dt_clause"]

    conn  = get_conn()
    cur   = conn.cursor()
    nodes = cur.execute(
        "SELECT node_id, status, expected_sampling_ms FROM enrollment_registry ORDER BY node_id"
    ).fetchall()

    node_stats = []
    for node in nodes:
        nid      = node["node_id"]
        admitted = cur.execute(
            f"SELECT COUNT(*) FROM verification_log WHERE {v_scope} AND node_id=?",
            eb["dt_params"] + [nid]).fetchone()[0]
        rej_rows = cur.execute(
            f"SELECT rejection_reason, COUNT(*) as cnt FROM rejection_log "
            f"WHERE {r_scope} AND node_id=? GROUP BY rejection_reason",
            eb["dt_params"] + [nid]).fetchall()
        rej            = {row["rejection_reason"]: row["cnt"] for row in rej_rows}
        total_rejected = sum(rej.values())
        total          = admitted + total_rejected
        lat_agg        = cur.execute(
            f"SELECT AVG(enforcement_latency_ms) as avg_ms, "
            f"MIN(enforcement_latency_ms) as min_ms, MAX(enforcement_latency_ms) as max_ms "
            f"FROM verification_log WHERE {v_scope} AND node_id=?",
            eb["dt_params"] + [nid]).fetchone()
        p95_ms = None
        if admitted > 0:
            p95_offset = max(0, int(math.ceil(admitted * 0.05)) - 1)
            p95_row    = cur.execute(
                f"SELECT enforcement_latency_ms FROM verification_log "
                f"WHERE {v_scope} AND node_id=? AND enforcement_latency_ms IS NOT NULL "
                f"ORDER BY enforcement_latency_ms DESC LIMIT 1 OFFSET {p95_offset}",
                eb["dt_params"] + [nid]).fetchone()
            p95_ms = round(p95_row["enforcement_latency_ms"], 3) if p95_row else None

        node_stats.append({
            "node_id":               nid,
            "status":                node["status"],
            "expected_interval_ms":  node["expected_sampling_ms"],
            "admitted":              admitted,
            "total_rejected":        total_rejected,
            "total_packets":         total,
            "rejection_rate_pct":    round(total_rejected / total * 100, 2) if total > 0 else 0.0,
            "L1_bounded_delay_clock":    rej.get("BOUNDED_DELAY_CLOCK",            0),
            "L1_interarrival":           rej.get("BOUNDED_DELAY_INTERARRIVAL",     0),
            "L2_seq_gap":                rej.get("SEQ_GAP",                        0),
            "L2_seq_retrograde":         rej.get("SEQ_RETROGRADE",                 0),
            "L3_session_continuity":     rej.get("SESSION_CONTINUITY_VIOLATION",   0),
            "PRE_unenrolled":            rej.get("UNENROLLED_NODE",                0),
            "PRE_malformed":             rej.get("MALFORMED_PACKET",               0),
            "latency_mean_ms":  round(lat_agg["avg_ms"], 3) if lat_agg["avg_ms"] else None,
            "latency_min_ms":   round(lat_agg["min_ms"], 3) if lat_agg["min_ms"] else None,
            "latency_max_ms":   round(lat_agg["max_ms"], 3) if lat_agg["max_ms"] else None,
            "latency_p95_ms":   p95_ms,
        })

    total_adm  = cur.execute(f"SELECT COUNT(*) FROM verification_log WHERE {v_scope}", eb["dt_params"]).fetchone()[0]
    total_rej  = cur.execute(f"SELECT COUNT(*) FROM rejection_log WHERE {r_scope}", eb["dt_params"]).fetchone()[0]
    n_batches  = cur.execute(f"SELECT COUNT(*) FROM merkle_batches WHERE {b_scope}").fetchone()[0]
    n_anchored = cur.execute(f"SELECT COUNT(*) FROM merkle_batches WHERE {b_scope} AND anchor_status='SOLANA_DEVNET'").fetchone()[0]
    overall_lat = cur.execute(
        f"SELECT AVG(enforcement_latency_ms) as avg_ms, MIN(enforcement_latency_ms) as min_ms, "
        f"MAX(enforcement_latency_ms) as max_ms FROM verification_log WHERE {v_scope}",
        eb["dt_params"]).fetchone()
    conn.close()

    if session_id:
        conn2  = get_conn()
        srow   = conn2.execute("SELECT started_at, label FROM recording_sessions WHERE id=?",
                               (session_id,)).fetchone()
        conn2.close()
        started_at = srow["started_at"] if srow else None
        label      = srow["label"]      if srow else ""
    else:
        b2         = get_session_bounds()
        started_at = b2["started_at"]
        label      = b2["label"] or ""

    ts = _ts()
    summary = {
        "exported_at":        ts,
        "session_started_at": started_at,
        "session_label":      label,
        "session_id":         session_id or "current",
        "overall": {
            "total_packets":           total_adm + total_rej,
            "total_admitted":          total_adm,
            "total_rejected":          total_rej,
            "overall_rejection_pct":   round(total_rej / (total_adm + total_rej) * 100, 2)
                                       if (total_adm + total_rej) > 0 else 0,
            "merkle_batches_total":    n_batches,
            "merkle_batches_anchored": n_anchored,
            "latency_mean_ms": round(overall_lat["avg_ms"], 3) if overall_lat["avg_ms"] else None,
            "latency_min_ms":  round(overall_lat["min_ms"], 3) if overall_lat["min_ms"] else None,
            "latency_max_ms":  round(overall_lat["max_ms"], 3) if overall_lat["max_ms"] else None,
        },
        "per_node": node_stats,
    }

    fmt = request.args.get("fmt", "json")
    if fmt == "csv":
        if not node_stats:
            return Response("node_id\n", mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename=aegis_run_summary_{ts}.csv"})
        headers = list(node_stats[0].keys())
        def _esc(v):
            v = "" if v is None else str(v)
            return f'"{v}"' if any(c in v for c in [",", '"', "\n"]) else v
        lines = [",".join(headers)] + [",".join(_esc(row.get(h)) for h in headers) for row in node_stats]
        return Response("\n".join(lines), mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=aegis_run_summary_{ts}.csv"})
    return Response(_json.dumps(summary, indent=2), mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename=aegis_run_summary_{ts}.json"})


@export_bp.route("/api/export/inclusion-proof")
def api_export_inclusion_proof():
    """
    Generate a Merkle inclusion proof for a single record within a batch.
    Proves the record belongs to the batch without requiring the full dataset.
    Required: ?batch_id=N&record_id=N
    """
    from core.merkle import sha256_hex, compute_merkle_root
    try:
        batch_id  = int(request.args.get("batch_id",  0))
        record_id = int(request.args.get("record_id", 0))
        if not batch_id or not record_id:
            return jsonify({"error": "batch_id and record_id are required"}), 400

        conn  = get_conn()
        cur   = conn.cursor()
        batch = cur.execute("SELECT * FROM merkle_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            conn.close()
            return jsonify({"error": f"Batch #{batch_id} not found"}), 404

        leaves_rows = cur.execute(
            "SELECT id, curr_hash FROM verification_log "
            "WHERE merkle_batch_id=? ORDER BY id ASC", (batch_id,)
        ).fetchall()
        target = cur.execute(
            "SELECT * FROM verification_log WHERE id=?", (record_id,)
        ).fetchone()
        conn.close()

        if not target:
            return jsonify({"error": f"Record #{record_id} not found"}), 404
        if target["merkle_batch_id"] != batch_id:
            return jsonify({"error": f"Record #{record_id} is not in batch #{batch_id}"}), 400

        leaves     = [r["curr_hash"] for r in leaves_rows]
        leaf_ids   = [r["id"] for r in leaves_rows]
        target_idx = leaf_ids.index(record_id)

        proof_path = []
        idx   = target_idx
        level = leaves[:]
        while len(level) > 1:
            if len(level) % 2 == 1:
                level.append(level[-1])
            sibling_idx = idx ^ 1
            proof_path.append({
                "sibling_hash": level[sibling_idx],
                "position":     "right" if idx % 2 == 0 else "left",
            })
            level = [sha256_hex(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
            idx   = idx // 2

        return jsonify({
            "proof_type":    "merkle_inclusion",
            "batch_id":      batch_id,
            "record_id":     record_id,
            "target_hash":   target["curr_hash"],
            "leaf_index":    target_idx,
            "total_leaves":  len(leaves),
            "merkle_root":   batch["merkle_root"],
            "anchor_status": batch["anchor_status"],
            "anchor_ref":    batch["anchor_ref"],
            "proof_path":    proof_path,
            "record":        dict(target),
            "verification_instructions": (
                "To verify: start with target_hash. For each step in proof_path, "
                "if position=right compute sha256(current + sibling_hash), "
                "else compute sha256(sibling_hash + current). "
                "The final result must equal merkle_root."
            ),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
