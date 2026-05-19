"""
api/admin.py
============
Operator-only node management routes.
All routes in this module require the operator to be authenticated
via the login_required decorator.
"""

from flask import Blueprint, request, jsonify
from api.auth import login_required
from db.connection import get_conn
from db.schema import utc_now_iso
from core.admission import (
    ensure_session_row, update_mqtt_status, write_admin_event,
)
from core.cache import (
    _cache_lock, _enroll_cache, _session_cache,
    refresh_enrollment, apply_session_reset, apply_enrollment_status,
)
from core.enforcement import _density_windows

admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/api/admin/enroll", methods=["POST"])
@login_required
def api_admin_enroll():
    data    = request.get_json(force=True)
    node_id = str(data["node_id"]).strip()
    now     = utc_now_iso()
    conn    = get_conn()
    cur     = conn.cursor()
    cur.execute("""
        INSERT INTO enrollment_registry (
            node_id, expected_sampling_ms, delay_threshold_ms, min_value, max_value,
            max_rate_of_change, density_min_interval_ms, status, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,'active',?,?)
        ON CONFLICT(node_id) DO UPDATE SET
            expected_sampling_ms    = excluded.expected_sampling_ms,
            delay_threshold_ms      = excluded.delay_threshold_ms,
            min_value               = excluded.min_value,
            max_value               = excluded.max_value,
            max_rate_of_change      = excluded.max_rate_of_change,
            density_min_interval_ms = excluded.density_min_interval_ms,
            status='active', updated_at=excluded.updated_at""",
        (node_id,
         int(data.get("expected_sampling_ms", 1000)),
         int(data.get("delay_threshold_ms", 3000)),
         float(data["min_value"]) if data.get("min_value") not in (None, "") else None,
         float(data["max_value"]) if data.get("max_value") not in (None, "") else None,
         float(data["max_rate_of_change"]) if data.get("max_rate_of_change") not in (None, "") else None,
         int(data.get("density_min_interval_ms", 250)), now, now))
    conn.commit()
    conn.close()
    ensure_session_row(node_id)
    write_admin_event(node_id, "ENROLLMENT_SAVE", {"updated_at": now})
    return jsonify({"ok": True, "node_id": node_id})


@admin_bp.route("/api/admin/reset", methods=["POST"])
@login_required
def api_admin_reset():
    node_id = str(request.get_json(force=True)["node_id"]).strip()
    ensure_session_row(node_id)
    now  = utc_now_iso()
    conn = get_conn()
    conn.execute("""
        UPDATE session_state
        SET last_seq_num=NULL, last_sensor_value=NULL,
            last_sensor_timestamp_ms=NULL, last_gateway_received_ms=NULL,
            mqtt_status='CONNECTED', continuity_allowed=1,
            last_reset_at=?, updated_at=?
        WHERE node_id=?""", (now, now, node_id))
    conn.commit()
    conn.close()
    write_admin_event(node_id, "SESSION_RESET", {"reset_at": now})
    return jsonify({"ok": True})


@admin_bp.route("/api/admin/disconnect", methods=["POST"])
@login_required
def api_admin_disconnect():
    node_id = str(request.get_json(force=True)["node_id"]).strip()
    update_mqtt_status(node_id, "DISCONNECTED")
    write_admin_event(node_id, "SESSION_INTERRUPTED", {"at": utc_now_iso()})
    return jsonify({"ok": True})


@admin_bp.route("/api/admin/reconnect", methods=["POST"])
@login_required
def api_admin_reconnect():
    node_id = str(request.get_json(force=True)["node_id"]).strip()
    ensure_session_row(node_id)
    now  = utc_now_iso()
    conn = get_conn()
    conn.execute("""
        UPDATE session_state
        SET mqtt_status='CONNECTED', continuity_allowed=1,
            last_connect_at=?, updated_at=?
        WHERE node_id=?""", (now, now, node_id))
    conn.commit()
    conn.close()
    write_admin_event(node_id, "SESSION_RECONNECTED", {"at": now})
    return jsonify({"ok": True})


@admin_bp.route("/api/admin/session-reset", methods=["POST"])
@login_required
def api_admin_session_reset():
    """
    Resets a node's session state so the next packet establishes a new seq baseline.
    Clears the in-memory density window so the first post-reset packet is not
    penalized by inter-arrival comparison against pre-reset data.
    """
    data    = request.get_json(force=True) or {}
    node_id = str(data.get("node_id", "")).strip()
    reason  = str(data.get("reason", "operator-triggered")).strip()
    if not node_id:
        return jsonify({"error": "node_id required"}), 400
    ensure_session_row(node_id)
    now  = utc_now_iso()
    conn = get_conn()
    conn.execute("""
        UPDATE session_state
        SET last_seq_num=NULL, last_sensor_value=NULL,
            last_sensor_timestamp_ms=NULL, last_gateway_received_ms=NULL,
            mqtt_status='CONNECTED', continuity_allowed=1,
            last_reset_at=?, updated_at=?
        WHERE node_id=?""", (now, now, node_id))
    conn.commit()
    conn.close()
    apply_session_reset(node_id, now)
    _density_windows.pop(node_id, None)
    write_admin_event(node_id, "SESSION_RESET", {"reset_at": now, "reason": reason})
    return jsonify({
        "ok": True, "node_id": node_id, "reset_at": now,
        "note": "Next packet from this node will establish a new seq baseline.",
    })


@admin_bp.route("/api/admin/node-action", methods=["POST"])
@login_required
def api_admin_node_action():
    """Suspend, reactivate, or conclude a node."""
    data    = request.get_json(force=True) or {}
    node_id = str(data.get("node_id", "")).strip()
    action  = str(data.get("action", "")).strip().lower()
    reason  = str(data.get("reason", "")).strip()
    if not node_id or action not in ("suspend", "reactivate", "conclude"):
        return jsonify({"error": "node_id and action (suspend|reactivate|conclude) required"}), 400
    status_map = {"suspend": "suspended", "reactivate": "active", "conclude": "concluded"}
    new_status = status_map[action]
    now  = utc_now_iso()
    conn = get_conn()
    conn.execute("UPDATE enrollment_registry SET status=?, updated_at=? WHERE node_id=?",
                 (new_status, now, node_id))
    if action == "conclude":
        conn.execute("""
            UPDATE session_state
            SET mqtt_status='INTERRUPTED', continuity_allowed=0, updated_at=?
            WHERE node_id=?""", (now, node_id))
    conn.commit()
    conn.close()
    apply_enrollment_status(node_id, new_status, now)
    if action == "conclude":
        with _cache_lock:
            if node_id in _session_cache:
                _session_cache[node_id]["mqtt_status"]        = "INTERRUPTED"
                _session_cache[node_id]["continuity_allowed"] = 0
                _session_cache[node_id]["updated_at"]         = now
    elif action == "reactivate":
        with _cache_lock:
            if node_id in _session_cache:
                _session_cache[node_id]["mqtt_status"]        = "CONNECTED"
                _session_cache[node_id]["continuity_allowed"] = 1
                _session_cache[node_id]["updated_at"]         = now
    write_admin_event(node_id, f"NODE_{action.upper()}D",
        {"new_status": new_status, "reason": reason or f"operator {action}"})
    return jsonify({"ok": True, "node_id": node_id, "status": new_status})


@admin_bp.route("/api/admin/node-delete", methods=["POST"])
@login_required
def api_admin_node_delete():
    """
    Permanently removes a node from the enrollment registry and session state.
    Historical records in verification_log and rejection_log are preserved.
    """
    data    = request.get_json(force=True) or {}
    node_id = str(data.get("node_id", "")).strip()
    if not node_id:
        return jsonify({"error": "node_id required"}), 400
    now = utc_now_iso()
    write_admin_event(node_id, "NODE_DELETED", {"deleted_at": now, "by": "operator"})
    conn = get_conn()
    conn.execute("DELETE FROM enrollment_registry WHERE node_id=?", (node_id,))
    conn.execute("DELETE FROM session_state WHERE node_id=?", (node_id,))
    conn.commit()
    conn.close()
    with _cache_lock:
        _enroll_cache.pop(node_id, None)
        _session_cache.pop(node_id, None)
    _density_windows.pop(node_id, None)
    return jsonify({"ok": True, "node_id": node_id, "deleted_at": now})


@admin_bp.route("/api/admin/node-edit", methods=["POST"])
@login_required
def api_admin_node_edit():
    """
    Updates enrollment parameters for an existing node.
    Automatically triggers a session reset so the new interval takes effect
    cleanly on the next packet.
    """
    data    = request.get_json(force=True) or {}
    node_id = str(data.get("node_id", "")).strip()
    if not node_id:
        return jsonify({"error": "node_id required"}), 400
    now  = utc_now_iso()
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT node_id FROM enrollment_registry WHERE node_id=?", (node_id,))
    if not cur.fetchone():
        conn.close()
        return jsonify({"error": f"Node '{node_id}' not found"}), 404
    fields = []
    values = []
    for col, key, cast in [
        ("expected_sampling_ms",    "expected_sampling_ms",    int),
        ("delay_threshold_ms",      "delay_threshold_ms",      int),
    ]:
        if key in data and data[key] not in (None, ""):
            fields.append(f"{col}=?")
            values.append(cast(data[key]))
    for col, key in [("min_value", "min_value"), ("max_value", "max_value"),
                     ("max_rate_of_change", "max_rate_of_change")]:
        if key in data:
            fields.append(f"{col}=?")
            values.append(float(data[key]) if data[key] not in (None, "") else None)
    if not fields:
        conn.close()
        return jsonify({"error": "No fields to update"}), 400
    fields.append("updated_at=?")
    values.append(now)
    values.append(node_id)
    cur.execute(f"UPDATE enrollment_registry SET {', '.join(fields)} WHERE node_id=?", values)
    cur.execute("""
        UPDATE session_state
        SET last_seq_num=NULL, last_sensor_value=NULL,
            last_sensor_timestamp_ms=NULL, last_gateway_received_ms=NULL,
            mqtt_status='CONNECTED', continuity_allowed=1,
            last_reset_at=?, updated_at=?
        WHERE node_id=?""", (now, now, node_id))
    conn.commit()
    conn.close()
    refresh_enrollment(node_id)
    apply_session_reset(node_id, now)
    _density_windows.pop(node_id, None)
    write_admin_event(node_id, "NODE_EDITED", {
        "updated_fields": list(data.keys()), "updated_at": now,
    })
    return jsonify({"ok": True, "node_id": node_id, "updated_at": now,
        "note": "Session reset applied. Next packet establishes new seq baseline."})


@admin_bp.route("/api/admin/reanchor", methods=["POST"])
@login_required
def api_admin_reanchor():
    """Manually trigger anchoring of all pending LOCAL_ONLY batches."""
    from core.merkle import anchor_pending_batches
    conn    = get_conn()
    pending = conn.execute(
        "SELECT COUNT(*) FROM merkle_batches WHERE anchor_status IN ('LOCAL_ONLY','ANCHOR_FAILED')"
    ).fetchone()[0]
    conn.close()
    anchor_pending_batches()
    conn = get_conn()
    anchored = conn.execute(
        "SELECT COUNT(*) FROM merkle_batches WHERE anchor_status='SOLANA_DEVNET'"
    ).fetchone()[0]
    failed = conn.execute(
        "SELECT COUNT(*) FROM merkle_batches WHERE anchor_status='ANCHOR_FAILED'"
    ).fetchone()[0]
    conn.close()
    return jsonify({"ok": True, "anchored": anchored, "failed": failed})


@admin_bp.route("/api/merkle/recompute/<int:batch_id>")
def api_merkle_recompute(batch_id: int):
    """Recompute the Merkle root for a single batch from stored curr_hash values."""
    from core.merkle import compute_merkle_root
    conn  = get_conn()
    cur   = conn.cursor()
    batch = cur.execute(
        "SELECT * FROM merkle_batches WHERE id=?", (batch_id,)
    ).fetchone()
    if not batch:
        conn.close()
        return jsonify({"error": f"Batch #{batch_id} not found"}), 404
    rows = cur.execute(
        "SELECT curr_hash FROM verification_log WHERE merkle_batch_id=? ORDER BY id",
        (batch_id,)
    ).fetchall()
    conn.close()
    leaves   = [r["curr_hash"] for r in rows]
    computed = compute_merkle_root(leaves)
    return jsonify({
        "batch_id":      batch_id,
        "stored_root":   batch["merkle_root"],
        "computed_root": computed,
        "match":         computed == batch["merkle_root"],
        "leaf_count":    len(leaves),
    })


@admin_bp.route("/api/merkle/recompute-all")
def api_merkle_recompute_all():
    """Recompute Merkle roots for all batches and return a mismatch count."""
    from core.merkle import compute_merkle_root
    conn    = get_conn()
    cur     = conn.cursor()
    batches = cur.execute("SELECT id, merkle_root FROM merkle_batches ORDER BY id").fetchall()
    results = []
    for batch in batches:
        rows   = cur.execute(
            "SELECT curr_hash FROM verification_log WHERE merkle_batch_id=? ORDER BY id",
            (batch["id"],)
        ).fetchall()
        leaves   = [r["curr_hash"] for r in rows]
        computed = compute_merkle_root(leaves)
        results.append({
            "batch_id":      batch["id"],
            "stored_root":   batch["merkle_root"],
            "computed_root": computed,
            "match":         computed == batch["merkle_root"],
            "leaf_count":    len(leaves),
        })
    conn.close()
    mismatches = sum(1 for r in results if not r["match"])
    return jsonify({"total": len(results), "mismatches": mismatches, "results": results})


@admin_bp.route("/api/ingest", methods=["POST"])
def api_ingest():
    """Direct HTTP packet injection endpoint (testing and non-MQTT deployments)."""
    from core.admission import process_packet
    packet   = request.get_json(force=True)
    ok, info = process_packet(packet, "direct/api")
    return jsonify(info), 200 if ok else 400
