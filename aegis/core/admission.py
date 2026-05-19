"""
core/admission.py
=================
Packet storage, admin event writing, session management helpers,
and the top-level process_packet() entry point.
"""

import time
import json
import hashlib
import threading
from collections import deque
from typing import Dict, Any, Tuple

from config import GENESIS_HASH, RATE_WINDOW_SECONDS, AUTO_ENROLL
from db.connection import get_conn
from db.schema import utc_now_iso
from db.writer import _db_write_queue, queue_raw_sql
from core.cache import (
    _cache_lock, _enroll_cache, _session_cache,
    _chain_write_lock, _last_hash_cache,
    get_prev_chain_hash, refresh_enrollment,
)

_packet_rate_windows: Dict[str, deque] = {}
_packet_rate_lock = threading.Lock()


def current_unix_ms() -> int:
    return int(time.time() * 1000)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def json_compact(data: Dict[str, Any]) -> str:
    return json.dumps(data, separators=(",", ":"), sort_keys=True)


def track_packet_rate(node_id: str, gateway_received_ms: int) -> None:
    """Update the rolling packet-rate window for a node."""
    with _packet_rate_lock:
        if node_id not in _packet_rate_windows:
            _packet_rate_windows[node_id] = deque()
        dw     = _packet_rate_windows[node_id]
        cutoff = gateway_received_ms - (RATE_WINDOW_SECONDS * 1000)
        while dw and dw[0] < cutoff:
            dw.popleft()
        dw.append(gateway_received_ms)


def get_packet_rate(node_id: str) -> float:
    """Return packets/second for a node over the last RATE_WINDOW_SECONDS."""
    with _packet_rate_lock:
        dw = _packet_rate_windows.get(node_id)
        if not dw or len(dw) < 2:
            return 0.0
        now_ms    = current_unix_ms()
        cutoff    = now_ms - (RATE_WINDOW_SECONDS * 1000)
        in_window = [t for t in dw if t >= cutoff]
        if len(in_window) < 2:
            return 0.0
        span_s = (in_window[-1] - in_window[0]) / 1000
        return round(len(in_window) / max(span_s, 1.0), 2)


def ensure_session_row(node_id: str) -> None:
    """
    Ensure a session_state row exists for node_id.
    Uses the cache as a fast-path guard — the database is only touched on
    first contact with a previously unseen node.
    """
    with _cache_lock:
        if node_id in _session_cache:
            return
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT node_id FROM session_state WHERE node_id=?", (node_id,))
    if cur.fetchone() is None:
        now = utc_now_iso()
        cur.execute("""
            INSERT INTO session_state (
                node_id, last_seq_num, last_sensor_value, last_sensor_timestamp_ms,
                last_gateway_received_ms, mqtt_status, continuity_allowed,
                last_connect_at, updated_at
            ) VALUES (?,NULL,NULL,NULL,NULL,'CONNECTED',1,?,?)""",
            (node_id, now, now))
        conn.commit()
    row = cur.execute(
        "SELECT * FROM session_state WHERE node_id=?", (node_id,)
    ).fetchone()
    conn.close()
    if row:
        with _cache_lock:
            _session_cache[node_id] = dict(row)


def update_mqtt_status(node_id: str, status: str) -> None:
    """Update a node's MQTT status in the database and cache."""
    ensure_session_row(node_id)
    now = utc_now_iso()
    if status == "CONNECTED":
        sql        = ("UPDATE session_state SET mqtt_status='CONNECTED', "
                      "last_connect_at=?, updated_at=? WHERE node_id=?")
        params     = (now, now, node_id)
        new_status = "CONNECTED"
    elif status == "DISCONNECTED":
        sql        = ("UPDATE session_state SET mqtt_status='INTERRUPTED', "
                      "continuity_allowed=0, last_disconnect_at=?, updated_at=? WHERE node_id=?")
        params     = (now, now, node_id)
        new_status = "INTERRUPTED"
    else:
        return
    queue_raw_sql(sql, params)
    with _cache_lock:
        if node_id in _session_cache:
            _session_cache[node_id]["mqtt_status"] = new_status
            if new_status == "INTERRUPTED":
                _session_cache[node_id]["continuity_allowed"] = 0


def write_admin_event(node_id: str, event_type: str, event_details: dict) -> None:
    """
    Append an admin event to the hash chain.
    Called from operator API routes and MQTT event handlers.
    Hash computation and cache update happen synchronously; the DB write is queued.
    """
    global _last_hash_cache
    now = utc_now_iso()
    with _chain_write_lock:
        prev_hash  = get_prev_chain_hash()
        serialized = json_compact({
            "node_id": node_id, "event_type": event_type,
            "event_details": event_details, "created_at": now,
            "prev_hash": prev_hash,
        })
        curr_hash = sha256_hex(serialized)
        import core.cache as _cc
        _cc._last_hash_cache = curr_hash
    _db_write_queue.put({
        "type": "admin_event",
        "args": (node_id, event_type, json_compact(event_details), now, prev_hash, curr_hash),
    })


def auto_enroll_node(node_id: str) -> None:
    """
    Automatically enroll an unrecognized node with default parameters.
    Hash computation and cache update are synchronous; DB writes are queued.
    """
    now = utc_now_iso()
    queue_raw_sql("""
        INSERT OR IGNORE INTO enrollment_registry (
            node_id, expected_sampling_ms, delay_threshold_ms, min_value, max_value,
            max_rate_of_change, density_min_interval_ms, status, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,'active',?,?)""",
        (node_id, 1000, 3000, None, None, None, 250, now, now))
    queue_raw_sql("""
        INSERT OR IGNORE INTO session_state (
            node_id, last_seq_num, last_sensor_value, last_sensor_timestamp_ms,
            last_gateway_received_ms, mqtt_status, continuity_allowed,
            last_connect_at, updated_at
        ) VALUES (?,NULL,NULL,NULL,NULL,'CONNECTED',1,?,?)""",
        (node_id, now, now))
    with _cache_lock:
        _enroll_cache[node_id] = {
            "node_id": node_id, "expected_sampling_ms": 1000,
            "delay_threshold_ms": 3000, "min_value": None, "max_value": None,
            "max_rate_of_change": None, "density_min_interval_ms": 250,
            "status": "active", "created_at": now, "updated_at": now,
        }
        _session_cache[node_id] = {
            "node_id": node_id, "last_seq_num": None, "last_sensor_value": None,
            "last_sensor_timestamp_ms": None, "last_gateway_received_ms": None,
            "mqtt_status": "CONNECTED", "continuity_allowed": 1,
            "last_connect_at": now, "updated_at": now,
        }
    write_admin_event(node_id, "AUTO_ENROLLED", {"enrolled_at": now})


def store_rejection(packet: dict, mqtt_topic: str, result) -> None:
    """Queue a rejection record for async write."""
    _db_write_queue.put({
        "type": "rejection",
        "args": (
            packet.get("node_id"), packet.get("seq_num"), packet.get("timestamp"),
            packet.get("sensor_value"), mqtt_topic, current_unix_ms(),
            result.enforcement_latency_ms,
            result.reason, json_compact(result.details), utc_now_iso(),
        ),
    })


def store_admission(packet: dict, mqtt_topic: str, result) -> None:
    """
    Compute the hash chain link synchronously, update all caches,
    then queue the DB write. The MQTT callback thread is never blocked on disk I/O.
    """
    global _last_hash_cache

    node_id             = str(packet["node_id"])
    seq_num             = int(packet["seq_num"])
    timestamp_value     = int(packet["timestamp"])
    sensor_value        = float(packet["sensor_value"])
    gateway_received_ms = current_unix_ms()
    received_at         = utc_now_iso()

    with _chain_write_lock:
        prev_hash  = get_prev_chain_hash()
        serialized = json_compact({
            "node_id": node_id, "seq_num": seq_num,
            "timestamp_value": timestamp_value, "sensor_value": sensor_value,
            "mqtt_topic": mqtt_topic, "gateway_received_ms": gateway_received_ms,
            "delay_ms": result.delay_ms,
            "anomaly_range_flag":   result.anomaly_range_flag,
            "anomaly_roc_flag":     result.anomaly_roc_flag,
            "anomaly_density_flag": result.anomaly_density_flag,
            "enforcement_status": "ADMITTED",
            "received_at": received_at, "prev_hash": prev_hash,
        })
        curr_hash = sha256_hex(serialized)
        import core.cache as _cc
        _cc._last_hash_cache = curr_hash

    _db_write_queue.put({
        "type": "admission",
        "args": (
            node_id, seq_num, timestamp_value, sensor_value, mqtt_topic,
            gateway_received_ms, result.delay_ms, result.enforcement_latency_ms,
            result.anomaly_range_flag, result.anomaly_roc_flag, result.anomaly_density_flag,
            prev_hash, curr_hash, received_at,
        ),
        "session_args": (
            seq_num, sensor_value, timestamp_value, gateway_received_ms, received_at, node_id,
        ),
    })

    with _cache_lock:
        if node_id in _session_cache:
            s = _session_cache[node_id]
            s["last_seq_num"]             = seq_num
            s["last_sensor_value"]        = sensor_value
            s["last_sensor_timestamp_ms"] = timestamp_value
            s["last_gateway_received_ms"] = gateway_received_ms
            s["mqtt_status"]              = "CONNECTED"
            s["updated_at"]               = received_at

    track_packet_rate(node_id, gateway_received_ms)


def process_packet(packet: Dict[str, Any], mqtt_topic: str = "direct/api") -> Tuple[bool, Dict[str, Any]]:
    """
    Top-level packet handler. Evaluates, stores, and returns a status dict.
    Called from the MQTT message callback and the /api/ingest REST endpoint.
    """
    from core.enforcement import evaluate_packet
    result = evaluate_packet(packet, mqtt_topic)
    if result.admitted:
        store_admission(packet, mqtt_topic, result)
        return True, {
            "status": "ADMITTED",
            "delay_ms": result.delay_ms,
            "enforcement_latency_ms": round(result.enforcement_latency_ms, 3),
            "anomaly_range_flag":   result.anomaly_range_flag,
            "anomaly_roc_flag":     result.anomaly_roc_flag,
            "anomaly_density_flag": result.anomaly_density_flag,
        }
    if result.details.get("_skip_db"):
        node_id = result.details.get("node_id", "unknown")
        print(f"[GATE] UNENROLLED_NODE dropped | node={node_id} topic={mqtt_topic}")
        return False, {
            "status": "REJECTED", "reason": result.reason,
            "details": result.details, "delay_ms": result.delay_ms,
            "enforcement_latency_ms": round(result.enforcement_latency_ms, 3),
        }
    store_rejection(packet, mqtt_topic, result)
    return False, {
        "status": "REJECTED", "reason": result.reason,
        "details": result.details, "delay_ms": result.delay_ms,
        "enforcement_latency_ms": round(result.enforcement_latency_ms, 3),
    }
