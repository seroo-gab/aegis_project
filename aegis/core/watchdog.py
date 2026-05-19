"""
core/watchdog.py
================
Node liveness watchdog daemon thread.

Runs every 2 seconds. Transitions nodes between CONNECTED → INACTIVE →
WAITING_FOR_FIRST_PACKET based on silence duration. Does not write to the
database directly — all state changes are queued through the async writer
and applied immediately to the in-memory cache.
"""

import time
import threading

from config import NODE_INACTIVITY_MULTIPLIER
from db.schema import utc_now_iso
from db.writer import queue_raw_sql
from core.cache import _cache_lock, _enroll_cache, _session_cache


def watchdog_loop() -> None:
    """
    Background daemon. Marks nodes INACTIVE when they exceed
    (expected_sampling_ms × NODE_INACTIVITY_MULTIPLIER) ms of silence.
    Nodes that have never sent a packet are marked WAITING_FOR_FIRST_PACKET
    and are never transitioned to INACTIVE.
    """
    while True:
        time.sleep(2)
        try:
            now_ms  = int(time.time() * 1000)
            now_iso = utc_now_iso()
            with _cache_lock:
                snapshot = [
                    (nid, dict(reg), dict(_session_cache.get(nid, {})))
                    for nid, reg in _enroll_cache.items()
                    if reg.get("status") == "active"
                ]
            for nid, reg, sess in snapshot:
                status = sess.get("mqtt_status")
                if sess.get("last_gateway_received_ms") is None:
                    if status not in ("WAITING_FOR_FIRST_PACKET", "INTERRUPTED"):
                        queue_raw_sql(
                            "UPDATE session_state "
                            "SET mqtt_status='WAITING_FOR_FIRST_PACKET', updated_at=? "
                            "WHERE node_id=?",
                            (now_iso, nid))
                        with _cache_lock:
                            if nid in _session_cache:
                                _session_cache[nid]["mqtt_status"] = "WAITING_FOR_FIRST_PACKET"
                    continue
                elapsed   = now_ms - int(sess["last_gateway_received_ms"])
                threshold = int(reg.get("expected_sampling_ms", 5000)) * NODE_INACTIVITY_MULTIPLIER
                if elapsed > threshold and status == "CONNECTED":
                    queue_raw_sql(
                        "UPDATE session_state SET mqtt_status='INACTIVE', updated_at=? "
                        "WHERE node_id=?",
                        (now_iso, nid))
                    with _cache_lock:
                        if nid in _session_cache:
                            _session_cache[nid]["mqtt_status"] = "INACTIVE"
                    print(f"[WATCHDOG] {nid} → INACTIVE (silent {elapsed}ms)")
        except Exception as e:
            print(f"[WATCHDOG] Error: {e}")


def start_watchdog() -> None:
    """Start the watchdog as a daemon thread."""
    t = threading.Thread(target=watchdog_loop, daemon=True, name="aegis-watchdog")
    t.start()
    print("[AEGIS] Node liveness watchdog started.")
