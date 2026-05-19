"""
core/cache.py
=============
In-memory caches that eliminate repeated database round-trips on the hot
MQTT callback path.

Three caches:
  _enroll_cache      — enrollment_registry rows, keyed by node_id.
                       Updated on enroll/edit/delete (operator actions).
  _session_cache     — session_state rows as plain dicts, keyed by node_id.
                       Updated write-through on every admitted packet and on
                       every status change (watchdog, LWT, session reset).
  _last_hash_cache   — curr_hash of the most recently written chain record.
                       Updated atomically inside _chain_write_lock after every
                       INSERT so the next hash computation uses it immediately.

All three are protected by _cache_lock for reads and writes outside the chain
write lock.
"""

import threading
from typing import Dict, Any, Optional

from db.connection import get_conn
from config import GENESIS_HASH


_cache_lock = threading.Lock()

_enroll_cache:    Dict[str, Any]  = {}
_session_cache:   Dict[str, Any]  = {}
_last_hash_cache: Optional[str]   = None

_chain_write_lock = threading.Lock()


def load_caches() -> None:
    """Populate all three caches from the database at startup."""
    global _last_hash_cache
    conn = get_conn()
    cur  = conn.cursor()
    for row in cur.execute("SELECT * FROM enrollment_registry"):
        _enroll_cache[row["node_id"]] = dict(row)
    for row in cur.execute("SELECT * FROM session_state"):
        _session_cache[row["node_id"]] = dict(row)
    row = cur.execute(
        "SELECT curr_hash FROM verification_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        row = cur.execute(
            "SELECT curr_hash FROM admin_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
    _last_hash_cache = row["curr_hash"] if row else GENESIS_HASH
    conn.close()


def get_prev_chain_hash() -> str:
    """Return the current last hash from cache. Called inside _chain_write_lock."""
    return _last_hash_cache or GENESIS_HASH


def refresh_enrollment(node_id: str) -> None:
    """Reload one node's enrollment row from the database into cache."""
    conn = get_conn()
    cur  = conn.cursor()
    row  = cur.execute(
        "SELECT * FROM enrollment_registry WHERE node_id=?", (node_id,)
    ).fetchone()
    conn.close()
    with _cache_lock:
        if row:
            _enroll_cache[node_id] = dict(row)
        else:
            _enroll_cache.pop(node_id, None)


def refresh_session(node_id: str) -> None:
    """Reload one node's session row from the database into cache."""
    conn = get_conn()
    cur  = conn.cursor()
    row  = cur.execute(
        "SELECT * FROM session_state WHERE node_id=?", (node_id,)
    ).fetchone()
    conn.close()
    with _cache_lock:
        if row:
            _session_cache[node_id] = dict(row)
        else:
            _session_cache.pop(node_id, None)


def apply_session_reset(node_id: str, now: str) -> None:
    """Sync a session reset into the cache without waiting for the writer thread."""
    with _cache_lock:
        if node_id in _session_cache:
            s = _session_cache[node_id]
            s["last_seq_num"]             = None
            s["last_sensor_value"]        = None
            s["last_sensor_timestamp_ms"] = None
            s["last_gateway_received_ms"] = None
            s["mqtt_status"]              = "CONNECTED"
            s["continuity_allowed"]       = 1
            s["last_reset_at"]            = now
            s["updated_at"]               = now


def apply_enrollment_status(node_id: str, new_status: str, now: str) -> None:
    """Sync an enrollment status change into the cache."""
    with _cache_lock:
        if node_id in _enroll_cache:
            _enroll_cache[node_id]["status"]     = new_status
            _enroll_cache[node_id]["updated_at"] = now
