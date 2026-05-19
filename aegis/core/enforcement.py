"""
core/enforcement.py
===================
Stateless multi-layer enforcement engine.

evaluate_packet() is called on every incoming MQTT packet. It applies each
enforcement layer in order and returns an EvaluationResult immediately on the
first violation. All I/O is in-memory (cache lookups only); no DB calls occur
inside this function on the hot path.

Enforcement layers
------------------
Pre-L1  MALFORMED_PACKET       — required JSON fields missing
Pre-L1  UNENROLLED_NODE        — node_id not in enrollment registry
Pre-L1  NODE_NOT_ACTIVE        — node enrolled but deactivated by operator
L1      BOUNDED_DELAY_CLOCK    — timestamp too old, too new, or in the future
L1      BOUNDED_DELAY_INTERARRIVAL — inter-arrival deviates beyond jitter tolerance
L2      SEQ_RETROGRADE         — sequence number went backward (duplicate/replay)
L2      SEQ_GAP                — sequence number skipped (missing packets)
L3      SESSION_CONTINUITY_VIOLATION — reconnected without operator acknowledgement

Non-gating anomaly flags (§3.8) are computed after admission:
  anomaly_range_flag    — sensor_value outside enrolled min/max
  anomaly_roc_flag      — rate of change exceeds enrolled threshold
  anomaly_density_flag  — temporal packet density deviates from expected
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Any

from config import (
    JITTER_TOLERANCE_MS, DENSITY_WINDOW_SECONDS, DENSITY_TOLERANCE,
    AUTO_ENROLL,
)
from core.cache import (
    _cache_lock, _enroll_cache, _session_cache,
    refresh_enrollment,
)

import threading

_density_windows: Dict[str, deque] = {}
_density_lock = threading.Lock()


@dataclass
class EvaluationResult:
    admitted:              bool
    reason:                str
    details:               Dict[str, Any]
    anomaly_range_flag:    int   = 0
    anomaly_roc_flag:      int   = 0
    anomaly_density_flag:  int   = 0
    delay_ms:              int   = 0
    enforcement_latency_ms: float = 0.0


def evaluate_packet(packet: Dict[str, Any], mqtt_topic: str) -> EvaluationResult:
    """
    Evaluate one incoming packet against all enforcement layers.

    Returns an EvaluationResult with admitted=True on success, or admitted=False
    with a rejection reason and details dict on any violation.
    """
    t_receipt = time.perf_counter()

    for key in ["node_id", "seq_num", "timestamp", "sensor_value"]:
        if key not in packet:
            lat = (time.perf_counter() - t_receipt) * 1000
            return EvaluationResult(False, "MALFORMED_PACKET",
                {"missing": key}, enforcement_latency_ms=lat)

    node_id         = str(packet["node_id"])
    seq_num         = int(packet["seq_num"])
    timestamp_value = int(packet["timestamp"])
    sensor_value    = float(packet["sensor_value"])

    from db.schema import utc_now_iso
    gateway_received_ms = int(time.time() * 1000)

    with _cache_lock:
        reg = _enroll_cache.get(node_id)

    if reg is None:
        if AUTO_ENROLL:
            from core.admission import auto_enroll_node
            auto_enroll_node(node_id)
            refresh_enrollment(node_id)
            with _cache_lock:
                reg = _enroll_cache.get(node_id)
        else:
            lat = (time.perf_counter() - t_receipt) * 1000
            return EvaluationResult(False, "UNENROLLED_NODE",
                {"node_id": node_id, "_skip_db": True},
                enforcement_latency_ms=lat)

    if reg["status"] != "active":
        lat = (time.perf_counter() - t_receipt) * 1000
        return EvaluationResult(False, "NODE_NOT_ACTIVE",
            {"node_id": node_id, "status": reg["status"]},
            enforcement_latency_ms=lat)

    from core.admission import ensure_session_row
    ensure_session_row(node_id)
    with _cache_lock:
        session = _session_cache.get(node_id, {})

    # L1 — Clock deviation (§3.7.1)
    delay_ms = gateway_received_ms - timestamp_value
    if delay_ms < 0:
        lat = (time.perf_counter() - t_receipt) * 1000
        return EvaluationResult(False, "BOUNDED_DELAY_CLOCK",
            {"delta_clock_ms": delay_ms, "reason": "future_timestamp"},
            delay_ms=delay_ms, enforcement_latency_ms=lat)
    if delay_ms > int(reg["delay_threshold_ms"]):
        lat = (time.perf_counter() - t_receipt) * 1000
        return EvaluationResult(False, "BOUNDED_DELAY_CLOCK",
            {"delta_clock_ms": delay_ms,
             "threshold_ms": reg["delay_threshold_ms"],
             "reason": "exceeds_clock_tolerance"},
            delay_ms=delay_ms, enforcement_latency_ms=lat)

    # L1 — Inter-arrival consistency (§3.7.1)
    # Uses sensor timestamp delta, not gateway arrival delta, to avoid false
    # positives from reconnection network jitter.
    last_sensor_ts    = session.get("last_sensor_timestamp_ms")
    expected_interval = int(reg["expected_sampling_ms"])
    if last_sensor_ts is not None:
        delta_inter   = timestamp_value - int(last_sensor_ts)
        deviation     = abs(delta_inter - expected_interval)
        gap_threshold = expected_interval * 2
        if delta_inter > gap_threshold:
            print(f"[L1] Large gap {delta_inter}ms > {gap_threshold}ms — inter-arrival skipped")
        elif deviation > JITTER_TOLERANCE_MS:
            lat = (time.perf_counter() - t_receipt) * 1000
            return EvaluationResult(False, "BOUNDED_DELAY_INTERARRIVAL",
                {"delta_inter_ms": delta_inter,
                 "expected_interval_ms": expected_interval,
                 "deviation_ms": deviation,
                 "jitter_tolerance_ms": JITTER_TOLERANCE_MS},
                delay_ms=delay_ms, enforcement_latency_ms=lat)

    # L2 — Monotonic sequence continuity (§3.7.2)
    last_seq = session.get("last_seq_num")
    if last_seq is not None:
        expected_seq = int(last_seq) + 1
        if seq_num < expected_seq:
            lat = (time.perf_counter() - t_receipt) * 1000
            return EvaluationResult(False, "SEQ_RETROGRADE",
                {"last_seq_num": last_seq, "expected_seq_num": expected_seq,
                 "received_seq_num": seq_num, "reason": "retrograde_or_duplicate"},
                delay_ms=delay_ms, enforcement_latency_ms=lat)
        if seq_num > expected_seq:
            lat = (time.perf_counter() - t_receipt) * 1000
            return EvaluationResult(False, "SEQ_GAP",
                {"last_seq_num": last_seq, "expected_seq_num": expected_seq,
                 "received_seq_num": seq_num, "reason": "sequence_gap"},
                delay_ms=delay_ms, enforcement_latency_ms=lat)

    # L3 — MQTT session continuity (§3.7.3)
    if int(session.get("continuity_allowed", 1)) != 1 or \
       session.get("mqtt_status") not in ("CONNECTED", "INACTIVE", "WAITING_FOR_FIRST_PACKET"):
        lat = (time.perf_counter() - t_receipt) * 1000
        return EvaluationResult(False, "SESSION_CONTINUITY_VIOLATION",
            {"mqtt_status": session.get("mqtt_status"),
             "continuity_allowed": int(session.get("continuity_allowed", 0))},
            delay_ms=delay_ms, enforcement_latency_ms=lat)

    # Non-gating anomaly flags (§3.8)
    anomaly_range_flag = anomaly_roc_flag = anomaly_density_flag = 0

    if reg.get("min_value") is not None and sensor_value < float(reg["min_value"]):
        anomaly_range_flag = 1
    if reg.get("max_value") is not None and sensor_value > float(reg["max_value"]):
        anomaly_range_flag = 1

    last_value = session.get("last_sensor_value")
    max_roc    = reg.get("max_rate_of_change")
    if last_value is not None and max_roc is not None:
        if abs(sensor_value - float(last_value)) > float(max_roc):
            anomaly_roc_flag = 1

    # Temporal density (§3.8.3)
    with _density_lock:
        dw         = _density_windows.setdefault(node_id, deque())
        window_ms  = DENSITY_WINDOW_SECONDS * 1000
        cutoff     = gateway_received_ms - window_ms
        while dw and dw[0] < cutoff:
            dw.popleft()
        Cw = len(dw)
        session_start = session.get("last_reset_at") or session.get("last_connect_at")
        elapsed_ms    = window_ms
        if session_start:
            try:
                from datetime import datetime as _dt
                s_ms       = int(_dt.fromisoformat(
                    session_start.replace("+08:00", "")).timestamp() * 1000)
                elapsed_ms = gateway_received_ms - s_ms
            except Exception:
                pass
        effective_window_ms = min(elapsed_ms, window_ms) if elapsed_ms > 0 else expected_interval
        Ew = max(1, int(effective_window_ms / expected_interval))
        if abs(Cw - Ew) > DENSITY_TOLERANCE:
            anomaly_density_flag = 1
        dw.append(gateway_received_ms)

    lat = (time.perf_counter() - t_receipt) * 1000
    return EvaluationResult(True, "ADMITTED",
        {"node_id": node_id, "mqtt_topic": mqtt_topic},
        anomaly_range_flag=anomaly_range_flag,
        anomaly_roc_flag=anomaly_roc_flag,
        anomaly_density_flag=anomaly_density_flag,
        delay_ms=delay_ms,
        enforcement_latency_ms=lat)
