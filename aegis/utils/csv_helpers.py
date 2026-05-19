"""
utils/csv_helpers.py
====================
Column-aware CSV formatting utilities for all export routes.

Two Excel compatibility issues are addressed:
  1. Large integer truncation — gateway_received_ms and timestamp_value are
     13-digit Unix millisecond timestamps. Excel converts them to scientific
     notation (1.78E+12). Fix: export as ISO 8601 datetime strings.
  2. Float precision loss — enforcement_latency_ms values with many decimal
     places are truncated on re-save. Fix: round to 6 decimal places.
"""

from datetime import datetime, timezone, timedelta

PHT = timezone(timedelta(hours=8))

_CSV_MS_TIMESTAMP_COLS = {
    "gateway_received_ms", "timestamp_value",
    "last_sensor_timestamp_ms", "last_gateway_received_ms",
    "interval_start_ms", "interval_end_ms",
}

_CSV_FLOAT_COLS = {
    "enforcement_latency_ms", "sensor_value", "last_sensor_value",
}

_CSV_FLOAT_DP = 6


def _ms_to_iso(ms) -> str:
    """Convert a Unix millisecond timestamp to an ISO 8601 string in PHT (UTC+8)."""
    try:
        ms_int       = int(ms)
        seconds      = ms_int // 1000
        milliseconds = ms_int % 1000
        dt           = datetime.fromtimestamp(seconds, tz=PHT)
        return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{milliseconds:03d}+08:00")
    except (TypeError, ValueError, OSError):
        return str(ms)


def csv_cell(col: str, v) -> str:
    """
    Format a single CSV cell value for export.
    - None              → empty string
    - ms timestamp cols → ISO 8601 datetime string
    - float cols        → rounded to _CSV_FLOAT_DP decimal places
    - Everything else   → standard escape (quote if value contains , " or newline)
    """
    if v is None:
        return ""
    if col in _CSV_MS_TIMESTAMP_COLS:
        return _ms_to_iso(v)
    if col in _CSV_FLOAT_COLS:
        try:
            return str(round(float(v), _CSV_FLOAT_DP))
        except (TypeError, ValueError):
            pass
    s = str(v)
    return f'"{s}"' if any(c in s for c in [",", '"', "\n"]) else s


def csv_rows(headers: list, data: list) -> str:
    """Build a complete CSV string from a list of column names and a list of dicts."""
    lines = [",".join(headers)]
    for row in data:
        lines.append(",".join(csv_cell(h, row.get(h)) for h in headers))
    return "\n".join(lines)
