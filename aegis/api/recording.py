"""
api/recording.py
================
Recording session management routes.
"""

from flask import Blueprint, request, jsonify
from api.auth import login_required
from db.schema import get_session_bounds, start_new_recording
from db.connection import get_conn

recording_bp = Blueprint("recording", __name__)


@recording_bp.route("/api/recording/start", methods=["POST"])
@login_required
def api_recording_start():
    data  = request.get_json(force=True) or {}
    label = str(data.get("label", "")).strip()
    bounds = start_new_recording(label)
    return jsonify({"ok": True, "session": bounds})


@recording_bp.route("/api/recording/current")
def api_recording_current():
    return jsonify(get_session_bounds())


@recording_bp.route("/api/recording/sessions")
def api_recording_sessions():
    """Returns all recording sessions with session numbers and end times."""
    conn     = get_conn()
    cur      = conn.cursor()
    rows     = cur.execute(
        "SELECT * FROM recording_sessions ORDER BY id ASC"
    ).fetchall()
    conn.close()
    sessions  = []
    row_list  = [dict(r) for r in rows]
    for i, s in enumerate(row_list):
        s["session_number"] = s["id"]
        s["ended_at"]       = row_list[i + 1]["started_at"] if i + 1 < len(row_list) else None
        s["is_current"]     = (i == len(row_list) - 1)
        sessions.append(s)
    sessions.reverse()
    return jsonify(sessions)
