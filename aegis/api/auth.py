"""
api/auth.py
===========
Operator authentication routes and the login_required decorator.

Authentication is session-based (Flask server-side sessions).
All operator-write API routes require a valid session. Read-only dashboard
routes are public and do not require authentication.

The operator password is set via the OPERATOR_PASSWORD environment variable.
"""

import functools
from flask import Blueprint, request, jsonify, session as flask_session
from config import OPERATOR_PASSWORD

auth_bp = Blueprint("auth", __name__)


def login_required(f):
    """Route decorator — returns HTTP 401 if the operator is not authenticated."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not flask_session.get("operator_logged_in"):
            return jsonify({
                "error": "Authentication required. Please log in via Operator Tools.",
                "auth":  False,
            }), 401
        return f(*args, **kwargs)
    return decorated


@auth_bp.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(force=True) or {}
    if data.get("password") == OPERATOR_PASSWORD:
        flask_session["operator_logged_in"] = True
        flask_session.permanent = False
        return jsonify({"ok": True, "message": "Authenticated."})
    return jsonify({"ok": False, "error": "Incorrect password."}), 401


@auth_bp.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    flask_session.clear()
    return jsonify({"ok": True})


@auth_bp.route("/api/auth/status")
def api_auth_status():
    return jsonify({"logged_in": bool(flask_session.get("operator_logged_in"))})
