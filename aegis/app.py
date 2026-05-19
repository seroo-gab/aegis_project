"""
app.py
======
AEGIS Gateway — application entry point.

Startup sequence:
  1. init_db()             — create tables and indexes (idempotent)
  2. seed_default_nodes()  — seed example node on first-ever launch
  3. reset_all_sessions()  — clear session state so each run starts clean
  4. load_caches()         — populate in-memory caches from the reset DB
  5. start_db_writer()     — start async write queue thread
  6. start_mqtt()          — connect to broker, start watchdog thread
  7. start_scheduler()     — start APScheduler for Merkle/Solana anchoring
  8. app.run()             — serve Flask on 0.0.0.0:5000

Environment variables are documented in .env.example.
"""

import os
from dotenv import load_dotenv

load_dotenv()

from flask import Flask, render_template

from config import SECRET_KEY, DB_PATH, MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, SOLANA_PRIVATE_KEY

from db         import init_db, seed_default_nodes, start_db_writer, utc_now_iso
from db.schema  import get_session_bounds
from db.connection import get_conn, query_rows
from core.cache import load_caches
from mqtt.handler import start_mqtt_background

from api.auth      import auth_bp
from api.admin     import admin_bp
from api.dashboard import dashboard_bp
from api.recording import recording_bp
from api.export    import export_bp
from api.workspace import workspace_bp


def reset_all_sessions() -> None:
    """
    Reset all active node sessions at startup. Writes a RECORDING_START admin
    event so auditors can identify where each run began. Called before
    load_caches() so the cache reflects the post-reset DB state.
    """
    from core.admission import write_admin_event
    conn  = get_conn()
    cur   = conn.cursor()
    now   = utc_now_iso()
    nodes = cur.execute(
        "SELECT node_id FROM enrollment_registry WHERE status='active'"
    ).fetchall()
    reset_ids = []
    for node in nodes:
        nid = node["node_id"]
        cur.execute("""
            UPDATE session_state
            SET last_seq_num=NULL, last_sensor_value=NULL,
                last_sensor_timestamp_ms=NULL, last_gateway_received_ms=NULL,
                mqtt_status='CONNECTED', continuity_allowed=1,
                last_reset_at=?, updated_at=?
            WHERE node_id=?""", (now, now, nid))
        reset_ids.append(nid)
    conn.commit()
    conn.close()
    if reset_ids:
        print(f"[AEGIS] All sessions reset for new recording run — nodes: {reset_ids}")
    write_admin_event("SYSTEM", "RECORDING_START",
        {"started_at": now, "reset_nodes": reset_ids})


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__, template_folder="templates")
    app.config["JSON_SORT_KEYS"] = False
    app.config["SECRET_KEY"]     = SECRET_KEY

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(recording_bp)
    app.register_blueprint(export_bp)
    app.register_blueprint(workspace_bp)

    @app.route("/")
    def index():
        return render_template("index.html")

    return app


if __name__ == "__main__":
    print("=" * 60)
    print("AEGIS Gateway Dashboard — Complete Edition")
    print("=" * 60)

    init_db()
    seed_default_nodes()
    reset_all_sessions()
    load_caches()
    start_db_writer()
    start_mqtt_background()

    from core.merkle import start_anchor_scheduler
    _scheduler = start_anchor_scheduler()

    app = create_app()

    print(f"DB        : {DB_PATH}")
    print(f"Dashboard : http://0.0.0.0:5000")
    print(f"MQTT      : {MQTT_BROKER}:{MQTT_PORT}  topic={MQTT_TOPIC}")
    print(f"Solana    : {'ENABLED' if SOLANA_PRIVATE_KEY else 'DISABLED — set SOLANA_PRIVATE_KEY'}")
    print("=" * 60)

    app.run(host="0.0.0.0", port=5000, debug=False)
