"""
mqtt/handler.py
===============
MQTT client setup, connection and message callbacks, and startup.

The MQTT thread handles network I/O and calls on_mqtt_message() for every
incoming packet. All blocking work (DB writes, hash computation) is decoupled
from this thread via the async write queue and in-memory caches.
"""

import json
import threading

from config import MQTT_ENABLED, MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, LWT_TOPIC_PREFIX
from db.schema import utc_now_iso
from core.admission import update_mqtt_status, write_admin_event, process_packet
from core.enforcement import EvaluationResult, _density_windows
from core.cache import _cache_lock, _session_cache, _enroll_cache
from core.watchdog import start_watchdog


mqtt_client  = None
mqtt_started = False
mqtt_lock    = threading.Lock()


def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[MQTT] Connected → {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        client.subscribe(f"{LWT_TOPIC_PREFIX}/#", qos=1)
        print(f"[MQTT] Subscribed: {MQTT_TOPIC} | LWT: {LWT_TOPIC_PREFIX}/#")

        # Clear retained LWT messages left on the broker from previous sessions.
        # If not cleared, subscribing on startup causes the broker to immediately
        # redeliver stale "offline" payloads, which would mark nodes INTERRUPTED
        # before they have sent a single packet in the new run.
        try:
            with _cache_lock:
                enrolled = [nid for nid, reg in _enroll_cache.items()
                            if reg.get("status") == "active"]
            for node_id in enrolled:
                client.publish(f"{LWT_TOPIC_PREFIX}/{node_id}", payload=b"", qos=1, retain=True)
            if enrolled:
                print(f"[MQTT] Cleared retained LWT messages for {len(enrolled)} nodes.")
        except Exception as e:
            print(f"[MQTT] Warning: could not clear retained LWT: {e}")
    else:
        print(f"[MQTT] Connect failed rc={rc}")


def on_mqtt_message(client, userdata, msg):
    try:
        # LWT signal (§3.7.3): ESP32 publishes "offline" on graceful disconnect.
        if msg.topic.startswith(LWT_TOPIC_PREFIX + "/"):
            try:
                status  = msg.payload.decode("utf-8").strip().lower()
                node_id = msg.topic.split("/")[-1]
                if status == "offline":
                    # Ignore stale retained messages for nodes that have never
                    # sent a packet in this session.
                    with _cache_lock:
                        _s = _session_cache.get(node_id)
                    if _s is None or _s.get("last_gateway_received_ms") is None:
                        print(f"[L3] LWT ignored — node={node_id} has not sent a packet yet")
                        return
                    update_mqtt_status(node_id, "DISCONNECTED")
                    write_admin_event(node_id, "LWT_OFFLINE",
                        {"topic": msg.topic, "at": utc_now_iso()})
                    print(f"[L3] LWT offline → session INTERRUPTED | node={node_id}")
            except Exception as e:
                print(f"[L3] LWT parse error on {msg.topic}: {e}")
            return

        packet  = json.loads(msg.payload.decode("utf-8"))
        node_id = packet.get("node_id")

        # Restore CONNECTED status for nodes that went INACTIVE or WAITING
        # without a formal disconnect. Cache-only update — no DB round-trip.
        if node_id:
            with _cache_lock:
                s = _session_cache.get(node_id)
                if s and s.get("mqtt_status") in ("INACTIVE", "WAITING_FOR_FIRST_PACKET"):
                    s["mqtt_status"] = "CONNECTED"
                elif (s and s.get("mqtt_status") == "INTERRUPTED"
                      and int(s.get("continuity_allowed", 0)) == 1):
                    s["mqtt_status"] = "CONNECTED"

        ok, details = process_packet(packet, msg.topic)
        print(f"[MQTT] {details['status']:8} node={node_id} "
              f"seq={packet.get('seq_num')} "
              f"lat={details.get('enforcement_latency_ms', 0):.2f}ms")

    except Exception as e:
        print(f"[MQTT] ERROR {e}")
        try:
            from db.writer import _db_write_queue
            from core.admission import store_rejection
            store_rejection({}, msg.topic,
                EvaluationResult(False, "MQTT_MESSAGE_ERROR", {"error": str(e)}))
        except Exception:
            pass


def start_mqtt_background() -> None:
    """
    Start the MQTT client, subscribe to sensor and LWT topics, and launch
    the node liveness watchdog thread.
    """
    global mqtt_client, mqtt_started
    if not MQTT_ENABLED:
        print("[MQTT] Disabled by MQTT_ENABLED=0.")
        return
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("[MQTT] paho-mqtt not installed — MQTT disabled.")
        return

    with mqtt_lock:
        if mqtt_started:
            return
        try:
            mqtt_client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                client_id="aegis-gateway",
            )
        except AttributeError:
            mqtt_client = mqtt.Client(client_id="aegis-gateway")

        try:
            mqtt_client.on_connect = on_mqtt_connect
            mqtt_client.on_message = on_mqtt_message
        except Exception:
            mqtt_client.on_connect = lambda c, u, f, rc: on_mqtt_connect(c, u, f, rc)
            mqtt_client.on_message = on_mqtt_message

        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        mqtt_started = True
        start_watchdog()
