"""
config.py
=========
Central configuration for the AEGIS gateway.
All values are read from environment variables at import time.
Load a .env file before importing this module, or set variables in the shell.
"""

import os
import hashlib

# ── Database ──────────────────────────────────────────────────────────────────

DB_PATH = os.environ.get("AEGIS_DB", "/mnt/aegis_ssd/aegis_gateway.db")

# ── MQTT ──────────────────────────────────────────────────────────────────────

MQTT_ENABLED      = os.environ.get("MQTT_ENABLED",   "1") == "1"
MQTT_BROKER       = os.environ.get("MQTT_BROKER",    "127.0.0.1")
MQTT_PORT         = int(os.environ.get("MQTT_PORT",  "1883"))
MQTT_TOPIC        = os.environ.get("MQTT_TOPIC",     "sensors/+")
LWT_TOPIC_PREFIX  = os.environ.get("LWT_TOPIC_PREFIX", "aegis/status")

# ── Enrollment ────────────────────────────────────────────────────────────────

AUTO_ENROLL = os.environ.get("AEGIS_AUTO_ENROLL", "1") == "1"

# ── Enforcement thresholds ────────────────────────────────────────────────────

# §3.7.1 — L1 inter-arrival jitter tolerance in milliseconds.
# Packets whose inter-arrival deviates from the enrolled interval by more
# than this value are rejected with BOUNDED_DELAY_INTERARRIVAL.
JITTER_TOLERANCE_MS = int(os.environ.get("JITTER_TOLERANCE_MS", "500"))

# §3.8.3 — Temporal density rolling window and deviation tolerance.
DENSITY_WINDOW_SECONDS = int(os.environ.get("DENSITY_WINDOW_SECONDS", "60"))
DENSITY_TOLERANCE      = int(os.environ.get("DENSITY_TOLERANCE",      "5"))

# Watchdog: mark a node INACTIVE after (interval × multiplier) ms of silence.
NODE_INACTIVITY_MULTIPLIER = 5

# §3.9.1 — Genesis hash: SHA-256("GENESIS"), used as prev_hash for the first record.
GENESIS_HASH: str = hashlib.sha256("GENESIS".encode("utf-8")).hexdigest()

# ── Merkle and Solana anchoring ───────────────────────────────────────────────

MERKLE_BATCH_SIZE   = int(os.environ.get("MERKLE_BATCH_SIZE",     "20"))
ANCHOR_INTERVAL_MIN = int(os.environ.get("ANCHOR_INTERVAL_MIN",   "10"))
SOLANA_PRIVATE_KEY  = os.environ.get("SOLANA_PRIVATE_KEY",  "")
SOLANA_RPC_URL      = os.environ.get("SOLANA_RPC_URL",
                                     "https://api.devnet.solana.com")

# ── Flask ─────────────────────────────────────────────────────────────────────

# Fallback SECRET_KEY derived from DB_PATH so the same gateway always produces
# the same key without requiring an explicit env var.
_fallback_key = hashlib.sha256(f"aegis-{DB_PATH}".encode()).hexdigest()
SECRET_KEY        = os.environ.get("SECRET_KEY",        _fallback_key)
OPERATOR_PASSWORD = os.environ.get("OPERATOR_PASSWORD", "aegis2024")

# ── Packet-rate tracking ──────────────────────────────────────────────────────

RATE_WINDOW_SECONDS = 30  # rolling window used to compute packets/second per node
