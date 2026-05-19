# AEGIS — Acquisition-Time Enforcement Gateway for IoT Systems

AEGIS is an edge-based IoT data acquisition and enforcement gateway built for a Raspberry Pi 5. It ingests sensor data from ESP32 nodes via MQTT, applies a multi-layer enforcement pipeline to each packet, builds cryptographic hash chains over admitted records, constructs Merkle trees at timed intervals, and anchors the Merkle roots to the Solana Devnet blockchain as immutable audit records.

Developed as an undergraduate thesis project at Cebu Technological University.

---

## Architecture

```
ESP32 Nodes (DHT11)
      │  MQTT (paho)
      ▼
Mosquitto Broker  ──────────────────────────────────────────────────────┐
      │                                                                  │
      ▼                                                                  │
MQTT Handler          ──► Enforcement Engine  ──► Async DB Write Queue  │
(mqtt/handler.py)          (core/enforcement.py)    (db/writer.py)      │
      │                         │                        │              │
      │               EvaluationResult              SQLite (WAL)        │
      │               ADMITTED / REJECTED                │              │
      │                                            verification_log     │
      │                                            rejection_log        │
      │                                            admin_events         │
      │                                                 │               │
      │                                          APScheduler            │
      │                                          (core/merkle.py)       │
      │                                                 │               │
      │                                         Merkle Tree             │
      │                                         SHA-256 root            │
      │                                                 │               │
      │                                         Solana Devnet           │
      │                                         (Memo Program)          │
      │                                                                  │
      └──────────────────────── Flask Dashboard (app.py) ───────────────┘
                                REST API + HTML/JS UI
```

---

## Enforcement Pipeline

Each incoming MQTT packet passes through the following layers in order:

| Layer | Reason Code | Description |
|---|---|---|
| Pre-L1 | `MALFORMED_PACKET` | Required JSON field missing |
| Pre-L1 | `UNENROLLED_NODE` | node_id not in enrollment registry |
| Pre-L1 | `NODE_NOT_ACTIVE` | Node deactivated by operator |
| L1 | `BOUNDED_DELAY_CLOCK` | Timestamp too old, future, or beyond threshold |
| L1 | `BOUNDED_DELAY_INTERARRIVAL` | Arrival timing deviates beyond jitter tolerance |
| L2 | `SEQ_RETROGRADE` | Sequence number went backward (duplicate/replay) |
| L2 | `SEQ_GAP` | Sequence number skipped (missing packets) |
| L3 | `SESSION_CONTINUITY_VIOLATION` | Node reconnected without operator acknowledgement |

Admitted packets also receive non-gating anomaly flags (`anomaly_range_flag`, `anomaly_roc_flag`, `anomaly_density_flag`) that are stored but do not block admission.

---

## Project Structure

```
aegis/
├── app.py                  # Flask application factory and startup entry point
├── config.py               # All environment variables and constants
│
├── core/
│   ├── enforcement.py      # evaluate_packet() — stateless multi-layer enforcement
│   ├── admission.py        # store_admission/rejection, process_packet, hash chain
│   ├── cache.py            # In-memory caches for enrollment, session, and last hash
│   ├── merkle.py           # Merkle tree construction, batch creation, Solana anchoring
│   └── watchdog.py         # Node liveness watchdog daemon thread
│
├── db/
│   ├── connection.py       # get_conn() — SQLite connection with PRAGMAs
│   ├── schema.py           # init_db(), session helpers, export bounds
│   └── writer.py           # Async DB write queue and background writer thread
│
├── mqtt/
│   └── handler.py          # MQTT connect/message callbacks and startup
│
├── api/
│   ├── auth.py             # login_required decorator and /api/auth/* routes
│   ├── admin.py            # Node management routes (/api/admin/*)
│   ├── dashboard.py        # Read-only stats, logs, trends, health routes
│   ├── recording.py        # Recording session routes (/api/recording/*)
│   ├── export.py           # CSV/JSON export routes (/api/export/*)
│   └── workspace.py        # Merkle verification workspace (/api/workspace/*)
│
├── utils/
│   └── csv_helpers.py      # Excel-safe CSV formatting utilities
│
├── templates/
│   └── index.html          # Dashboard single-page application
│
├── requirements.txt
├── .env.example
└── README.md
```

---

## Installation

### Requirements

- Raspberry Pi 5 running Raspberry Pi OS Lite 64-bit
- Python 3.11+
- Mosquitto MQTT broker
- Chrony (NTP server for ESP32 clock synchronization)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/seroo-gab/aegis.git
cd aegis

# 2. Create virtual environment
python3 -m venv ~/aegis-env
source ~/aegis-env/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
nano .env   # fill in MQTT_BROKER, OPERATOR_PASSWORD, SECRET_KEY, SOLANA_PRIVATE_KEY

# 5. Mount SSD and verify path
sudo mkdir -p /mnt/aegis_ssd
sudo mount /dev/sda1 /mnt/aegis_ssd

# 6. Run
export $(cat .env | xargs)
python app.py
```

### Mosquitto configuration

```
# /etc/mosquitto/mosquitto.conf
listener 1883
allow_anonymous true
```

### Chrony (LAN NTP server for ESP32 nodes)

```bash
echo "allow 192.168.1.0/24" | sudo tee -a /etc/chrony/chrony.conf
sudo systemctl restart chrony
```

---

## ESP32 Firmware

Each ESP32 node publishes JSON packets to `sensors/<node_id>` at the enrolled interval:

```json
{
  "node_id": "Indoor_Temp_Sensor_1",
  "seq_num": 1234,
  "timestamp": 1778079801487,
  "sensor_value": 29.3,
  "humidity": 65.0
}
```

Key firmware requirements:
- NTP server must be set to the gateway IP (`192.168.1.150`) for sub-5ms clock divergence
- `timestamp` must be captured **before** sensor read to avoid DHT11 retry jitter
- `seq_num` must roll back on publish failure to maintain continuity

---

## Database Schema

| Table | Description |
|---|---|
| `enrollment_registry` | Enrolled nodes with enforcement parameters |
| `session_state` | Per-node MQTT and sequence tracking |
| `verification_log` | All admitted packets with hash chain and anomaly flags |
| `rejection_log` | All rejected packets with enforcement reason |
| `admin_events` | Operator actions and system events, hash-chained |
| `merkle_batches` | Batch records with Merkle roots and Solana tx references |
| `recording_sessions` | Session boundaries for dashboard scoping |

---

## Blockchain Anchoring

Every `ANCHOR_INTERVAL_MIN` minutes, APScheduler:

1. Collects all admitted records without a `merkle_batch_id`
2. Uses their `curr_hash` values as Merkle tree leaves
3. Computes the SHA-256 Merkle root
4. Submits a Solana Devnet memo transaction: `AEGIS:<batch_id>:<merkle_root>`
5. Updates the batch record with `anchor_status=SOLANA_DEVNET` and the transaction signature

Verify on [Solana Explorer](https://explorer.solana.com/?cluster=devnet) using the stored transaction signature.

---

## Dashboard

Access the dashboard at `http://<pi-ip>:5000` after starting the gateway.

Features:
- Real-time node status, packet rates, and enforcement statistics
- Verification log and rejection log with filtering
- Sensor trends and latency histograms
- Merkle Workspace — upload a verification log CSV for cryptographic verification against the live database and Solana anchors
- Export Center — session-scoped CSV/JSON exports for all tables
- Operator Tools — node enrollment, session management, and recording sessions

---

## Tech Stack

| Layer | Technology |
|---|---|
| Sensor nodes | ESP32 DevKit-D, DHT11, C++/Arduino |
| Transport | MQTT over WiFi (Mosquitto) |
| Gateway OS | Raspberry Pi OS Lite 64-bit |
| Time sync | Chrony (Stratum 3 LAN NTP) |
| Backend | Python 3, Flask, APScheduler, paho-mqtt |
| Storage | SQLite (WAL mode) on USB SSD |
| Cryptography | SHA-256 hash chain, Merkle trees |
| Blockchain | Solana Devnet, SPL Memo Program |
| Remote access | Cloudflare Tunnel |

---

## License

Academic thesis project — Cebu Technological University, 2024–2025.
