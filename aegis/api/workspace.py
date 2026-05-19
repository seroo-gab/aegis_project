"""
api/workspace.py
================
Merkle verification workspace API routes.

These endpoints back the three-step verification pipeline in the dashboard:

  Step 1 — Field-level hash verification
    POST /api/workspace/verify-csv    — chunked CSV row comparison
    POST /api/workspace/verify-fields — direct field comparison for single-node mode

  Step 2 — Hash chain linkage
    POST /api/workspace/verify-chain-csv — server-side chain walk including admin_events

  Step 3 — Merkle root vs blockchain anchor
    POST /api/workspace/recompute-roots   — server-side Merkle root recomputation
    POST /api/workspace/fetch-all-anchors — sequential Solana RPC fetches (rate-limit safe)
    GET  /api/workspace/fetch-anchor/<id> — single batch Solana anchor fetch
"""

import time
import json as _json
from typing import Dict
from flask import Blueprint, request, jsonify

from config import SOLANA_RPC_URL
from db.connection import get_conn
from core.merkle import compute_merkle_root
from utils.csv_helpers import _CSV_FLOAT_DP

workspace_bp = Blueprint("workspace", __name__)


# ── Shared normalizers ────────────────────────────────────────────────────────
# Convert CSV string values and DB values to a comparable canonical form.
# The CSV exports timestamps as ISO 8601 strings; the DB stores them as
# Unix millisecond integers. Both sides are normalised to integers for comparison.

def _strip(v):
    if v is None:
        return ""
    s = str(v).strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    return s

def _int(v):
    s = _strip(v)
    try:
        return int(float(s)) if s not in ("", "None", "null") else None
    except Exception:
        return None

def _float_dp(v):
    s = _strip(v)
    try:
        return round(float(s), _CSV_FLOAT_DP) if s not in ("", "None", "null") else None
    except Exception:
        return None

def _str(v):
    return _strip(v)

def _ts(v):
    """Accept both Unix-ms integer and ISO 8601 string; return Unix-ms integer."""
    s = _strip(v)
    if not s or s in ("None", "null"):
        return None
    try:
        return int(float(s))
    except ValueError:
        pass
    try:
        from datetime import datetime as _dt
        return int(_dt.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return None


# Fields compared in both verify-fields and verify-csv.
# merkle_batch_id is intentionally excluded: it is NULL at admission and
# assigned later by APScheduler, so comparing it produces false tamper flags
# when a CSV is exported before the batch job runs and verified afterward.
FIELDS = [
    ("node_id",                "node_id",                _str),
    ("seq_num",                "seq_num",                _int),
    ("timestamp_value",        "timestamp_value",        _ts),
    ("sensor_value",           "sensor_value",           _float_dp),
    ("mqtt_topic",             "mqtt_topic",             _str),
    ("gateway_received_ms",    "gateway_received_ms",    _ts),
    ("delay_ms",               "delay_ms",               _int),
    ("enforcement_latency_ms", "enforcement_latency_ms", _float_dp),
    ("anomaly_range_flag",     "anomaly_range_flag",     _int),
    ("anomaly_roc_flag",       "anomaly_roc_flag",       _int),
    ("anomaly_density_flag",   "anomaly_density_flag",   _int),
    ("enforcement_status",     "enforcement_status",     _str),
    ("prev_hash",              "prev_hash",              _str),
    ("curr_hash",              "curr_hash",              _str),
    ("received_at",            "received_at",            _str),
]

DB_SELECT = (
    "SELECT id, node_id, seq_num, timestamp_value, sensor_value, mqtt_topic, "
    "gateway_received_ms, delay_ms, enforcement_latency_ms, "
    "anomaly_range_flag, anomaly_roc_flag, anomaly_density_flag, "
    "enforcement_status, prev_hash, curr_hash, merkle_batch_id, received_at "
    "FROM verification_log"
)


def _fetch_db_records(record_ids: list) -> Dict[int, dict]:
    """Batch-fetch verification_log records by ID. Chunks to stay under SQLite variable limit."""
    db_records: Dict[int, dict] = {}
    if not record_ids:
        return db_records
    CHUNK = 900
    conn  = get_conn()
    cur   = conn.cursor()
    for i in range(0, len(record_ids), CHUNK):
        chunk = record_ids[i:i + CHUNK]
        phs   = ",".join(["?"] * len(chunk))
        for db_row in cur.execute(f"{DB_SELECT} WHERE id IN ({phs})", chunk).fetchall():
            db_records[db_row["id"]] = dict(db_row)
    conn.close()
    return db_records


def _compare_rows(rows: list, db_records: Dict[int, dict]) -> list:
    """
    Compare a list of CSV rows against their DB counterparts.
    Returns a list of result dicts, one per row.
    """
    results = []
    for r in rows:
        try:
            record_id = int(float(r.get("id", 0)))
        except (TypeError, ValueError):
            results.append({"id": r.get("id"), "match": False,
                             "error": "Invalid or missing id column"})
            continue

        db_row = db_records.get(record_id)
        if db_row is None:
            results.append({
                "id":                record_id,
                "node_id":           r.get("node_id"),
                "seq_num":           r.get("seq_num"),
                "sensor_value":      r.get("sensor_value"),
                "merkle_batch_id":   r.get("merkle_batch_id"),
                "match":             False,
                "hash_modified":     None,
                "error":             f"Record id={record_id} not found in DB",
                "mismatched_fields": [],
            })
            continue

        mismatched = []
        for csv_key, db_key, norm in FIELDS:
            csv_val = norm(r.get(csv_key))
            db_val  = norm(db_row.get(db_key))
            if csv_val != db_val:
                mismatched.append({
                    "field":     csv_key,
                    "csv_value": str(r.get(csv_key)),
                    "db_value":  str(db_row.get(db_key)),
                })

        csv_hash = _str(r.get("curr_hash", ""))
        db_hash  = _str(db_row.get("curr_hash", ""))
        results.append({
            "id":                record_id,
            "node_id":           r.get("node_id"),
            "seq_num":           r.get("seq_num"),
            "sensor_value":      r.get("sensor_value"),
            "merkle_batch_id":   r.get("merkle_batch_id"),
            "match":             len(mismatched) == 0,
            "hash_modified":     csv_hash != db_hash,
            "mismatched_fields": mismatched,
        })
    return results


# ── Step 1 routes ─────────────────────────────────────────────────────────────

@workspace_bp.route("/api/workspace/verify-csv", methods=["POST"])
def api_workspace_verify_csv():
    """
    Step 1 verification for the all-nodes (full batch) workspace path.
    Receives a chunk of CSV rows and compares every field against the live DB.

    Hash recomputation from CSV values is not viable because the CSV export
    format converts timestamps to ISO 8601 and rounds floats — that precision
    cannot be recovered to reproduce the original hash. Direct DB comparison
    is strictly stronger: it catches tampering even when the attacker also
    recalculates curr_hash to match the modified data.
    """
    data = request.get_json(force=True) or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"error": "No rows provided"}), 400

    record_ids = []
    for r in rows:
        try:
            rid = int(float(r.get("id", 0)))
            if rid > 0:
                record_ids.append(rid)
        except (TypeError, ValueError):
            pass

    db_records = _fetch_db_records(record_ids)
    results    = _compare_rows(rows, db_records)
    tampered   = [r for r in results if not r["match"]]
    return jsonify({
        "total":    len(results),
        "tampered": len(tampered),
        "clean":    len(results) - len(tampered),
        "results":  results,
    })


@workspace_bp.route("/api/workspace/verify-fields", methods=["POST"])
def api_workspace_verify_fields():
    """
    Step 1 verification for the single-node (inclusion proof) workspace path.
    Same comparison logic as verify-csv, returning per-record field diffs.
    """
    data = request.get_json(force=True) or {}
    rows = data.get("rows", [])
    if not rows:
        return jsonify({"error": "No rows provided"}), 400

    record_ids = []
    for r in rows:
        try:
            rid = int(float(r.get("id", 0)))
            if rid > 0:
                record_ids.append(rid)
        except (TypeError, ValueError):
            pass

    db_records = _fetch_db_records(record_ids)
    results    = _compare_rows(rows, db_records)
    tampered   = [r for r in results if not r.get("match")]
    return jsonify({
        "total":    len(results),
        "tampered": len(tampered),
        "clean":    len(results) - len(tampered),
        "results":  results,
    })


# ── Step 2 route ──────────────────────────────────────────────────────────────

@workspace_bp.route("/api/workspace/verify-chain-csv", methods=["POST"])
def api_workspace_verify_chain_csv():
    """
    Step 2 — server-side hash chain verification.

    Client-side chain checks only have the verification_log CSV and will flag
    false chain breaks whenever an admin_event (session reset, enrollment, edit)
    intervenes between two consecutive vlog IDs — because vlog[i].prev_hash
    points to the admin_event's curr_hash, not vlog[i-1].curr_hash.

    This endpoint fetches both verification_log and admin_events from the DB,
    builds a complete curr_hash → source map, and determines for each pair
    whether the link is direct, routes through an admin_event (legitimate), or
    is genuinely broken.
    """
    data = request.get_json(force=True) or {}
    ids  = data.get("ids", [])
    if not ids:
        return jsonify({"error": "No IDs provided"}), 400

    CHUNK    = 900
    db_vlogs: Dict[int, dict] = {}
    conn     = get_conn()
    cur      = conn.cursor()
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        phs   = ",".join(["?"] * len(chunk))
        for r in cur.execute(
            f"SELECT id, node_id, seq_num, merkle_batch_id, prev_hash, curr_hash "
            f"FROM verification_log WHERE id IN ({phs})", chunk).fetchall():
            db_vlogs[r["id"]] = dict(r)

    admin_events = cur.execute(
        "SELECT id, event_type, prev_hash, curr_hash FROM admin_events ORDER BY id ASC"
    ).fetchall()
    conn.close()

    chain_map: Dict[str, dict] = {}
    for ae in admin_events:
        if ae["curr_hash"]:
            chain_map[ae["curr_hash"]] = {
                "table": "admin_events", "id": ae["id"], "event_type": ae["event_type"]
            }
    for vid, vr in db_vlogs.items():
        if vr["curr_hash"]:
            chain_map[vr["curr_hash"]] = {"table": "verification_log", "id": vid}

    sorted_ids       = sorted(db_vlogs.keys())
    pairs_checked    = 0
    pairs_skipped    = 0
    breaks           = []
    admin_intervened = []

    for i in range(1, len(sorted_ids)):
        prev_id = sorted_ids[i - 1]
        curr_id = sorted_ids[i]
        prev_r  = db_vlogs[prev_id]
        curr_r  = db_vlogs[curr_id]

        if not prev_r.get("curr_hash") or not curr_r.get("prev_hash"):
            continue

        link_target = curr_r["prev_hash"]

        if link_target == prev_r["curr_hash"]:
            pairs_checked += 1

        elif link_target in chain_map and chain_map[link_target]["table"] == "admin_events":
            pairs_skipped += 1
            admin_intervened.append({
                "prev_vlog_id":   prev_id,
                "curr_vlog_id":   curr_id,
                "admin_event_id": chain_map[link_target]["id"],
                "event_type":     chain_map[link_target]["event_type"],
                "note": (f"Admin event (id={chain_map[link_target]['id']}, "
                         f"type={chain_map[link_target]['event_type']}) "
                         f"intervened between vlog id={prev_id} and id={curr_id}"),
            })

        else:
            pairs_checked += 1
            breaks.append({
                "id":             curr_id,
                "node_id":        curr_r.get("node_id"),
                "seq_num":        curr_r.get("seq_num"),
                "merkle_batch_id":curr_r.get("merkle_batch_id"),
                "expected_prev":  prev_r["curr_hash"],
                "found_prev":     curr_r["prev_hash"],
                "note": f"prev_hash does not link to vlog id={prev_id} or any known admin_event",
            })

    return jsonify({
        "pairs_checked":    pairs_checked,
        "pairs_skipped":    pairs_skipped,
        "admin_intervened": len(admin_intervened),
        "breaks":           breaks,
        "admin_events":     admin_intervened,
        "chain_intact":     len(breaks) == 0,
    })


# ── Step 3 routes ─────────────────────────────────────────────────────────────

@workspace_bp.route("/api/workspace/recompute-roots", methods=["POST"])
def api_workspace_recompute_roots():
    """
    Server-side Merkle root recomputation from CSV curr_hash leaves.
    Called by Step 3 instead of client-side crypto.subtle, which requires
    HTTPS and is unavailable over plain HTTP on a local network.

    Input:  { "batches": { "<batch_id>": ["hash1", "hash2", ...] } }
    Output: { "<batch_id>": "<recomputed_root>" }
    """
    data    = request.get_json(force=True) or {}
    batches = data.get("batches", {})
    if not batches:
        return jsonify({"error": "No batches provided"}), 400
    results = {}
    for bid, leaves in batches.items():
        try:
            results[str(bid)] = compute_merkle_root([str(h) for h in leaves if h])
        except Exception as e:
            results[str(bid)] = {"error": str(e)}
    return jsonify(results)


@workspace_bp.route("/api/workspace/fetch-all-anchors", methods=["POST"])
def api_workspace_fetch_all_anchors():
    """
    Fetch Solana anchor data for a list of batch IDs sequentially on the server.

    All RPC calls happen in a single HTTP request with a configurable delay
    between calls (default 100ms). This eliminates the race condition caused
    by the browser firing 100+ parallel requests that each create a Solana
    RPC connection, which overwhelms the devnet free-tier rate limit.
    """
    data      = request.get_json(force=True) or {}
    batch_ids = data.get("batch_ids", [])
    delay_ms  = data.get("delay_ms", 100)
    if not batch_ids:
        return jsonify({}), 200

    try:
        from solana.rpc.api  import Client as SolanaClient
        from solders.signature import Signature as SolanaSignature
    except ImportError:
        return jsonify({"error": "solana/solders not installed"}), 500

    CHUNK      = 900
    db_batches: Dict[int, dict] = {}
    conn       = get_conn()
    cur        = conn.cursor()
    for i in range(0, len(batch_ids), CHUNK):
        chunk = batch_ids[i:i + CHUNK]
        phs   = ",".join(["?"] * len(chunk))
        for r in cur.execute(
            f"SELECT id, anchor_status, anchor_ref, merkle_root FROM merkle_batches WHERE id IN ({phs})",
            chunk).fetchall():
            db_batches[r["id"]] = dict(r)
    conn.close()

    results = {}
    client  = SolanaClient(SOLANA_RPC_URL)

    for bid in batch_ids:
        batch = db_batches.get(int(bid))
        if not batch:
            results[str(bid)] = {"error": f"Batch {bid} not found"}
            continue
        if batch["anchor_status"] != "SOLANA_DEVNET":
            results[str(bid)] = {
                "batch_id": bid, "anchor_status": batch["anchor_status"],
                "db_root": batch["merkle_root"], "solana_root": None,
            }
            continue

        tx_sig = batch["anchor_ref"]
        if not tx_sig:
            results[str(bid)] = {"error": "No tx signature"}
            continue

        try:
            sig  = SolanaSignature.from_string(tx_sig)
            resp = client.get_transaction(sig, encoding="json", max_supported_transaction_version=0)
            if not resp.value:
                results[str(bid)] = {
                    "error": "Transaction not found on devnet",
                    "db_root": batch["merkle_root"],
                }
                continue

            memo_text   = None
            tx_envelope = resp.value.transaction
            meta        = tx_envelope.meta
            if meta and meta.log_messages:
                for log in meta.log_messages:
                    if "AEGIS:" in log:
                        raw       = log.split("AEGIS:")[1].strip().strip('"')
                        memo_text = "AEGIS:" + raw
                        break

            if not memo_text:
                results[str(bid)] = {
                    "batch_id": bid, "tx_sig": tx_sig,
                    "db_root": batch["merkle_root"], "solana_root": None,
                    "error": "Could not parse memo",
                    "explorer": f"https://explorer.solana.com/tx/{tx_sig}?cluster=devnet",
                }
                continue

            parts       = memo_text.split(":")
            solana_root = parts[2].strip() if len(parts) >= 3 else None
            db_root     = batch["merkle_root"]
            results[str(bid)] = {
                "batch_id":      bid,
                "tx_sig":        tx_sig,
                "db_root":       db_root,
                "solana_root":   solana_root,
                "roots_match":   solana_root == db_root if solana_root else None,
                "anchor_status": "SOLANA_DEVNET",
                "explorer":      f"https://explorer.solana.com/tx/{tx_sig}?cluster=devnet",
            }
        except Exception as e:
            results[str(bid)] = {"error": str(e)[:200], "db_root": batch.get("merkle_root")}

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

    return jsonify(results)


@workspace_bp.route("/api/workspace/fetch-anchor/<int:batch_id>")
def api_workspace_fetch_anchor(batch_id: int):
    """
    Fetch the anchored Merkle root for a single batch directly from Solana.
    Returns the root embedded in the blockchain memo, independent of the local DB.

    A fresh SolanaClient is created per request. solana-py is not thread-safe
    when a shared client instance is used across concurrent Flask threads.
    """
    conn  = get_conn()
    cur   = conn.cursor()
    batch = cur.execute("SELECT * FROM merkle_batches WHERE id=?", (batch_id,)).fetchone()
    conn.close()

    if not batch:
        return jsonify({"error": f"Batch {batch_id} not found"}), 404

    if batch["anchor_status"] != "SOLANA_DEVNET":
        return jsonify({
            "batch_id":      batch_id,
            "anchor_status": batch["anchor_status"],
            "db_root":       batch["merkle_root"],
            "solana_root":   None,
            "note":          "Batch not yet anchored to Solana — comparison uses DB root only.",
        })

    tx_sig = batch["anchor_ref"]
    if not tx_sig:
        return jsonify({"error": "No transaction signature stored"}), 400

    try:
        from solana.rpc.api   import Client as SolanaClient
        from solders.signature import Signature as SolanaSignature

        client = SolanaClient(SOLANA_RPC_URL)
        sig    = SolanaSignature.from_string(tx_sig)
        resp   = client.get_transaction(sig, encoding="json", max_supported_transaction_version=0)

        if not resp.value:
            return jsonify({
                "batch_id": batch_id, "tx_sig": tx_sig,
                "error": "Transaction not found on Solana devnet — may have been pruned.",
                "db_root": batch["merkle_root"],
            })

        memo_text = None
        try:
            tx_envelope = resp.value.transaction
            meta        = tx_envelope.meta
            if meta and meta.log_messages:
                for log in meta.log_messages:
                    if "AEGIS:" in log:
                        raw       = log.split("AEGIS:")[1].strip().strip('"')
                        memo_text = "AEGIS:" + raw
                        break
            if not memo_text:
                tx_data = tx_envelope.transaction
                msg     = tx_data.message if hasattr(tx_data, "message") else None
                if msg:
                    for ix in (msg.instructions if hasattr(msg, "instructions") else []):
                        try:
                            raw     = bytes(ix.data) if hasattr(ix, "data") else b""
                            decoded = raw.decode("utf-8")
                            if decoded.startswith("AEGIS:"):
                                memo_text = decoded
                                break
                        except Exception:
                            pass
        except Exception as parse_err:
            print(f"[WORKSPACE] Memo parse error batch={batch_id}: {parse_err}")

        if not memo_text:
            return jsonify({
                "batch_id":    batch_id, "tx_sig": tx_sig,
                "db_root":     batch["merkle_root"], "solana_root": None,
                "error":       "Could not parse memo from transaction.",
                "explorer":    f"https://explorer.solana.com/tx/{tx_sig}?cluster=devnet",
            })

        parts       = memo_text.split(":")
        solana_root = parts[2].strip() if len(parts) >= 3 else None
        db_root     = batch["merkle_root"]
        return jsonify({
            "batch_id":      batch_id,
            "tx_sig":        tx_sig,
            "db_root":       db_root,
            "solana_root":   solana_root,
            "roots_match":   (solana_root == db_root) if solana_root else None,
            "memo_text":     memo_text,
            "anchor_status": batch["anchor_status"],
            "explorer":      f"https://explorer.solana.com/tx/{tx_sig}?cluster=devnet",
        })

    except ImportError:
        return jsonify({"error": "solana/solders not installed"}), 500
    except Exception as e:
        print(f"[WORKSPACE] fetch-anchor error batch={batch_id}: {e}")
        return jsonify({"error": str(e)[:300]}), 500
