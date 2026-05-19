"""
core/merkle.py
==============
Merkle tree construction, time-based batch creation, and Solana Devnet anchoring.

Batching strategy (§3.9.2, FR8):
  Every ANCHOR_INTERVAL_MIN minutes, all admitted records without a batch ID are
  collected, their curr_hash values are used as Merkle leaves, a tree root is
  computed, and the root is published to Solana as a memo transaction.

Memory safety:
  Records are read in chunks of 5,000 rows to avoid loading large datasets into
  memory during 24-hour runs that may exceed 50,000+ records.
"""

import hashlib
from typing import Optional, Tuple

from config import SOLANA_PRIVATE_KEY, SOLANA_RPC_URL, ANCHOR_INTERVAL_MIN
from db.connection import get_conn
from db.schema import utc_now_iso


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_merkle_root(leaves: list) -> str:
    """
    Build a binary Merkle tree from a list of leaf hashes and return the root.
    Odd-length levels are padded by duplicating the last leaf.
    An empty leaf list returns SHA-256("EMPTY").
    """
    if not leaves:
        return sha256_hex("EMPTY")
    level = leaves[:]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])
        level = [sha256_hex(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
    return level[0]


def create_time_based_merkle_batch() -> Optional[int]:
    """
    Collect all unanchored admitted records and group them into one Merkle batch.
    Records are read in chunks to bound memory usage.
    Returns the new batch ID, or None if there were no unanchored records.
    """
    CHUNK_SIZE        = 5000
    log_ids           = []
    leaves            = []
    interval_start_ms = None
    interval_end_ms   = None
    offset            = 0

    while True:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT id, curr_hash, gateway_received_ms
            FROM verification_log
            WHERE merkle_batch_id IS NULL AND enforcement_status='ADMITTED'
            ORDER BY id ASC LIMIT ? OFFSET ?""",
            (CHUNK_SIZE, offset))
        chunk = cur.fetchall()
        conn.close()
        if not chunk:
            break
        for r in chunk:
            log_ids.append(r[0])
            leaves.append(r[1])
            if interval_start_ms is None and r[2]:
                interval_start_ms = r[2]
            if r[2]:
                interval_end_ms = r[2]
        offset += CHUNK_SIZE
        if len(chunk) < CHUNK_SIZE:
            break

    if not log_ids:
        print("[MERKLE] No unanchored records — skipping batch.")
        return None

    merkle_root = compute_merkle_root(leaves)
    now         = utc_now_iso()
    conn        = get_conn()
    cur         = conn.cursor()
    cur.execute("""
        INSERT INTO merkle_batches
            (start_log_id, end_log_id, record_count, merkle_root,
             interval_start_ms, interval_end_ms, anchor_status, created_at)
        VALUES (?,?,?,?,?,?,'LOCAL_ONLY',?)""",
        (log_ids[0], log_ids[-1], len(log_ids), merkle_root,
         interval_start_ms, interval_end_ms, now))
    batch_id = cur.lastrowid

    UPDATE_CHUNK = 900
    for i in range(0, len(log_ids), UPDATE_CHUNK):
        chunk_ids = log_ids[i:i + UPDATE_CHUNK]
        cur.execute(
            f"UPDATE verification_log SET merkle_batch_id=? "
            f"WHERE id IN ({','.join(['?'] * len(chunk_ids))})",
            [batch_id] + chunk_ids)
    conn.commit()
    conn.close()
    print(f"[MERKLE] Batch #{batch_id} — {len(log_ids)} records — root={merkle_root[:16]}…")
    return batch_id


def anchor_batch_to_solana(batch_id: int, merkle_root: str) -> Tuple[str, str]:
    """
    Submit a Merkle root to Solana Devnet via the SPL Memo program.
    Memo format: AEGIS:<batch_id>:<merkle_root>
    Returns (anchor_status, anchor_ref).
    """
    if not SOLANA_PRIVATE_KEY:
        return "ANCHOR_FAILED", "NO_PRIVATE_KEY_CONFIGURED"
    try:
        from solders.keypair     import Keypair
        from solders.pubkey      import Pubkey
        from solders.instruction import Instruction, AccountMeta
        from solders.message     import Message
        from solders.transaction import Transaction
        from solana.rpc.api      import Client

        keypair = Keypair.from_base58_string(SOLANA_PRIVATE_KEY)
        client  = Client(SOLANA_RPC_URL)

        MEMO_PROGRAM_ID = Pubkey.from_string("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
        memo_text = f"AEGIS:{batch_id}:{merkle_root}"
        memo_ix   = Instruction(
            program_id=MEMO_PROGRAM_ID,
            accounts=[AccountMeta(pubkey=keypair.pubkey(), is_signer=True, is_writable=False)],
            data=memo_text.encode("utf-8"),
        )
        recent    = client.get_latest_blockhash()
        blockhash = recent.value.blockhash
        msg       = Message.new_with_blockhash([memo_ix], keypair.pubkey(), blockhash)
        tx        = Transaction.new_unsigned(msg)
        tx.sign([keypair], blockhash)
        result    = client.send_transaction(tx)
        tx_sig    = str(result.value)
        print(f"[SOLANA] Batch #{batch_id} anchored → {tx_sig[:20]}…")
        return "SOLANA_DEVNET", tx_sig

    except ImportError:
        return "ANCHOR_FAILED", "solana/solders not installed"
    except Exception as e:
        print(f"[SOLANA] Anchor failed for batch #{batch_id}: {e}")
        return "ANCHOR_FAILED", str(e)[:200]


def anchor_pending_batches() -> None:
    """Anchor all LOCAL_ONLY or ANCHOR_FAILED batches to Solana Devnet."""
    conn    = get_conn()
    cur     = conn.cursor()
    pending = cur.execute("""
        SELECT id, merkle_root, retry_count FROM merkle_batches
        WHERE anchor_status IN ('LOCAL_ONLY','ANCHOR_FAILED') ORDER BY id""").fetchall()
    conn.close()
    if not pending:
        return
    print(f"[SOLANA] Anchoring {len(pending)} pending batch(es)…")
    results = []
    for batch in pending:
        status, ref = anchor_batch_to_solana(batch["id"], batch["merkle_root"])
        new_retries = batch["retry_count"] + (1 if status == "ANCHOR_FAILED" else 0)
        results.append((status, ref, new_retries, batch["id"]))
    conn = get_conn()
    for row in results:
        conn.execute(
            "UPDATE merkle_batches SET anchor_status=?, anchor_ref=?, retry_count=? WHERE id=?",
            row)
    conn.commit()
    conn.close()


def run_merkle_and_anchor() -> None:
    """Combined scheduler job: create batch, then anchor pending batches."""
    create_time_based_merkle_batch()
    anchor_pending_batches()


def start_anchor_scheduler():
    """Start the APScheduler background job for Merkle batch creation and anchoring."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(
            run_merkle_and_anchor, "interval",
            minutes=ANCHOR_INTERVAL_MIN, id="merkle_anchor",
            max_instances=1, misfire_grace_time=60,
        )
        scheduler.start()
        print(f"[AEGIS] Solana anchoring scheduler started — every {ANCHOR_INTERVAL_MIN} min")
        return scheduler
    except ImportError:
        print("[AEGIS] APScheduler not installed — Solana anchoring disabled.")
        return None
