"""
db/connection.py
================
SQLite connection factory and lightweight query utilities.
All connection-level PRAGMAs are applied here on every new connection.
WAL mode is set once in schema.init_db() and persists in the database file.
"""

import sqlite3
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    """
    Open a SQLite connection with per-connection performance PRAGMAs.

    synchronous=NORMAL  — durable enough for WAL mode; avoids full fsync on every commit.
    cache_size=-8000    — 8 MB page cache per connection.
    temp_store=MEMORY   — keep temporary tables in RAM.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-8000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def query_rows(sql: str, params: tuple = ()):
    """Execute a SELECT and return all rows. Opens and closes its own connection."""
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows
