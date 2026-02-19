"""SQLite schema, upsert helpers, and query utilities."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from pipeline.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address_raw TEXT,
            address_norm TEXT UNIQUE NOT NULL,
            zip_code TEXT,
            latitude REAL,
            longitude REAL,
            property_type TEXT DEFAULT 'unknown',
            total_score REAL DEFAULT 0,
            tier TEXT DEFAULT 'C',
            first_seen TEXT NOT NULL,
            last_updated TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_id INTEGER NOT NULL REFERENCES properties(id),
            source TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            signal_weight REAL DEFAULT 0,
            detail TEXT,
            event_date TEXT,
            fetched_at TEXT NOT NULL,
            UNIQUE(source, source_record_id)
        );

        CREATE INDEX IF NOT EXISTS idx_signals_property ON signals(property_id);
        CREATE INDEX IF NOT EXISTS idx_signals_source ON signals(source);

        CREATE TABLE IF NOT EXISTS normalization_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address_raw TEXT,
            address_norm TEXT,
            source TEXT,
            latitude REAL,
            longitude REAL,
            nearest_property_id INTEGER,
            distance_meters REAL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            sources TEXT,
            properties_count INTEGER,
            signals_count INTEGER,
            status TEXT DEFAULT 'running'
        );
    """)
    conn.commit()


def upsert_property(conn: sqlite3.Connection, address_raw: str, address_norm: str,
                     zip_code: str = None, lat: float = None, lng: float = None,
                     property_type: str = "unknown") -> int:
    """Insert or update a property, returning its ID."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """INSERT INTO properties (address_raw, address_norm, zip_code, latitude, longitude,
                                   property_type, first_seen, last_updated)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(address_norm) DO UPDATE SET
               address_raw = COALESCE(excluded.address_raw, properties.address_raw),
               zip_code = COALESCE(excluded.zip_code, properties.zip_code),
               latitude = COALESCE(excluded.latitude, properties.latitude),
               longitude = COALESCE(excluded.longitude, properties.longitude),
               property_type = CASE WHEN excluded.property_type != 'unknown'
                                    THEN excluded.property_type
                                    ELSE properties.property_type END,
               last_updated = excluded.last_updated
        """,
        (address_raw, address_norm, zip_code, lat, lng, property_type, now, now),
    )
    if cursor.lastrowid:
        return cursor.lastrowid
    row = conn.execute("SELECT id FROM properties WHERE address_norm = ?", (address_norm,)).fetchone()
    return row["id"]


def upsert_signal(conn: sqlite3.Connection, property_id: int, source: str,
                   source_record_id: str, signal_type: str, signal_weight: float,
                   detail: dict = None, event_date: str = None) -> bool:
    """Insert a signal if not already present. Returns True if inserted."""
    now = datetime.now(timezone.utc).isoformat()
    detail_json = json.dumps(detail) if detail else None
    try:
        conn.execute(
            """INSERT INTO signals (property_id, source, source_record_id, signal_type,
                                    signal_weight, detail, event_date, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (property_id, source, source_record_id, signal_type, signal_weight,
             detail_json, event_date, now),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def find_nearby_property(conn: sqlite3.Connection, lat: float, lng: float,
                          threshold: float = 0.0001) -> Optional[sqlite3.Row]:
    """Find a property within threshold degrees of the given coordinates."""
    if lat is None or lng is None:
        return None
    return conn.execute(
        """SELECT *, ABS(latitude - ?) + ABS(longitude - ?) AS dist
           FROM properties
           WHERE latitude IS NOT NULL AND longitude IS NOT NULL
             AND ABS(latitude - ?) < ? AND ABS(longitude - ?) < ?
           ORDER BY dist LIMIT 1""",
        (lat, lng, lat, threshold, lng, threshold),
    ).fetchone()


def log_normalization_issue(conn: sqlite3.Connection, address_raw: str, address_norm: str,
                             source: str, lat: float = None, lng: float = None,
                             nearest_property_id: int = None, distance_meters: float = None):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO normalization_issues
           (address_raw, address_norm, source, latitude, longitude,
            nearest_property_id, distance_meters, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (address_raw, address_norm, source, lat, lng, nearest_property_id,
         distance_meters, now),
    )


def start_pipeline_run(conn: sqlite3.Connection, sources: List[str]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "INSERT INTO pipeline_runs (started_at, sources, status) VALUES (?, ?, 'running')",
        (now, ",".join(sources)),
    )
    conn.commit()
    return cursor.lastrowid


def complete_pipeline_run(conn: sqlite3.Connection, run_id: int,
                           properties_count: int, signals_count: int):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE pipeline_runs SET completed_at = ?, properties_count = ?,
           signals_count = ?, status = 'completed' WHERE id = ?""",
        (now, properties_count, signals_count, run_id),
    )
    conn.commit()
