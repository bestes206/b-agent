"""Scoring engine — loads scoring_config.yaml, computes property scores and tiers."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import yaml

from pipeline.config import SCORING_CONFIG_PATH


def load_config() -> Dict[str, Any]:
    with open(SCORING_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def score_property(conn: sqlite3.Connection, property_id: int, config: Dict[str, Any]) -> tuple:
    """Compute total score and tier for a property based on its signals.

    Returns (total_score, tier).
    """
    weights = config.get("signal_weights", {})
    bonuses = config.get("bonuses", {})
    recency = config.get("recency", {})
    tiers = config.get("tiers", {})

    cutoff_days = recency.get("cutoff_days", 365)
    boost = recency.get("boost_multiplier", 1.5)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=cutoff_days)

    rows = conn.execute(
        "SELECT signal_type, event_date, source FROM signals WHERE property_id = ?",
        (property_id,),
    ).fetchall()

    total = 0.0
    sources = set()

    for row in rows:
        signal_type = row["signal_type"]
        base_weight = weights.get(signal_type, 1)

        # Recency boost
        event_date_str = row["event_date"]
        multiplier = 1.0
        if event_date_str:
            try:
                event_date = datetime.fromisoformat(event_date_str.replace("Z", "+00:00"))
                if event_date.tzinfo is None:
                    event_date = event_date.replace(tzinfo=timezone.utc)
                if event_date > cutoff_date:
                    multiplier = boost
            except (ValueError, TypeError):
                pass

        total += base_weight * multiplier
        sources.add(row["source"])

    # Multi-source bonus
    threshold = bonuses.get("multi_source_threshold", 2)
    bonus_points = bonuses.get("multi_source_points", 5)
    if len(sources) >= threshold:
        total += bonus_points

    # Tier assignment
    tier_a = tiers.get("A", 25)
    tier_b = tiers.get("B", 12)
    if total >= tier_a:
        tier = "A"
    elif total >= tier_b:
        tier = "B"
    else:
        tier = "C"

    return (total, tier)


def rescore_all(conn: sqlite3.Connection, config: Dict[str, Any] = None):
    """Rescore all properties. Used after fetching or when tuning weights."""
    if config is None:
        config = load_config()

    property_ids = conn.execute("SELECT id FROM properties").fetchall()

    for row in property_ids:
        pid = row["id"]
        total, tier = score_property(conn, pid, config)
        conn.execute(
            "UPDATE properties SET total_score = ?, tier = ? WHERE id = ?",
            (total, tier, pid),
        )

    conn.commit()
