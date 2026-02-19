"""Scoring engine — loads scoring_config.yaml, computes property scores and tiers.

Supports:
- 5-year max age cutoff (signals older than max_age_days are excluded)
- Linear decay curve (newer signals worth more, smoothly decaying to 1.0x at cutoff)
- Status-aware multipliers (closed/resolved violations get reduced weight)
- Negative signal weights (active permits, recent sales subtract from score)
"""

from __future__ import annotations

import json
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
    status_multipliers = config.get("status_multipliers", {})

    max_age_days = recency.get("max_age_days", 1825)
    decay_boost = recency.get("decay_boost", 0.5)
    now = datetime.now(timezone.utc)
    max_age_cutoff = now - timedelta(days=max_age_days)

    rows = conn.execute(
        "SELECT signal_type, event_date, source, detail FROM signals WHERE property_id = ?",
        (property_id,),
    ).fetchall()

    total = 0.0
    sources = set()

    for row in rows:
        signal_type = row["signal_type"]
        base_weight = weights.get(signal_type, 1)

        # --- Max age exclusion & decay curve ---
        event_date_str = row["event_date"]
        decay_mult = 1.0

        if event_date_str:
            try:
                event_date = datetime.fromisoformat(event_date_str.replace("Z", "+00:00"))
                if event_date.tzinfo is None:
                    event_date = event_date.replace(tzinfo=timezone.utc)

                # Exclude signals older than max_age_days
                if event_date < max_age_cutoff:
                    continue

                age_days = (now - event_date).days
                # Linear decay: 1.0 + decay_boost at age 0, decaying to 1.0 at max_age
                decay_mult = 1.0 + decay_boost * max(0, 1 - age_days / max_age_days)
            except (ValueError, TypeError):
                pass
        # Signals with no date (URM, some permits) still count at base weight (decay_mult = 1.0)

        # --- Status multiplier ---
        status_mult = 1.0
        source = row["source"]
        detail_str = row["detail"]

        if detail_str and source in status_multipliers:
            try:
                detail = json.loads(detail_str)
                status = (detail.get("status") or "").lower().strip()
                if status in status_multipliers[source]:
                    status_mult = status_multipliers[source][status]
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        total += base_weight * decay_mult * status_mult
        sources.add(source)

    # Multi-source bonus
    threshold = bonuses.get("multi_source_threshold", 2)
    bonus_points = bonuses.get("multi_source_points", 5)
    if len(sources) >= threshold:
        total += bonus_points

    # Tier assignment
    tier_a = tiers.get("A", 15)
    tier_b = tiers.get("B", 8)
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
