"""Fetcher for Fire 911 Calls (kzjm-xkqj) — geographic filter."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pipeline.config import (
    DATASETS,
    FIRE_CENTER_LAT,
    FIRE_CENTER_LNG,
    FIRE_RADIUS_METERS,
)
from pipeline.fetchers.base import SODAFetcher

# Fire incident types that indicate property distress
_FIRE_TYPES = {
    "RESIDENTIAL FIRE",
    "BUILDING FIRE",
    "FIRE IN BUILDING",
    "FIRE IN SINGLE FAMILY RES",
    "FIRE IN MULTI FAMILY RES",
    "FIRE, RESIDENTIAL",
    "STRUCTURE FIRE",
}


class FireCallsFetcher(SODAFetcher):
    dataset_id = DATASETS["fire_911"]
    source_name = "fire_911"

    def build_where_clause(self) -> str:
        return (
            f"within_circle(report_location, {FIRE_CENTER_LAT}, {FIRE_CENTER_LNG}, {FIRE_RADIUS_METERS})"
            f" AND ("
            + " OR ".join(
                f"upper(type) like '%{t}%'" for t in [
                    "RESIDENTIAL FIRE", "BUILDING FIRE", "STRUCTURE FIRE",
                    "FIRE IN BUILDING", "FIRE IN SINGLE", "FIRE IN MULTI",
                ]
            )
            + ")"
        )

    def extract_address(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("address")

    def extract_coords(self, record: Dict[str, Any]) -> tuple:
        lat = record.get("latitude")
        lng = record.get("longitude")
        if lat and lng:
            try:
                return (float(lat), float(lng))
            except (ValueError, TypeError):
                pass
        # Try report_location nested object
        loc = record.get("report_location")
        if loc and isinstance(loc, dict):
            try:
                return (float(loc.get("latitude", 0)), float(loc.get("longitude", 0)))
            except (ValueError, TypeError):
                pass
        return (None, None)

    def extract_zip(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("zipcode")

    def extract_signals(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        record_id = record.get("incident_number") or record.get(":id", "")
        incident_type = (record.get("type") or "").upper().strip()

        if any(t in incident_type for t in ("RESIDENTIAL", "SINGLE FAMILY", "MULTI FAMILY")):
            signal_type = "residential_fire"
        else:
            signal_type = "building_fire"

        return [{
            "source_record_id": str(record_id),
            "signal_type": signal_type,
            "detail": {
                "type": incident_type,
                "datetime": record.get("datetime"),
            },
            "event_date": record.get("datetime"),
        }]
