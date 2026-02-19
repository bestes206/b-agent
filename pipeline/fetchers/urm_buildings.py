"""Fetcher for Unreinforced Masonry Buildings (54qs-2h7f)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pipeline.config import DATASETS, WEST_SEATTLE_ZIPS
from pipeline.fetchers.base import SODAFetcher


class URMBuildingsFetcher(SODAFetcher):
    dataset_id = DATASETS["urm"]
    source_name = "urm"

    def build_where_clause(self) -> str:
        zips = ",".join(f"'{z}'" for z in WEST_SEATTLE_ZIPS)
        return f"zip_code in({zips})"

    def extract_address(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("address") or record.get("street_address")

    def extract_coords(self, record: Dict[str, Any]) -> tuple:
        # This dataset uses geocoded_column with GeoJSON format
        geo = record.get("geocoded_column")
        if geo and isinstance(geo, dict):
            coords = geo.get("coordinates")
            if coords and len(coords) >= 2:
                try:
                    # GeoJSON is [lng, lat]
                    return (float(coords[1]), float(coords[0]))
                except (ValueError, TypeError, IndexError):
                    pass
        # Fallback to flat lat/lng fields
        lat = record.get("latitude")
        lng = record.get("longitude")
        if lat and lng:
            try:
                return (float(lat), float(lng))
            except (ValueError, TypeError):
                pass
        return (None, None)

    def extract_zip(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("zip_code")

    def extract_signals(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Use address as fallback record ID since this dataset has no unique ID field
        record_id = record.get("address") or record.get(":id", "")
        retrofit_status = (record.get("retrofit_level") or record.get("retrofit") or "").upper().strip()
        risk_category = (record.get("preliminary_risk_category") or record.get("risk_category") or "").upper().strip()

        # "NO VISIBLE RETROFIT" and "NO RETROFIT" mean NOT retrofitted
        has_retrofit = ("RETROFIT" in retrofit_status
                        and "NO" not in retrofit_status
                        and "NOT" not in retrofit_status
                        and "NONE" not in retrofit_status)

        if has_retrofit:
            signal_type = "urm_retrofitted"
        elif "HIGH" in risk_category:
            signal_type = "urm_high_risk_no_retrofit"
        else:
            signal_type = "urm_no_retrofit"

        return [{
            "source_record_id": f"urm_{record_id}",
            "signal_type": signal_type,
            "detail": {
                "retrofit_status": retrofit_status,
                "risk_category": risk_category,
                "building_use": record.get("building_use", ""),
                "year_built": record.get("year_built", ""),
                "neighborhood": record.get("neighborhood", ""),
            },
            "event_date": None,
        }]
