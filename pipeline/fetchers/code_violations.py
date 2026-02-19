"""Fetcher for Code Complaints & Violations (ez4a-iug7).

Actual SODA fields:
  recordnum, recordtype, recordtypemapped, recordtypedesc, description,
  opendate, lastinspdate, lastinspresult, statuscurrent,
  originaladdress1, originalcity, originalstate, originalzip,
  link, latitude, longitude, location1
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pipeline.config import DATASETS, WEST_SEATTLE_ZIPS
from pipeline.fetchers.base import SODAFetcher

# Map recordtypedesc / recordtypemapped values to signal types
_SIGNAL_MAP = {
    "UNFIT FOR HABITATION": "unfit_building",
    "VACANT BUILDING": "vacant_building",
    "NOTICE OF VIOLATION": "notice_of_violation",
    "CITATION": "citation",
}


class CodeViolationsFetcher(SODAFetcher):
    dataset_id = DATASETS["code_violations"]
    source_name = "code_violations"

    def build_where_clause(self) -> str:
        zips = ",".join(f"'{z}'" for z in WEST_SEATTLE_ZIPS)
        return f"originalzip in({zips})"

    def extract_address(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("originaladdress1")

    def extract_coords(self, record: Dict[str, Any]) -> tuple:
        lat = record.get("latitude")
        lng = record.get("longitude")
        if lat and lng:
            try:
                return (float(lat), float(lng))
            except (ValueError, TypeError):
                pass
        return (None, None)

    def extract_zip(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("originalzip")

    def extract_signals(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        record_id = record.get("recordnum") or record.get(":id", "")
        record_type = (record.get("recordtypedesc") or record.get("recordtypemapped") or "").upper().strip()
        status = (record.get("statuscurrent") or "").upper().strip()

        # Determine signal type from record type description
        signal_type = None
        for key, stype in _SIGNAL_MAP.items():
            if key in record_type:
                signal_type = stype
                break

        if not signal_type and "CONSTRUCTION" in record_type:
            signal_type = "complaint_construction"
        elif not signal_type and "LANDLORD" in record_type:
            signal_type = "complaint_landlord_tenant"
        elif not signal_type:
            signal_type = "complaint_other"

        # Boost for NOV/citation in status regardless of record type
        signals = []
        if "NOTICE OF VIOLATION" in status and signal_type != "notice_of_violation":
            signals.append({
                "source_record_id": f"{record_id}_nov",
                "signal_type": "notice_of_violation",
                "detail": {"record_type": record_type, "status": status},
                "event_date": record.get("opendate"),
            })
        if "CITATION" in status and signal_type != "citation":
            signals.append({
                "source_record_id": f"{record_id}_citation",
                "signal_type": "citation",
                "detail": {"record_type": record_type, "status": status},
                "event_date": record.get("opendate"),
            })

        signals.append({
            "source_record_id": str(record_id),
            "signal_type": signal_type,
            "detail": {
                "record_type": record_type,
                "status": status,
                "description": record.get("description", ""),
                "last_inspection_result": record.get("lastinspresult", ""),
            },
            "event_date": record.get("opendate"),
        })

        return signals
