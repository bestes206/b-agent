"""Fetcher for Building Permits (76t5-zqzr).

Actual SODA fields:
  permitnum, permitclass, permitclassmapped, permittypemapped, permittypedesc,
  description, housingunits, housingunitsremoved, housingunitsadded,
  estprojectcost, applieddate, issueddate, expiresdate, statuscurrent,
  originaladdress1, originalcity, originalstate, originalzip,
  link, latitude, longitude, location1
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pipeline.config import DATASETS, WEST_SEATTLE_ZIPS
from pipeline.fetchers.base import SODAFetcher


class PermitsFetcher(SODAFetcher):
    dataset_id = DATASETS["permits"]
    source_name = "permits"

    def build_where_clause(self) -> str:
        zips = ",".join(f"'{z}'" for z in WEST_SEATTLE_ZIPS)
        # Focus on expired, canceled, or demolition permits
        return (
            f"originalzip in({zips}) AND "
            f"(statuscurrent = 'Expired' OR statuscurrent = 'Canceled' OR "
            f"upper(description) like '%DEMOLISH%' OR upper(description) like '%DEMOLITION%')"
        )

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
        record_id = record.get("permitnum") or record.get(":id", "")
        status = (record.get("statuscurrent") or "").upper().strip()
        description = (record.get("description") or "").upper()

        # Determine signal type
        if "DEMOLISH" in description or "DEMOLITION" in description:
            signal_type = "demolished"
        elif status == "EXPIRED":
            try:
                cost = float(record.get("estprojectcost") or 0)
            except (ValueError, TypeError):
                cost = 0
            signal_type = "expired_permit_major" if cost > 50000 else "expired_permit_minor"
        elif status == "CANCELED":
            signal_type = "permit_cancelled"
        else:
            signal_type = "expired_permit_minor"

        return [{
            "source_record_id": str(record_id),
            "signal_type": signal_type,
            "detail": {
                "status": status,
                "description": record.get("description", ""),
                "est_cost": record.get("estprojectcost"),
                "permit_type": record.get("permittypedesc") or record.get("permittypemapped", ""),
            },
            "event_date": record.get("applieddate") or record.get("issueddate"),
        }]
