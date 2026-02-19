"""Fetcher for recent sales via Redfin CSV export.

Best-effort coverage — Redfin's gis-csv endpoint returns ~10-25 results per
zip code. This won't catch every sale but penalizes the most visible ones.
Gracefully handles empty/failed responses since Redfin may block at any time.
"""

from __future__ import annotations

import csv
import io
import time
from typing import Any, Dict, Generator, List, Optional

import requests

from pipeline.config import WEST_SEATTLE_ZIPS
from pipeline.normalize import normalize_address


class RecentSalesFetcher:
    """Fetches recent sales from Redfin's CSV export endpoint."""

    source_name = "king_county_sales"

    REDFIN_CSV_URL = "https://www.redfin.com/stingray/api/gis-csv"

    # Redfin params: region_type=2 means zip code, status=9 means sold,
    # uipt=1,2,3 means house/condo/townhouse
    REDFIN_PARAMS = {
        "al": 1,
        "market": "seattle",
        "region_type": 2,
        "sold_within_days": 365,
        "status": 9,
        "uipt": "1,2,3",
        "v": 8,
    }

    # Delay between zip code requests to avoid rate limiting
    REQUEST_DELAY = 2.0

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def paginate(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Yield one page of results per zip code."""
        for zip_code in WEST_SEATTLE_ZIPS:
            try:
                records = self._fetch_zip(zip_code)
                if records:
                    yield records
            except Exception as e:
                print(f"    Warning: Redfin fetch failed for {zip_code}: {e}", flush=True)
                continue
            time.sleep(self.REQUEST_DELAY)

    def _fetch_zip(self, zip_code: str) -> List[Dict[str, Any]]:
        """Fetch recent sales for a single zip code."""
        params = {**self.REDFIN_PARAMS, "region_id": zip_code}
        resp = self.session.get(self.REDFIN_CSV_URL, params=params, timeout=30)

        if resp.status_code != 200:
            print(f"    Redfin returned {resp.status_code} for zip {zip_code}", flush=True)
            return []

        text = resp.text.strip()
        if not text or text.startswith("<!") or text.startswith("{"):
            # HTML error page or JSON error — not CSV
            return []

        reader = csv.DictReader(io.StringIO(text))
        records = []
        for row in reader:
            # Filter to only West Seattle zips (Redfin sometimes returns nearby)
            row_zip = (row.get("ZIP OR POSTAL CODE") or "").strip()
            if row_zip not in WEST_SEATTLE_ZIPS:
                continue
            records.append(row)

        return records

    def extract_address(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("ADDRESS")

    def extract_coords(self, record: Dict[str, Any]) -> tuple:
        lat = record.get("LATITUDE")
        lng = record.get("LONGITUDE")
        if lat and lng:
            try:
                return (float(lat), float(lng))
            except (ValueError, TypeError):
                pass
        return (None, None)

    def extract_zip(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("ZIP OR POSTAL CODE")

    def extract_signals(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract a recently_sold signal from a Redfin sale record."""
        address = record.get("ADDRESS", "")
        mls = record.get("MLS#", "")
        record_id = f"redfin-{mls}" if mls else f"redfin-{address}"

        return [{
            "source_record_id": record_id,
            "signal_type": "recently_sold",
            "detail": {
                "price": record.get("PRICE"),
                "sale_type": record.get("SALE TYPE"),
                "property_type": record.get("PROPERTY TYPE"),
                "status": "sold",
            },
            "event_date": record.get("SOLD DATE"),
        }]
