"""Base SODA fetcher with pagination and rate limiting."""

from __future__ import annotations

import time
from typing import Any, Dict, Generator, List, Optional

import requests

from pipeline.config import (
    SODA_APP_TOKEN,
    SODA_BASE,
    SODA_PAGE_SIZE,
    SODA_RATE_LIMIT_DELAY,
)


class SODAFetcher:
    """Base class for fetching data from Seattle's SODA API."""

    dataset_id: str = ""
    source_name: str = ""

    def __init__(self):
        self.session = requests.Session()
        if SODA_APP_TOKEN:
            self.session.headers["X-App-Token"] = SODA_APP_TOKEN

    @property
    def endpoint(self) -> str:
        return f"{SODA_BASE}/{self.dataset_id}.json"

    def build_where_clause(self) -> str:
        """Override in subclasses to provide the $where filter."""
        raise NotImplementedError

    def paginate(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Yield pages of records from the SODA API."""
        where = self.build_where_clause()
        offset = 0

        while True:
            params = {
                "$where": where,
                "$limit": SODA_PAGE_SIZE,
                "$offset": offset,
                "$order": ":id",
            }
            resp = self.session.get(self.endpoint, params=params)
            resp.raise_for_status()
            records = resp.json()

            if not records:
                break

            yield records
            offset += len(records)

            if len(records) < SODA_PAGE_SIZE:
                break

            time.sleep(SODA_RATE_LIMIT_DELAY)

    def fetch_all(self) -> List[Dict[str, Any]]:
        """Fetch all matching records (use paginate() for streaming)."""
        all_records = []
        for page in self.paginate():
            all_records.extend(page)
        return all_records

    def extract_address(self, record: Dict[str, Any]) -> Optional[str]:
        """Override to extract the raw address from a record."""
        raise NotImplementedError

    def extract_coords(self, record: Dict[str, Any]) -> tuple:
        """Override to extract (lat, lng) from a record. Returns (None, None) if unavailable."""
        return (None, None)

    def extract_zip(self, record: Dict[str, Any]) -> Optional[str]:
        """Override to extract zip code from a record."""
        return None

    def extract_signals(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Override to extract signal dicts from a record.

        Each dict should have: source_record_id, signal_type, detail, event_date
        """
        raise NotImplementedError
