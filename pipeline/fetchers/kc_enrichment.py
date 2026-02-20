"""King County parcel enrichment fetcher.

Joins four KC data sources on PIN to produce ownership and financial signals:
  1. GIS Layer 2 — parcel spine (address, zip, assessed values)
  2. RPAcct CSV   — owner mailing address (absentee detection)
  3. RPSale CSV   — sales history (long-term ownership, recent sales)
  4. SODA nx4x    — foreclosure list
"""

from __future__ import annotations

import csv
import io
import json
import re
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set

import requests

from pipeline.config import (
    DOWNLOADS_DIR,
    KC_DOWNLOAD_CACHE_DAYS,
    KC_FORECLOSURE_DATASET,
    KC_GIS_PAGE_SIZE,
    KC_GIS_PARCELS_URL,
    KC_RPACCT_URL,
    KC_RPSALE_URL,
    KC_SODA_BASE,
    WEST_SEATTLE_ZIPS,
)


def _make_pin(major: str, minor: str) -> str:
    """Build 10-digit PIN from Major (6) + Minor (4), zero-padded."""
    return major.strip().zfill(6) + minor.strip().zfill(4)


def _parse_city_state(city_state: str) -> tuple:
    """Parse 'SEATTLE WA' or 'SEATTLE, WA' into (city, state)."""
    text = city_state.strip()
    if not text:
        return ("", "")
    # Try "CITY, ST" or "CITY ST" — state is always last 2 chars
    m = re.match(r"^(.+?)[,\s]+([A-Za-z]{2})$", text)
    if m:
        return (m.group(1).strip(), m.group(2).upper())
    return (text, "")


class KCEnrichmentFetcher:
    """Fetches and joins King County parcel, mailing, sales, and foreclosure data."""

    source_name = "kc_enrichment"

    def __init__(self):
        self.session = requests.Session()
        self._parcels: Dict[str, Dict] = {}      # PIN → {addr, zip, land_val, impr_val}
        self._mailing: Dict[str, Dict] = {}       # PIN → {addr, city, state, zip}
        self._sales: Dict[str, Dict] = {}          # PIN → {last_date, last_price, buyer}
        self._foreclosures: Set[str] = set()        # set of PINs
        self._loaded = False

    # ------------------------------------------------------------------
    # Public interface (matches what run.py expects)
    # ------------------------------------------------------------------

    def paginate(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Yield batches of enriched parcel records that have >= 1 signal."""
        self._load_all_data()
        batch: List[Dict[str, Any]] = []
        for pin, parcel in self._parcels.items():
            record = self._enrich(pin, parcel)
            if self._has_signals(record):
                batch.append(record)
                if len(batch) >= 500:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def extract_address(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("address")

    def extract_coords(self, record: Dict[str, Any]) -> tuple:
        return (None, None)

    def extract_zip(self, record: Dict[str, Any]) -> Optional[str]:
        return record.get("zip")

    def extract_signals(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Produce 0+ signals from an enriched parcel record."""
        signals: List[Dict[str, Any]] = []
        pin = record["pin"]
        mailing = record.get("mailing")
        sale = record.get("sale")
        land_val = record.get("land_val") or 0
        impr_val = record.get("impr_val") or 0

        # --- Absentee owner ---
        if mailing:
            state = mailing.get("state", "")
            city = mailing.get("city", "")
            if state and state != "WA":
                signals.append({
                    "source_record_id": f"kc-absentee-{pin}",
                    "signal_type": "absentee_owner_out_of_state",
                    "detail": {
                        "mailing_city": city,
                        "mailing_state": state,
                    },
                })
            elif state == "WA" and city and city.upper() != "SEATTLE":
                signals.append({
                    "source_record_id": f"kc-absentee-{pin}",
                    "signal_type": "absentee_owner_in_state",
                    "detail": {
                        "mailing_city": city,
                        "mailing_state": state,
                    },
                })

        # --- Long-term ownership ---
        if sale:
            last_date = sale.get("last_date")
            if last_date:
                try:
                    sale_dt = datetime.strptime(last_date[:10], "%m/%d/%Y")
                except ValueError:
                    try:
                        sale_dt = datetime.strptime(last_date[:10], "%Y-%m-%d")
                    except ValueError:
                        sale_dt = None

                if sale_dt:
                    age_years = (datetime.now() - sale_dt).days / 365.25
                    if age_years >= 20:
                        signals.append({
                            "source_record_id": f"kc-longterm-{pin}",
                            "signal_type": "long_term_owner_20yr",
                            "detail": {"last_sale_date": last_date, "years": round(age_years, 1)},
                            "event_date": last_date[:10],
                        })
                    elif age_years >= 10:
                        signals.append({
                            "source_record_id": f"kc-longterm-{pin}",
                            "signal_type": "long_term_owner_10yr",
                            "detail": {"last_sale_date": last_date, "years": round(age_years, 1)},
                            "event_date": last_date[:10],
                        })

                    # --- Recently sold (negative signal) ---
                    sale_price = sale.get("last_price") or 0
                    try:
                        sale_price = int(float(sale_price))
                    except (ValueError, TypeError):
                        sale_price = 0
                    if age_years < 1 and sale_price > 0:
                        signals.append({
                            "source_record_id": f"kc-sold-{pin}",
                            "signal_type": "recently_sold",
                            "detail": {
                                "price": sale_price,
                                "buyer": sale.get("buyer", ""),
                                "status": "sold",
                            },
                            "event_date": last_date[:10],
                        })
        else:
            # No sale on record at all — treat as long-term
            signals.append({
                "source_record_id": f"kc-longterm-{pin}",
                "signal_type": "long_term_owner_20yr",
                "detail": {"last_sale_date": None, "years": None},
            })

        # --- Foreclosure ---
        if pin in self._foreclosures:
            signals.append({
                "source_record_id": f"kc-foreclosure-{pin}",
                "signal_type": "foreclosure",
                "detail": {},
            })

        # --- Low improvement ratio ---
        if land_val > 0 and impr_val < land_val * 0.3:
            signals.append({
                "source_record_id": f"kc-lowimpr-{pin}",
                "signal_type": "low_improvement_ratio",
                "detail": {
                    "land_val": land_val,
                    "impr_val": impr_val,
                    "ratio": round(impr_val / land_val, 2) if land_val else 0,
                },
            })

        return signals

    # ------------------------------------------------------------------
    # Internal data loading
    # ------------------------------------------------------------------

    def _load_all_data(self):
        if self._loaded:
            return
        self._load_parcels()
        self._load_mailing()
        self._load_sales()
        self._load_foreclosures()
        self._loaded = True
        print(f"    KC data loaded: {len(self._parcels)} parcels, "
              f"{len(self._mailing)} mailing records, "
              f"{len(self._sales)} sales, "
              f"{len(self._foreclosures)} foreclosures", flush=True)

    def _load_parcels(self):
        """Paginate GIS Layer 2 for West Seattle residential parcels."""
        zips_filter = ",".join(f"'{z}'" for z in WEST_SEATTLE_ZIPS)
        where = f"ZIP5 IN ({zips_filter}) AND PROPTYPE='R'"
        offset = 0
        total = 0

        while True:
            params = {
                "where": where,
                "outFields": "PIN,ADDR_FULL,ZIP5,APPRLNDVAL,APPR_IMPR",
                "returnGeometry": "false",
                "f": "json",
                "resultRecordCount": KC_GIS_PAGE_SIZE,
                "resultOffset": offset,
                "orderByFields": "OBJECTID",
            }
            resp = self.session.get(KC_GIS_PARCELS_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                attrs = feat.get("attributes", {})
                pin = (attrs.get("PIN") or "").strip()
                if not pin:
                    continue
                self._parcels[pin] = {
                    "address": attrs.get("ADDR_FULL", ""),
                    "zip": attrs.get("ZIP5", ""),
                    "land_val": attrs.get("APPRLNDVAL") or 0,
                    "impr_val": attrs.get("APPR_IMPR") or 0,
                }

            total += len(features)
            offset += len(features)
            print(f"    GIS parcels loaded: {total}", end="\r", flush=True)

            if not data.get("exceededTransferLimit", False):
                break

            time.sleep(0.3)

        print(f"    GIS parcels loaded: {total}  ", flush=True)

    def _load_mailing(self):
        """Download RPAcct CSV, filter to West Seattle PINs."""
        zip_path = self._ensure_downloaded(KC_RPACCT_URL, "rpacct.zip")
        ws_pins = set(self._parcels.keys())
        count = 0

        with zipfile.ZipFile(zip_path) as zf:
            # Find the CSV inside the zip
            csv_name = self._find_csv_in_zip(zf, "RPAcct")
            if not csv_name:
                print("    WARNING: No RPAcct CSV found in zip", flush=True)
                return

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
                for row in reader:
                    pin = _make_pin(row.get("Major", ""), row.get("Minor", ""))
                    if pin not in ws_pins:
                        continue

                    city_state_raw = row.get("CityState", "")
                    city, state = _parse_city_state(city_state_raw)

                    self._mailing[pin] = {
                        "addr": row.get("AddrLine", "").strip(),
                        "city": city,
                        "state": state,
                        "zip": row.get("ZipCode", "").strip()[:5],
                    }
                    count += 1

        print(f"    RPAcct mailing records matched: {count}", flush=True)

    def _load_sales(self):
        """Download RPSale CSV, compute last sale per West Seattle PIN."""
        zip_path = self._ensure_downloaded(KC_RPSALE_URL, "rpsale.zip")
        ws_pins = set(self._parcels.keys())
        rows_read = 0

        with zipfile.ZipFile(zip_path) as zf:
            csv_name = self._find_csv_in_zip(zf, "RPSale")
            if not csv_name:
                print("    WARNING: No RPSale CSV found in zip", flush=True)
                return

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
                for row in reader:
                    rows_read += 1
                    pin = _make_pin(row.get("Major", ""), row.get("Minor", ""))
                    if pin not in ws_pins:
                        continue

                    doc_date = (row.get("DocumentDate") or "").strip()
                    if not doc_date:
                        continue

                    # Keep the most recent sale per PIN
                    existing = self._sales.get(pin)
                    if existing and existing["last_date"] >= doc_date:
                        continue

                    self._sales[pin] = {
                        "last_date": doc_date,
                        "last_price": row.get("SalePrice", "0"),
                        "buyer": (row.get("BuyerName") or "").strip(),
                    }

                    if rows_read % 500_000 == 0:
                        print(f"    RPSale rows scanned: {rows_read}", end="\r", flush=True)

        print(f"    RPSale rows scanned: {rows_read}, matched: {len(self._sales)}", flush=True)

    def _load_foreclosures(self):
        """Fetch foreclosure PINs from KC SODA."""
        url = f"{KC_SODA_BASE}/{KC_FORECLOSURE_DATASET}.json"
        resp = self.session.get(url, params={"$limit": 5000}, timeout=30)
        resp.raise_for_status()
        records = resp.json()
        for rec in records:
            pin = (rec.get("parcels") or "").strip()
            if pin:
                self._foreclosures.add(pin)
        print(f"    Foreclosure PINs loaded: {len(self._foreclosures)}", flush=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_downloaded(self, url: str, filename: str) -> Path:
        """Download file to DOWNLOADS_DIR if missing or stale (>7 days)."""
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        path = DOWNLOADS_DIR / filename

        if path.exists():
            age = time.time() - path.stat().st_mtime
            if age < KC_DOWNLOAD_CACHE_DAYS * 86400:
                print(f"    Using cached {filename} ({age / 86400:.1f} days old)", flush=True)
                return path

        print(f"    Downloading {filename}...", flush=True)
        resp = self.session.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"    {filename}: {pct}% ({downloaded // 1024 // 1024}MB)",
                          end="\r", flush=True)

        print(f"    Downloaded {filename}: {downloaded // 1024 // 1024}MB  ", flush=True)
        return path

    @staticmethod
    def _find_csv_in_zip(zf: zipfile.ZipFile, prefix: str) -> Optional[str]:
        """Find a CSV file in a zip whose name contains the given prefix."""
        for name in zf.namelist():
            if prefix.lower() in name.lower() and name.lower().endswith(".csv"):
                return name
        return None

    def _enrich(self, pin: str, parcel: Dict) -> Dict[str, Any]:
        """Merge parcel with mailing, sales, foreclosure data."""
        return {
            "pin": pin,
            "address": parcel["address"],
            "zip": parcel["zip"],
            "land_val": parcel.get("land_val", 0),
            "impr_val": parcel.get("impr_val", 0),
            "mailing": self._mailing.get(pin),
            "sale": self._sales.get(pin),
            "in_foreclosure": pin in self._foreclosures,
        }

    def _has_signals(self, record: Dict[str, Any]) -> bool:
        """Quick check: will this record produce any distress signals?"""
        # Absentee?
        mailing = record.get("mailing")
        if mailing:
            state = mailing.get("state", "")
            city = mailing.get("city", "")
            if state and state != "WA":
                return True
            if state == "WA" and city and city.upper() != "SEATTLE":
                return True

        # Foreclosure?
        if record.get("in_foreclosure"):
            return True

        # Long-term or no sale?
        sale = record.get("sale")
        if not sale:
            return True  # no sale on record
        last_date = sale.get("last_date")
        if last_date:
            try:
                sale_dt = datetime.strptime(last_date[:10], "%m/%d/%Y")
            except ValueError:
                try:
                    sale_dt = datetime.strptime(last_date[:10], "%Y-%m-%d")
                except ValueError:
                    sale_dt = None
            if sale_dt:
                age_years = (datetime.now() - sale_dt).days / 365.25
                if age_years >= 10:
                    return True
                # Recently sold is a negative signal — still worth emitting
                sale_price = sale.get("last_price") or 0
                try:
                    sale_price = int(float(sale_price))
                except (ValueError, TypeError):
                    sale_price = 0
                if age_years < 1 and sale_price > 0:
                    return True

        # Low improvement ratio?
        land_val = record.get("land_val") or 0
        impr_val = record.get("impr_val") or 0
        if land_val > 0 and impr_val < land_val * 0.3:
            return True

        return False
