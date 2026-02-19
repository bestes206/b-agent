"""CLI entry point — wire all fetchers together and run the pipeline.

Usage:
    python -m pipeline.run                          # all sources, full pipeline
    python -m pipeline.run --source code_violations # one source at a time
    python -m pipeline.run --rescore-only           # re-apply scoring without re-fetching
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, Type

from pipeline.db import (
    complete_pipeline_run,
    find_nearby_property,
    get_connection,
    init_db,
    log_normalization_issue,
    start_pipeline_run,
    upsert_property,
    upsert_signal,
)
from pipeline.fetchers.base import SODAFetcher
from pipeline.fetchers.code_violations import CodeViolationsFetcher
from pipeline.fetchers.fire_calls import FireCallsFetcher
from pipeline.fetchers.permits import PermitsFetcher
from pipeline.fetchers.recent_sales import RecentSalesFetcher
from pipeline.fetchers.urm_buildings import URMBuildingsFetcher
from pipeline.normalize import normalize_address
from pipeline.scoring import load_config, rescore_all

FETCHERS: Dict[str, Type] = {
    "code_violations": CodeViolationsFetcher,
    "permits": PermitsFetcher,
    "fire_911": FireCallsFetcher,
    "urm": URMBuildingsFetcher,
    "king_county_sales": RecentSalesFetcher,
}


def run_fetcher(conn, fetcher) -> tuple:
    """Run a single fetcher, upserting properties and signals.

    Returns (properties_touched, signals_inserted).
    """
    props_touched = 0
    signals_inserted = 0
    skipped = 0

    print(f"  Fetching from {fetcher.source_name}...", flush=True)

    for page_num, page in enumerate(fetcher.paginate(), 1):
        print(f"    Page {page_num}: {len(page)} records", flush=True)

        for record in page:
            raw_addr = fetcher.extract_address(record)
            if not raw_addr:
                skipped += 1
                continue

            norm_addr = normalize_address(raw_addr)
            if not norm_addr:
                skipped += 1
                continue

            lat, lng = fetcher.extract_coords(record)
            zip_code = fetcher.extract_zip(record)

            # Upsert property
            prop_id = upsert_property(
                conn, raw_addr, norm_addr,
                zip_code=zip_code, lat=lat, lng=lng,
            )
            props_touched += 1

            # Extract and insert signals
            for sig in fetcher.extract_signals(record):
                inserted = upsert_signal(
                    conn, prop_id, fetcher.source_name,
                    sig["source_record_id"], sig["signal_type"],
                    signal_weight=0,  # actual weight computed during scoring
                    detail=sig.get("detail"),
                    event_date=sig.get("event_date"),
                )
                if inserted:
                    signals_inserted += 1

        conn.commit()

    # Check for normalization issues — records near existing properties but with different addresses
    _check_normalization_issues(conn, fetcher)

    if skipped:
        print(f"    Skipped {skipped} records (no/empty address)", flush=True)

    return (props_touched, signals_inserted)


def _check_normalization_issues(conn, fetcher):
    """Post-pass: find signals whose properties might be duplicates based on proximity."""
    # This is a lightweight check — for each property that only appears in one source,
    # see if there's a nearby property from a different source
    rows = conn.execute(
        """SELECT DISTINCT p.id, p.address_raw, p.address_norm, p.latitude, p.longitude
           FROM properties p
           JOIN signals s ON s.property_id = p.id
           WHERE s.source = ? AND p.latitude IS NOT NULL AND p.longitude IS NOT NULL""",
        (fetcher.source_name,),
    ).fetchall()

    issues_found = 0
    for row in rows:
        nearby = find_nearby_property(conn, row["latitude"], row["longitude"])
        if nearby and nearby["id"] != row["id"]:
            log_normalization_issue(
                conn,
                address_raw=row["address_raw"],
                address_norm=row["address_norm"],
                source=fetcher.source_name,
                lat=row["latitude"],
                lng=row["longitude"],
                nearest_property_id=nearby["id"],
                distance_meters=nearby["dist"] * 111000,  # rough degree-to-meter conversion
            )
            issues_found += 1

    if issues_found:
        print(f"    Found {issues_found} potential normalization issues", flush=True)
    conn.commit()


def print_summary(conn):
    """Print a summary of the database state."""
    total = conn.execute("SELECT COUNT(*) as c FROM properties").fetchone()["c"]
    tiers = conn.execute(
        "SELECT tier, COUNT(*) as c FROM properties GROUP BY tier ORDER BY tier"
    ).fetchall()
    signals = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
    issues = conn.execute("SELECT COUNT(*) as c FROM normalization_issues").fetchone()["c"]

    print(f"\n{'='*50}")
    print(f"Pipeline Summary")
    print(f"{'='*50}")
    print(f"Total properties: {total}")
    print(f"Total signals:    {signals}")
    for t in tiers:
        print(f"  Tier {t['tier']}: {t['c']}")
    if issues:
        print(f"Normalization issues: {issues}")

    # Top 10
    top = conn.execute(
        "SELECT address_raw, total_score, tier FROM properties ORDER BY total_score DESC LIMIT 10"
    ).fetchall()
    if top:
        print(f"\nTop 10 properties:")
        for i, row in enumerate(top, 1):
            print(f"  {i}. [{row['tier']}] {row['total_score']:.1f} — {row['address_raw']}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Distressed Property Pipeline")
    parser.add_argument("--source", choices=list(FETCHERS.keys()),
                        help="Run only a specific source")
    parser.add_argument("--rescore-only", action="store_true",
                        help="Re-apply scoring from YAML without re-fetching")
    args = parser.parse_args()

    conn = get_connection()
    init_db(conn)

    if args.rescore_only:
        print("Rescoring all properties from scoring_config.yaml...")
        config = load_config()
        rescore_all(conn, config)
        print_summary(conn)
        conn.close()
        return

    # Determine which sources to run
    if args.source:
        sources = [args.source]
    else:
        sources = list(FETCHERS.keys())

    run_id = start_pipeline_run(conn, sources)
    total_props = 0
    total_sigs = 0

    print(f"Pipeline run #{run_id} — sources: {', '.join(sources)}")

    for source_name in sources:
        fetcher_cls = FETCHERS[source_name]
        fetcher = fetcher_cls()
        try:
            props, sigs = run_fetcher(conn, fetcher)
            total_props += props
            total_sigs += sigs
            print(f"  {source_name}: {props} properties touched, {sigs} new signals")
        except Exception as e:
            print(f"  ERROR in {source_name}: {e}", file=sys.stderr)

    # Rescore everything
    print("Rescoring all properties...")
    config = load_config()
    rescore_all(conn, config)

    complete_pipeline_run(conn, run_id, total_props, total_sigs)
    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
