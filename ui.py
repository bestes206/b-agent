"""Local Flask UI for the distressed property pipeline."""

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

from pipeline.scoring import load_config as load_scoring_config

load_dotenv()

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "distressed.db")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def index():
    return render_template("index.html", google_maps_api_key=GOOGLE_MAPS_API_KEY)


@app.route("/api/properties")
def api_properties():
    conn = get_db()

    # Filters
    tier = request.args.get("tier")
    zip_code = request.args.get("zip")
    source = request.args.get("source")
    min_score = request.args.get("min_score", type=float)
    search = request.args.get("search", "").strip()

    # Sorting
    sort_col = request.args.get("sort", "total_score")
    sort_dir = request.args.get("dir", "desc")
    allowed_sorts = {"total_score", "tier", "address_raw", "zip_code", "signal_count", "source_count"}
    if sort_col not in allowed_sorts:
        sort_col = "total_score"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    # Pagination
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    per_page = min(per_page, 200)
    offset = (page - 1) * per_page

    where_clauses = []
    params = []

    if tier:
        where_clauses.append("p.tier = ?")
        params.append(tier)
    if zip_code:
        where_clauses.append("p.zip_code = ?")
        params.append(zip_code)
    if min_score is not None:
        where_clauses.append("p.total_score >= ?")
        params.append(min_score)
    if search:
        where_clauses.append("p.address_raw LIKE ?")
        params.append(f"%{search}%")
    if source:
        where_clauses.append("p.id IN (SELECT DISTINCT property_id FROM signals WHERE source = ?)")
        params.append(source)

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # Count total matching
    count_row = conn.execute(
        f"SELECT COUNT(DISTINCT p.id) as total FROM properties p {where_sql}", params
    ).fetchone()
    total = count_row["total"]

    # Fetch page
    rows = conn.execute(
        f"""SELECT p.id, p.address_raw, p.address_norm, p.zip_code, p.latitude, p.longitude,
                   p.total_score, p.tier,
                   COUNT(s.id) as signal_count,
                   COUNT(DISTINCT s.source) as source_count,
                   GROUP_CONCAT(DISTINCT s.source) as sources
            FROM properties p
            LEFT JOIN signals s ON s.property_id = p.id
            {where_sql}
            GROUP BY p.id
            ORDER BY {sort_col} {sort_dir}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    properties = []
    for r in rows:
        properties.append({
            "id": r["id"],
            "address_raw": r["address_raw"],
            "address_norm": r["address_norm"],
            "zip_code": r["zip_code"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "total_score": r["total_score"],
            "tier": r["tier"],
            "signal_count": r["signal_count"],
            "source_count": r["source_count"],
            "sources": r["sources"].split(",") if r["sources"] else [],
        })

    conn.close()
    return jsonify({
        "properties": properties,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    })


@app.route("/api/properties/<int:property_id>/signals")
def api_signals(property_id):
    conn = get_db()
    rows = conn.execute(
        """SELECT source, source_record_id, signal_type, signal_weight, detail, event_date
           FROM signals WHERE property_id = ? ORDER BY event_date DESC""",
        (property_id,),
    ).fetchall()

    signals = []
    for r in rows:
        detail = None
        if r["detail"]:
            try:
                detail = json.loads(r["detail"])
            except (json.JSONDecodeError, TypeError):
                detail = r["detail"]
        signals.append({
            "source": r["source"],
            "source_record_id": r["source_record_id"],
            "signal_type": r["signal_type"],
            "detail": detail,
            "event_date": r["event_date"],
        })

    conn.close()
    return jsonify(signals)


@app.route("/api/properties/<int:property_id>/breakdown")
def api_breakdown(property_id):
    """Per-source score subtotals using the same logic as pipeline/scoring.py."""
    config = load_scoring_config()
    weights = config.get("signal_weights", {})
    recency = config.get("recency", {})
    status_multipliers = config.get("status_multipliers", {})

    max_age_days = recency.get("max_age_days", 1825)
    decay_boost = recency.get("decay_boost", 0.5)
    now = datetime.now(timezone.utc)
    max_age_cutoff = now - timedelta(days=max_age_days)

    conn = get_db()
    rows = conn.execute(
        "SELECT signal_type, event_date, source, detail FROM signals WHERE property_id = ?",
        (property_id,),
    ).fetchall()
    conn.close()

    by_source = {}
    for row in rows:
        signal_type = row["signal_type"]
        source = row["source"]
        base_weight = weights.get(signal_type, 1)

        # Decay curve
        decay_mult = 1.0
        event_date_str = row["event_date"]
        if event_date_str:
            try:
                event_date = datetime.fromisoformat(event_date_str.replace("Z", "+00:00"))
                if event_date.tzinfo is None:
                    event_date = event_date.replace(tzinfo=timezone.utc)
                if event_date < max_age_cutoff:
                    continue
                age_days = (now - event_date).days
                decay_mult = 1.0 + decay_boost * max(0, 1 - age_days / max_age_days)
            except (ValueError, TypeError):
                pass

        # Status multiplier
        status_mult = 1.0
        detail_str = row["detail"]
        if detail_str and source in status_multipliers:
            try:
                detail = json.loads(detail_str)
                status = (detail.get("status") or "").lower().strip()
                if status in status_multipliers[source]:
                    status_mult = status_multipliers[source][status]
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        by_source[source] = by_source.get(source, 0) + base_weight * decay_mult * status_mult

    return jsonify(by_source)


@app.route("/api/stats")
def api_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as c FROM properties").fetchone()["c"]
    signals = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
    tiers = conn.execute("SELECT tier, COUNT(*) as c FROM properties GROUP BY tier ORDER BY tier").fetchall()
    zips = conn.execute(
        "SELECT zip_code, COUNT(*) as c FROM properties WHERE zip_code IS NOT NULL GROUP BY zip_code ORDER BY zip_code"
    ).fetchall()
    sources = conn.execute(
        "SELECT source, COUNT(*) as c FROM signals GROUP BY source ORDER BY source"
    ).fetchall()
    conn.close()

    return jsonify({
        "total_properties": total,
        "total_signals": signals,
        "tiers": {r["tier"]: r["c"] for r in tiers},
        "zips": {r["zip_code"]: r["c"] for r in zips},
        "sources": {r["source"]: r["c"] for r in sources},
    })


if __name__ == "__main__":
    app.run(port=5001)
