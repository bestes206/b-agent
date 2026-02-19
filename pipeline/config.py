"""Pipeline configuration — zip codes, endpoints, DB path, constants."""

import os
from pathlib import Path

# West Seattle zip codes
WEST_SEATTLE_ZIPS = ["98106", "98116", "98126", "98136", "98146"]

# SODA API base
SODA_BASE = "https://data.seattle.gov/resource"

# Dataset IDs
DATASETS = {
    "code_violations": "ez4a-iug7",
    "permits": "76t5-zqzr",
    "fire_911": "kzjm-xkqj",
    "urm": "54qs-2h7f",
}

# Fire 911 geographic filter — center of West Seattle, 5km radius
FIRE_CENTER_LAT = 47.5615
FIRE_CENTER_LNG = -122.3706
FIRE_RADIUS_METERS = 5000

# SODA pagination
SODA_PAGE_SIZE = 1000
SODA_RATE_LIMIT_DELAY = 0.5  # seconds between paginated requests

# Optional app token for higher rate limits
SODA_APP_TOKEN = os.environ.get("SODA_APP_TOKEN")

# Database
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "distressed.db"

# Downloads directory (for KC assessor CSVs, etc.)
DOWNLOADS_DIR = PROJECT_ROOT / "data" / "downloads"

# Scoring config
SCORING_CONFIG_PATH = Path(__file__).resolve().parent / "scoring_config.yaml"

# Lat/lng proximity threshold for fuzzy matching (~10 meters)
PROXIMITY_DEGREES = 0.0001
