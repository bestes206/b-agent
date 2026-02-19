"""Address normalization for cross-source matching.

Multi-pass rule-based approach to produce a canonical address string
that can be used as a join key across Seattle open data sources.
"""

from __future__ import annotations

import re

# --- Directional mappings ---
_DIRECTIONALS = {
    "SOUTHWEST": "SW", "SOUTH WEST": "SW", "S WEST": "SW", "S.W.": "SW", "S.W": "SW",
    "NORTHWEST": "NW", "NORTH WEST": "NW", "N WEST": "NW", "N.W.": "NW", "N.W": "NW",
    "SOUTHEAST": "SE", "SOUTH EAST": "SE", "S EAST": "SE", "S.E.": "SE", "S.E": "SE",
    "NORTHEAST": "NE", "NORTH EAST": "NE", "N EAST": "NE", "N.E.": "NE", "N.E": "NE",
    "SOUTH": "S", "NORTH": "N", "EAST": "E", "WEST": "W",
}

# Build regex: match longest first to avoid partial matches
_DIR_PATTERN = re.compile(
    r'\b(' + '|'.join(
        re.escape(k) for k in sorted(_DIRECTIONALS.keys(), key=len, reverse=True)
    ) + r')(?:\b|(?=\s|$))',
    re.IGNORECASE,
)

# Single-letter with period: "S." "N." "E." "W." (but not mid-word)
_SINGLE_DIR_PATTERN = re.compile(r'\b([SNEW])\.\s*(?=[A-Z0-9]|\s|$)', re.IGNORECASE)

# --- Street suffix mappings ---
_SUFFIXES = {
    "STREET": "ST", "STR": "ST", "ST.": "ST",
    "AVENUE": "AVE", "AVE.": "AVE", "AV": "AVE",
    "DRIVE": "DR", "DR.": "DR",
    "BOULEVARD": "BLVD", "BLVD.": "BLVD",
    "PLACE": "PL", "PL.": "PL",
    "COURT": "CT", "CT.": "CT",
    "LANE": "LN", "LN.": "LN",
    "ROAD": "RD", "RD.": "RD",
    "CIRCLE": "CIR", "CIR.": "CIR",
    "TERRACE": "TER", "TER.": "TER",
    "PARKWAY": "PKWY", "PKWY.": "PKWY",
    "WAY": "WAY",
}

_SUFFIX_PATTERN = re.compile(
    r'\b(' + '|'.join(
        re.escape(k) for k in sorted(_SUFFIXES.keys(), key=len, reverse=True)
    ) + r')\b\.?',
    re.IGNORECASE,
)

# Unit/apt/suite pattern
_UNIT_PATTERN = re.compile(
    r'\b(?:UNIT|APT|SUITE|STE|#|BLDG|BUILDING|FLOOR|FL|RM|ROOM)\s*[#.]?\s*\S*',
    re.IGNORECASE,
)

# City/state/zip suffix
_CITY_STATE_ZIP = re.compile(
    r',?\s*(?:SEATTLE)?\s*,?\s*(?:WA|WASHINGTON)?\s*,?\s*\d{5}(?:-\d{4})?\s*$',
    re.IGNORECASE,
)

# Ordinal street number normalization: "FIRST" -> "1ST", etc. and "1 ST" -> "1ST"
_ORDINAL_WORDS = {
    "FIRST": "1ST", "SECOND": "2ND", "THIRD": "3RD", "FOURTH": "4TH",
    "FIFTH": "5TH", "SIXTH": "6TH", "SEVENTH": "7TH", "EIGHTH": "8TH",
    "NINTH": "9TH", "TENTH": "10TH",
}

# Match "1 ST" or "2 ND" etc. where a space crept in before the ordinal suffix
_SPLIT_ORDINAL = re.compile(r'\b(\d+)\s+(ST|ND|RD|TH)\b')


def normalize_address(raw: str) -> str | None:
    """Normalize a raw address string to a canonical form for matching.

    Returns None if the input is empty or produces an empty result.
    """
    if not raw:
        return None

    addr = raw.strip()

    # Flatten newlines
    addr = addr.replace("\n", " ").replace("\r", " ")

    # Pass 1 — Clean up
    addr = addr.upper()
    addr = _CITY_STATE_ZIP.sub("", addr)
    addr = _UNIT_PATTERN.sub("", addr)
    # Remove bare hash+number (e.g. "#201") that unit pattern may not catch
    addr = re.sub(r'#\s*\w+', ' ', addr)
    addr = re.sub(r',', ' ', addr)  # remove commas
    addr = re.sub(r'\s+', ' ', addr).strip()

    # Pass 2 — Normalize directionals (BEFORE stripping periods, so S.W. works)
    # Compound directionals first (S.W., N.E., etc.), then single-letter (S., N.)
    addr = _DIR_PATTERN.sub(lambda m: _DIRECTIONALS.get(m.group(1).upper(), m.group(1).upper()), addr)
    addr = _SINGLE_DIR_PATTERN.sub(lambda m: m.group(1).upper() + " ", addr)

    # Pass 3 — Normalize street suffixes (handles trailing periods like ST.)
    addr = _SUFFIX_PATTERN.sub(lambda m: _SUFFIXES.get(m.group(1).upper(), m.group(1).upper()), addr)

    # Now strip remaining periods
    addr = re.sub(r'\.', ' ', addr)

    # Pass 4 — Edge cases
    # Ordinal words
    for word, replacement in _ORDINAL_WORDS.items():
        addr = re.sub(rf'\b{word}\b', replacement, addr)

    # Fix split ordinals: "1 ST" -> "1ST" (but only when it's a street number, not "1 ST" as suffix)
    # We do this carefully: only when followed by more address content
    addr = _SPLIT_ORDINAL.sub(r'\1\2', addr)

    # Final cleanup
    addr = re.sub(r'\s+', ' ', addr).strip()

    return addr if addr else None


# --- Inline test cases ---
if __name__ == "__main__":
    tests = [
        ("5812 SW Spokane St", "5812 SW SPOKANE ST"),
        ("5812 S.W. Spokane Street", "5812 SW SPOKANE ST"),
        ("5812 South West Spokane St.", "5812 SW SPOKANE ST"),
        ("5812 sw spokane street, seattle, wa 98106", "5812 SW SPOKANE ST"),
        ("4th Ave SW", "4TH AVE SW"),
        ("123 NE 45th Street #201", "123 NE 45TH ST"),
        ("456 S. Main Ave, Apt 3B, Seattle, WA 98136", "456 S MAIN AVE"),
        ("789 First Avenue S", "789 1ST AVE S"),
        ("100 North West 3rd Place", "100 NW 3RD PL"),
        ("222 N.W. Market Street\nSeattle WA 98107", "222 NW MARKET ST"),
        ("", None),
        (None, None),
    ]

    passed = 0
    for raw, expected in tests:
        result = normalize_address(raw)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            print(f"  {status}: normalize({raw!r}) = {result!r}, expected {expected!r}")
        else:
            passed += 1
    print(f"{passed}/{len(tests)} tests passed")
