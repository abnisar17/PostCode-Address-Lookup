import re

# Common UK street abbreviations → expansions
_STREET_ABBREVIATIONS: dict[str, str] = {
    "RD": "ROAD",
    "ST": "STREET",
    "AVE": "AVENUE",
    "AV": "AVENUE",
    "DR": "DRIVE",
    "LN": "LANE",
    "CT": "COURT",
    "PL": "PLACE",
    "CL": "CLOSE",
    "CRES": "CRESCENT",
    "TERR": "TERRACE",
    "GRN": "GREEN",
    "GDN": "GARDEN",
    "GDNS": "GARDENS",
    "SQ": "SQUARE",
    "PK": "PARK",
    "BLVD": "BOULEVARD",
}

_MULTI_SPACE_RE = re.compile(r"\s+")


def normalise_street(raw: str | None) -> str | None:
    """Normalise a street name: title case, expand abbreviations."""
    if not raw:
        return None

    cleaned = _MULTI_SPACE_RE.sub(" ", raw.strip()).upper()
    if not cleaned:
        return None

    words = cleaned.split()
    expanded = [_STREET_ABBREVIATIONS.get(w, w) for w in words]
    return " ".join(expanded).title()


def normalise_city(raw: str | None) -> str | None:
    """Normalise a city name: strip whitespace, title case."""
    if not raw:
        return None

    cleaned = _MULTI_SPACE_RE.sub(" ", raw.strip())
    if not cleaned:
        return None

    return cleaned.title()
