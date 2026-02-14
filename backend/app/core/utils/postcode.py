import re

# UK postcode regex — covers standard formats:
#   A9 9AA, A99 9AA, A9A 9AA, AA9 9AA, AA99 9AA, AA9A 9AA
_POSTCODE_RE = re.compile(
    r"^([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})$",
    re.IGNORECASE,
)


def normalise_postcode(raw: str) -> str | None:
    """Normalise a UK postcode to 'A9 9AA' format.

    Returns the normalised string, or None if the input is not a valid UK postcode.
    """
    if not raw:
        return None

    cleaned = raw.strip().upper()
    match = _POSTCODE_RE.match(cleaned)
    if not match:
        return None

    outward, inward = match.group(1), match.group(2)
    return f"{outward} {inward}"


def validate_postcode(raw: str) -> bool:
    """Check if a string is a valid UK postcode."""
    return normalise_postcode(raw) is not None


def postcode_no_space(normalised: str) -> str:
    """Remove the space from a normalised postcode. E.g. 'SW1A 1AA' -> 'SW1A1AA'."""
    return normalised.replace(" ", "")
