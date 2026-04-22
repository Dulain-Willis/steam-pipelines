"""Text-processing utilities for pipeline transformations."""

import re


# Business suffixes that should not be treated as comma-separated entities.
# Input is expected to be lowercased (by silver_stg).
_COMPOUND_SUFFIXES = [
    r"co\.\s*,\s*ltd\.?",   # co., ltd.
]

_SINGLE_SUFFIXES = [
    r"llc\.?",
    r"inc\.?",
    r"ltd\.?",
    r"l\.l\.c\.?",
    r"co\.",
    r"corp\.?",
]

_SENTINEL = "\x00"


def smart_split_comma(value: str) -> list[str]:
    """Split a comma-delimited string while preserving business-name suffixes.

    Handles cases like "dev a, dev b, llc, dev c, co., ltd." by keeping
    business suffixes attached to the preceding name. Input is expected to
    be already lowercased.

    Args:
        value: A comma-separated string of entity names (lowercased).

    Returns:
        A list of individual entity names, stripped of whitespace.
    """
    if not value or not value.strip():
        return []

    text = value

    # Pass 1: protect compound suffixes (e.g. "co., ltd.")
    for pattern in _COMPOUND_SUFFIXES:
        text = re.sub(
            r",\s*(?=" + pattern + r"(?:\s*,|\s*$))",
            _SENTINEL,
            text,
        )

    # Pass 2: protect commas before single suffixes
    for pattern in _SINGLE_SUFFIXES:
        text = re.sub(
            r",\s*(?=" + pattern + r"(?:\s*,|\s*$))",
            _SENTINEL,
            text,
        )

    # Split on real commas
    parts = text.split(",")

    # Restore sentinels (put back ", " since the regex consumed comma + whitespace)
    result = []
    for part in parts:
        cleaned = part.replace(_SENTINEL, ", ").strip()
        if cleaned:
            result.append(cleaned)

    return result
