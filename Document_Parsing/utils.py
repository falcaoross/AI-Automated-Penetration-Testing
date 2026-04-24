"""
utils.py — Shared utility functions for the Autopilot-QA Ingestion Engine.

Three pure functions, no side effects, no imports beyond stdlib:
  - roman_to_int(s)             : Roman numeral string → int (I–X) or None
  - normalize_section_path(raw) : Section numeral token → dot-separated path string
  - clean_text(text)            : Raw PDF text → cleaned ASCII-safe string

These functions are imported by toc_parser, section_tracker, body_extractor,
and block_classifier. Do not add any other functions to this file.
"""

import re
import unicodedata


def roman_to_int(s: str):
    """Convert a Roman numeral string (I–X) to an integer, or return None."""
    if s is None:
        return None
    s = s.strip().upper()
    if not s:
        return None

    values = {'I': 1, 'V': 5, 'X': 10}
    if not all(ch in values for ch in s):
        return None

    result = 0
    prev = 0
    for ch in reversed(s):
        curr = values[ch]
        if curr < prev:
            result -= curr
        else:
            result += curr
        prev = curr

    if result < 1 or result > 10:
        return None
    return result


def normalize_section_path(raw: str) -> str:
    """Convert a raw section numeral token to a normalized dot-separated path string."""
    if raw is None:
        return ""
    raw_stripped = raw.strip()

    # Rule 1 — Already numeric dot notation
    if re.match(r"^\d+(\.\d+)*$", raw_stripped):
        return raw_stripped

    # Rule 2 — Roman numeral (checked before alpha to handle V, X correctly)
    n = roman_to_int(raw_stripped)
    if n is not None:
        return "A." + str(n)

    # Rule 3 — Single alphabetic letter
    if re.match(r"^[A-Za-z]$", raw_stripped):
        n = ord(raw_stripped.upper()) - ord('A') + 1
        return "A." + str(n)

    # Rule 4 — Fallback: return unchanged
    return raw


def clean_text(text: str) -> str:
    """Clean raw PDF text: normalize Unicode dashes/spaces, collapse excess spaces."""
    if not text:
        return ""

    # Replace Unicode hyphen/dash variants with ASCII hyphen-minus
    dash_chars = '\u00ad\u2013\u2014\u2012\u2011'
    for ch in dash_chars:
        text = text.replace(ch, '-')

    # Replace Unicode whitespace (Zs category), zero-width space, tab with ASCII space
    cleaned = []
    for ch in text:
        if ch == '\u200b' or ch == '\t':
            cleaned.append(' ')
        elif ch == '\n':
            cleaned.append('\n')
        elif unicodedata.category(ch) == 'Zs':
            cleaned.append(' ')
        else:
            cleaned.append(ch)
    text = ''.join(cleaned)

    # Collapse runs of 3+ consecutive spaces to exactly 2
    text = re.sub(r' {3,}', '  ', text)

    # Strip leading and trailing whitespace (but preserve internal newlines)
    text = text.strip(' ')

    return text
