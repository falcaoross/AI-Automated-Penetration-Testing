from __future__ import annotations

"""Shared utility helpers for Layer 2 requirement understanding."""

import json
from pathlib import Path
import unicodedata
import re


def load_blocks(path: str | Path) -> list[dict]:
    """Load and return the blocks list from a Layer 1 blocks JSON file.

    The input file must be a JSON object containing a top-level "blocks"
    key whose value is a list of block dictionaries. The function raises a
    clear exception for missing files, invalid JSON, or missing required keys.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Blocks file not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in blocks file {path}: {exc}") from exc

    if "blocks" not in data:
        raise KeyError(f'Missing "blocks" key in blocks file: {path}')

    return data["blocks"]


def load_skeleton(path: str | Path) -> dict:
    """Load and return the full JSON object from a Layer 1 skeleton file.

    The input file must be a valid JSON object representing the document
    skeleton. The function raises a clear exception for missing files or
    invalid JSON and does not return fallback empty structures.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Skeleton file not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in skeleton file {path}: {exc}") from exc

    return data["document_skeleton"]


def clean_text(text: str) -> str:
    """Normalize Unicode and collapse whitespace in arbitrary text input.

    The function is safe to call on any block text value. Non-string inputs
    return an empty string. String inputs are normalized to NFC, zero-width
    and non-breaking spaces are treated as whitespace, internal whitespace
    runs are collapsed to a single space, and leading or trailing whitespace
    is removed.
    """
    if not isinstance(text, str):
        return ""

    text = unicodedata.normalize("NFC", text)
    text = text.replace("\u00a0", " ").replace("\u200b", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_source_reference(block: dict) -> dict:
    """Build a source_reference dictionary from a single Layer 1 block.

    The returned mapping always contains the five required provenance keys.
    Each value is copied directly from the block dictionary when present,
    otherwise None is used for missing keys.
    """
    return {
        "doc_id": block.get("doc_id"),
        "section_path": block.get("section_path"),
        "source_locator": block.get("source_locator"),
        "module": block.get("module"),
        "version": block.get("version"),
    }


def map_confidence(extraction_method: str, has_nulls: bool) -> str:
    """Map extraction provenance to a normalized confidence label.

    Deterministic extraction without nulls is high confidence, deterministic
    extraction with nulls is medium confidence, all llm_structured results are
    medium confidence, and regex_fallback or unrecognized methods are low
    confidence.
    """
    if extraction_method == "deterministic" and has_nulls is False:
        return "high"

    if extraction_method == "deterministic" and has_nulls is True:
        return "medium"

    if extraction_method in ("llm", "llm_structured") and has_nulls is False:
        return "high"

    if extraction_method in ("llm", "llm_structured") and has_nulls is True:
        return "medium"

    if extraction_method == "regex_fallback":
        return "low"

    return "low"