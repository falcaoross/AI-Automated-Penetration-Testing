from __future__ import annotations

"""labeled_extractor.py — Deterministic extractor for "labeled" format groups.

Single responsibility: extract structured fields from a RequirementGroup
whose format has been set to "labeled" and return an ExtractedRequirement.

No LLM calls. No HTTP requests. Pure regex and string operations only.
"""

import re
from typing import Optional

from block_grouper import RequirementGroup
from schemas import ExtractedRequirement
from utils import clean_text, build_source_reference, map_confidence


# ---------------------------------------------------------------------------
# Compiled patterns — prefix stripping
# ---------------------------------------------------------------------------

_PREFIX_ID    = re.compile(r'^ID\s*:?\s*',    re.IGNORECASE)
_PREFIX_TITLE = re.compile(r'^TITLE\s*:?\s*', re.IGNORECASE)
_PREFIX_DESC  = re.compile(r'^DESC\s*:?\s*',  re.IGNORECASE)
_PREFIX_RAT   = re.compile(r'^RAT\s*:?\s*',   re.IGNORECASE)
_PREFIX_DEP   = re.compile(r'^DEP\s*:?\s*',   re.IGNORECASE)
_SPLIT_DEP    = re.compile(r'[,\s]+')
_MODAL        = re.compile(r'\b(should|shall|must|can|will)\b', re.IGNORECASE)
_SENTENCE_SEP = re.compile(r'(?<=[.!?])\s+')
_NOISE_DEP    = {"none", "n/a", "na"}


# Actor tokens searched in priority order: (search_token, return_value)
_ACTOR_TOKENS = [
    ("restaurant owner", "Restaurant Owner"),
    ("administrator",    "Administrator"),
    ("admin",            "Administrator"),
    ("user",             "User"),
    ("system",           "System"),
]

_DEFAULT_ACTOR = "System"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _strip_prefix(pattern: re.Pattern[str], text: str) -> str:
    """Remove a labeled-field prefix from text and apply clean_text()."""
    return clean_text(pattern.sub("", text, count=1))


def _extract_actor(text: str) -> str:
    """Return the first matching actor token found in text (case-insensitive)."""
    if not text:
        return _DEFAULT_ACTOR
    lowered = text.lower()
    for token, label in _ACTOR_TOKENS:
        if token in lowered:
            return label
    return _DEFAULT_ACTOR


def _extract_actions(description: Optional[str]) -> list[str]:
    """Return sentences containing a modal verb, or the full description."""
    if not description:
        return []
    sentences = [s.strip() for s in _SENTENCE_SEP.split(description) if s.strip()]
    actions = [clean_text(s) for s in sentences if _MODAL.search(s)]
    if not actions:
        cleaned = clean_text(description)
        return [cleaned] if cleaned else []
    return actions


def _extract_table(block: dict) -> list[dict]:
    """Extract planguage_table rows from a table_block dict."""
    table = block.get("table")
    if table and "rows" in table:
        result: list[dict] = []
        for row in table["rows"]:
            if len(row) >= 2:
                result.append({"key": clean_text(row[0]), "value": clean_text(row[1])})
            elif len(row) == 1:
                result.append({"key": clean_text(row[0]), "value": None})
        return result

    # Fallback: parse block["text"] as colon-separated lines
    result = []
    raw_text = block.get("text", "")
    for line in raw_text.splitlines():
        if ":" in line:
            key, _, value = line.partition(":")
            k = clean_text(key)
            v = clean_text(value)
            if k:
                result.append({"key": k, "value": v or None})
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_labeled(group: RequirementGroup) -> ExtractedRequirement:
    """Extract a structured ExtractedRequirement from a labeled-format group.

    Iterates group.blocks exactly once, dispatching on structural_role to
    populate each field. Actor and action extraction operate on the assembled
    description string after the block loop.

    Args:
        group: A RequirementGroup with format == "labeled".

    Returns:
        A fully populated ExtractedRequirement instance.
    """
    req_id:               Optional[str]        = None
    title:                Optional[str]         = None
    description:          Optional[str]         = None
    acceptance_criteria:  Optional[str]         = None
    dependencies:         list[str]             = []
    planguage_table:      Optional[list[dict]]  = None

    first_block: Optional[dict] = group.blocks[0] if group.blocks else None
    _in_desc_continuation = False

    for block in group.blocks:
        role = block["structural_role"]
        text = block.get("text", "")

        if role == "req_id_label":
            req_id = _strip_prefix(_PREFIX_ID, text)
            _in_desc_continuation = False

        elif role == "req_title_label":
            title = _strip_prefix(_PREFIX_TITLE, text)
            _in_desc_continuation = False

        elif role == "req_desc_label":
            stripped = _strip_prefix(_PREFIX_DESC, text)
            description = stripped if stripped else None
            _in_desc_continuation = True

        elif role == "prose_content" and _in_desc_continuation:
            chunk = clean_text(text)
            if chunk:
                if description:
                    description = description + " " + chunk
                else:
                    description = chunk

        elif role == "req_rationale_label":
            acceptance_criteria = _strip_prefix(_PREFIX_RAT, text)
            _in_desc_continuation = False

        elif role == "req_dependency_label":
            _in_desc_continuation = False
            stripped = _strip_prefix(_PREFIX_DEP, text)
            tokens = _SPLIT_DEP.split(stripped)
            dependencies = [
                t for t in tokens
                if t and t.lower() not in _NOISE_DEP
            ]

        elif role == "table_block":
            _in_desc_continuation = False
            candidate = _extract_table(block)
            valid_rows = [r for r in candidate if r.get("value") is not None]
            if len(valid_rows) >= 2:
                planguage_table = candidate

        # All other roles (section_heading, prose_content, req_fit_label,
        # planguage_keyword, gherkin_keyword) are skipped.

    # Strip leading/trailing whitespace from description accumulated via continuation
    if description is not None:
        description = description.strip()

    # ── Actor extraction ──────────────────────────────────────────────────
    actor = _extract_actor(description or "")
    if actor == _DEFAULT_ACTOR and not description:
        actor = _extract_actor(acceptance_criteria or "")

    # ── Action extraction ─────────────────────────────────────────────────
    actions = _extract_actions(description)

    # ── Fallback req_id ───────────────────────────────────────────────────
    final_req_id = req_id or group.candidate_req_id or ""

    # ── Confidence ────────────────────────────────────────────────────────
    has_nulls = not title or not description or not acceptance_criteria
    confidence = map_confidence("deterministic", has_nulls)

    # ── Source reference ──────────────────────────────────────────────────
    source_ref = build_source_reference(first_block) if first_block else {
        "doc_id": None,
        "section_path": group.section_path,
        "source_locator": None,
        "module": None,
        "version": None,
    }

    return ExtractedRequirement(
        req_id                = final_req_id,
        section_path          = group.section_path,
        section_semantic_type = group.section_semantic_type,
        input_format          = "labeled",
        extraction_method     = "deterministic",
        title                 = title or "",
        actor                 = actor,
        actions               = actions,
        description           = description or None,
        constraints           = None,
        dependencies          = dependencies,
        acceptance_criteria   = acceptance_criteria or None,
        outputs               = None,
        planguage_table       = planguage_table,
        scenarios             = None,
        confidence            = confidence,
        source_reference      = source_ref,
    )
