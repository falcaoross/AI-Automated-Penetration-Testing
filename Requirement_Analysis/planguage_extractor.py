from __future__ import annotations

"""planguage_extractor.py — Deterministic extractor for "planguage" format groups.

Single responsibility: parse the PLanguage keyword lines from a design-constraints
or quality-attributes RequirementGroup and return an ExtractedRequirement.

No LLM calls. No HTTP requests. Pure regex and string operations only.
Actor is always "System" for PLanguage requirements.

Fix applied:
  Bug 5 (audit medium issue) — _select_content_block() previously returned only
  the first matching block, so PLanguage groups where Tag/Gist/Scale/etc. each
  live in separate blocks (e.g. 3.5.1 SystemReliability) were parsed from a
  single block and produced null description/constraints. Fixed by replacing
  single-block selection with _assemble_group_text(), which concatenates text
  from ALL non-empty blocks in the group before calling _parse_planguage_lines().
"""

import re
from typing import Optional

from block_grouper import RequirementGroup
from schemas import ExtractedRequirement
from utils import clean_text, build_source_reference, map_confidence


# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

_LABEL_RE = re.compile(
    r'^(Tag|Gist|Scale|Meter|Must|Wish|Plan|Defined)\s*:\s*',
    re.IGNORECASE,
)

_MODAL = re.compile(r'\b(should|shall|must|can|will)\b', re.IGNORECASE)
_SENTENCE_SEP = re.compile(r'(?<=[.!?])\s+')


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _assemble_group_text(group: RequirementGroup) -> str:
    """Concatenate text from all blocks in the group into one parseable string.

    BUG FIX: the original _select_content_block() returned only the first
    matching block. When PLanguage keywords are spread across multiple blocks
    (one block per keyword line, as in 3.5.1 SystemReliability), only the Tag
    line was parsed and all other fields came back null.

    This function concatenates ALL block texts so _parse_planguage_lines()
    can see every keyword regardless of how many blocks they span.
    """
    parts: list[str] = []
    for block in group.blocks:
        text = block.get("text", "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts)


def _parse_planguage_lines(text: str) -> dict[str, str]:
    """Parse PLanguage keyword lines from a multi-line text block.

    Returns a dict with lower-cased label names as keys:
        tag, gist, scale, meter, must, wish, plan, defined

    Multi-line continuation values are joined with a single space.
    """
    fields: dict[str, str] = {}
    current_label: Optional[str] = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        m = _LABEL_RE.match(line)
        if m:
            label = m.group(1).lower()
            value = clean_text(line[m.end():])
            fields[label] = value
            current_label = label
        elif current_label is not None:
            continuation = clean_text(line)
            if continuation:
                fields[current_label] = fields[current_label] + " " + continuation

    return fields


def _extract_actions(description: Optional[str]) -> list[str]:
    """Return modal-verb sentences from description, or full description."""
    if not description:
        return []
    sentences = [s.strip() for s in _SENTENCE_SEP.split(description) if s.strip()]
    actions = [clean_text(s) for s in sentences if _MODAL.search(s)]
    if not actions:
        cleaned = clean_text(description)
        return [cleaned] if cleaned else []
    return actions


def _parse_tag(tag_value: str) -> tuple[str, str]:
    """Split a Tag value into (req_id, title).

    The first whitespace-separated token is req_id; everything after is title.
    If there is no following text, title equals req_id.
    """
    parts = tag_value.split(None, 1)
    if not parts:
        return "", ""
    req_id = parts[0]
    title = clean_text(parts[1]) if len(parts) > 1 else req_id
    return req_id, title


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_planguage(group: RequirementGroup) -> ExtractedRequirement:
    """Extract a structured ExtractedRequirement from a planguage-format group.

    Assembles text from ALL blocks (not just the first matching block),
    parses PLanguage keyword lines, derives actor (always "System") and
    actions from the Gist field, then assembles the output schema.

    Args:
        group: A RequirementGroup with format == "planguage".

    Returns:
        A fully populated ExtractedRequirement instance.
    """
    first_block: Optional[dict] = group.blocks[0] if group.blocks else None

    # Empty group edge case
    if first_block is None:
        return ExtractedRequirement(
            req_id                = group.candidate_req_id or "",
            section_path          = group.section_path,
            section_semantic_type = group.section_semantic_type,
            input_format          = "planguage",
            extraction_method     = "deterministic",
            title                 = "",
            actor                 = "System",
            actions               = [],
            description           = None,
            constraints           = None,
            dependencies          = [],
            acceptance_criteria   = None,
            outputs               = None,
            planguage_table       = None,
            scenarios             = None,
            confidence            = "low",
            source_reference      = {
                "doc_id":         None,
                "section_path":   group.section_path,
                "source_locator": None,
                "module":         None,
                "version":        None,
            },
        )

    # BUG FIX: assemble text from ALL blocks, not just one
    raw_text = _assemble_group_text(group)
    fields = _parse_planguage_lines(raw_text)

    # ── Tag → req_id + title ──────────────────────────────────────────────
    tag_value = fields.get("tag", "")
    if tag_value:
        req_id, title = _parse_tag(tag_value)
    else:
        req_id = (
            first_block.get("candidate_req_id")
            or group.candidate_req_id
            or ""
        )
        title = ""

    # ── Field mapping ─────────────────────────────────────────────────────
    description = fields.get("gist") or None
    constraints = fields.get("must")  or None
    outputs     = fields.get("wish")  or None
    # scale, meter, plan, defined — parsed but not mapped to output schema fields.
    # They are preserved in acceptance_criteria as a structured summary if present.
    scale   = fields.get("scale")
    meter   = fields.get("meter")
    plan    = fields.get("plan")
    defined = fields.get("defined")

    acceptance_criteria: Optional[str] = None
    measurement_parts = []
    if scale:
        measurement_parts.append(f"Scale: {scale}")
    if meter:
        measurement_parts.append(f"Meter: {meter}")
    if plan:
        measurement_parts.append(f"Plan: {plan}")
    if defined:
        measurement_parts.append(f"Defined: {defined}")
    if measurement_parts:
        acceptance_criteria = "; ".join(measurement_parts)

    # ── Actions from Gist ─────────────────────────────────────────────────
    actions = _extract_actions(description)

    # ── Confidence ────────────────────────────────────────────────────────
    has_nulls = description is None or constraints is None
    confidence = map_confidence("deterministic", has_nulls)

    # ── Source reference ──────────────────────────────────────────────────
    source_ref = build_source_reference(first_block)

    return ExtractedRequirement(
        req_id                = req_id or group.candidate_req_id or "",
        section_path          = group.section_path,
        section_semantic_type = group.section_semantic_type,
        input_format          = "planguage",
        extraction_method     = "deterministic",
        title                 = title,
        actor                 = "System",
        actions               = actions,
        description           = description,
        constraints           = constraints,
        dependencies          = [],
        acceptance_criteria   = acceptance_criteria,
        outputs               = outputs,
        planguage_table       = None,
        scenarios             = None,
        confidence            = confidence,
        source_reference      = source_ref,
    )