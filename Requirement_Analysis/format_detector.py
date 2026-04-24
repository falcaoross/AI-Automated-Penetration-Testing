from __future__ import annotations

"""format_detector.py — Detect the input format of a RequirementGroup.

Single responsibility: inspect the structural_role values of a group's
blocks and return one of "labeled", "gherkin", "planguage", or "prose".

No extraction logic. No LLM calls. No file I/O.
Detection is based solely on structural_role — no block text is read.
"""

from block_grouper import RequirementGroup


def detect_format(group: RequirementGroup) -> str:
    """Return the input format string for a RequirementGroup.

    Evaluates four format conditions in strict priority order and returns
    on the first match. The group is never mutated; the caller is
    responsible for assigning the result to group.format.

    Args:
        group: A RequirementGroup produced by block_grouper.group_blocks().

    Returns:
        One of: "labeled", "gherkin", "planguage", "prose".
    """
    # Single pass — collect the set of structural_role values once.
    roles: set[str] = {b["structural_role"] for b in group.blocks}

    has_id      = "req_id_label"    in roles
    has_desc    = "req_desc_label"  in roles
    has_gherkin = "gherkin_keyword" in roles

    # ── Priority 1: labeled ───────────────────────────────────────────────
    # Checked before gherkin: some DESC fields open with "Given that …"
    # which would false-trigger the gherkin branch if order were reversed.
    if has_id and has_desc:
        return "labeled"

    # ── Priority 2: gherkin ───────────────────────────────────────────────
    if has_id and has_gherkin and not has_desc:
        return "gherkin"

    # ── Priority 3: planguage ─────────────────────────────────────────────
    # After excluding section_heading and req_id_label, every remaining
    # block must have a structural_role that starts with "planguage_".
    # A group with no such remaining blocks does NOT qualify.
    if has_id:
        _EXCLUDED = {
            "section_heading",
            "req_id_label",
            "prose_content",
            "table_block",
        }
        remaining = [
            b["structural_role"]
            for b in group.blocks
            if b["structural_role"] not in _EXCLUDED
        ]
        if remaining and all(r.startswith("planguage_") for r in remaining):
            return "planguage"

    # ── Priority 4: prose ─────────────────────────────────────────────────
    return "prose"
