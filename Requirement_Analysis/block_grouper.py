from __future__ import annotations

"""block_grouper.py — Group Layer 1 blocks into per-requirement units.

Single responsibility: partition the flat block list produced by
block_classifier.py into RequirementGroup objects, one per unique
section_path that belongs to a processable semantic type.

No extraction logic, no format detection, no LLM calls.
"""

from dataclasses import dataclass, field
from typing import Optional
from collections import defaultdict

from utils import clean_text

# ---------------------------------------------------------------------------
# Processable semantic types — routing signal for Layer 2
# ---------------------------------------------------------------------------

PROCESSABLE: frozenset[str] = frozenset({
    "functional_requirements",
    "performance_requirements",
    "interface_requirements",
    "design_constraints",
    "quality_attributes",
})

# ---------------------------------------------------------------------------
# RequirementGroup dataclass
# ---------------------------------------------------------------------------

@dataclass
class RequirementGroup:
    """A collection of contiguous blocks belonging to one requirement section."""

    section_path: str
    section_semantic_type: str
    section_title: str
    candidate_req_id: str | None
    blocks: list[dict] = field(default_factory=list)
    format: str | None = None

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _skeleton_sst(section_path: str, document_skeleton: dict) -> Optional[str]:
    """Look up section_semantic_type for a path directly in the skeleton.

    Returns None if the path is not found or has no section_semantic_type.
    """
    entry = document_skeleton.get(section_path)
    if not entry:
        return None
    return entry.get("section_semantic_type") or None

def _resolve_sst_from_skeleton(
    section_path: str,
    document_skeleton: dict,
) -> Optional[str]:
    """Walk up the skeleton tree ONE level to resolve a processable SST.

    Used to recover blocks whose section_semantic_type is "other" or missing
    in the blocks file due to block_classifier.py inheritance gaps (Bug #2).

    Returns a processable SST string if found in the direct parent, else None.
    Only walks one level — no recursion.
    """
    parent_path = ".".join(section_path.split(".")[:-1])
    if not parent_path:
        return None
    parent_sst = _skeleton_sst(parent_path, document_skeleton)
    if parent_sst and parent_sst in PROCESSABLE:
        return parent_sst
    return None

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def group_blocks(
    blocks: list[dict],
    document_skeleton: dict,
) -> tuple[list[RequirementGroup], dict]:
    """Group blocks into RequirementGroup objects and return grouping audit stats.

    Args:
        blocks:            Flat list of block dicts from block_classifier.py.
        document_skeleton: Full skeleton dict from Layer 1.

    Returns:
        A 2-tuple of:
            [0] list[RequirementGroup] — one group per processable section_path,
                in document order, including groups with no req_id_label.
            [1] dict — audit_stats with counts and collected warnings.
    """
    warnings: list[dict] = []

    # ── Step 1 — Filter ───────────────────────────────────────────────────
    # BUG FIX #2 — blocks for 3.4.x arrive with section_semantic_type "other"
    # because block_classifier.py's inheritance patch didn't cover
    # design_constraints. The grouper already receives document_skeleton but
    # the original code never used it for recovery. Now, any block whose SST
    # is not in PROCESSABLE (or is "other") is given a second chance: look up
    # the direct parent in the skeleton and use its SST if it is processable.
    # A WARN-SST-RECOVERED warning is emitted for every block that required
    # recovery so the issue is visible in the audit log.
    recovered_sections: set[str] = set()  # track to emit one warn per section

    def _effective_sst(block: dict) -> str:
        """Return the block's SST, recovering from skeleton if needed."""
        sst = block.get("section_semantic_type", "")
        if sst in PROCESSABLE:
            return sst
        # Attempt single-level skeleton recovery
        recovered = _resolve_sst_from_skeleton(
            block["section_path"], document_skeleton
        )
        if recovered:
            sp = block["section_path"]
            if sp not in recovered_sections:
                recovered_sections.add(sp)
                warnings.append({
                    "code": "WARN-SST-RECOVERED",
                    "section_path": sp,
                    "original_sst": sst or "other",
                    "recovered_sst": recovered,
                    "message": (
                        f"Section {sp} had section_semantic_type "
                        f"'{sst or 'other'}' in blocks file — "
                        f"recovered '{recovered}' from skeleton parent. "
                        f"Fix block_classifier.py inheritance patch."
                    ),
                })
            return recovered
        return sst  # still not processable — will be excluded below

    filtered = [
        b for b in blocks
        if b["skip"] is False
        and _effective_sst(b) in PROCESSABLE
    ]

    # After recovery, stamp the corrected SST onto each block so downstream
    # steps see the right value without re-calling _effective_sst.
    for b in filtered:
        b["section_semantic_type"] = _effective_sst(b)

    # ── Step 2 — Group by section_path, preserving document order ─────────
    grouped: dict[str, list[dict]] = defaultdict(list)
    for block in filtered:
        grouped[block["section_path"]].append(block)

    # ── Steps 3 & 4 — Build RequirementGroup per candidate group ──────────
    groups: list[RequirementGroup] = []

    for section_path, section_blocks in grouped.items():
        sst = section_blocks[0]["section_semantic_type"]
        raw_title = section_blocks[0].get("section_title", "") or ""
        section_title = clean_text(raw_title)

        # Find first req_id_label block for candidate_req_id
        candidate_req_id: str | None = None
        for b in section_blocks:
            if b["structural_role"] == "req_id_label":
                candidate_req_id = b["candidate_req_id"]
                break

        # BUG FIX #1 — the original code checked has_req_content (only true
        # for labeled/gherkin/planguage structural roles) and silently skipped
        # sections where it was False. Pure-prose interface sections (3.1.1,
        # 3.1.2, 3.1.4) have only prose_content blocks, so has_req_content
        # was always False for them and they were dropped with no warning.
        #
        # New rule: any section_path that survived the Step 1 filter has at
        # least one non-skipped processable block and therefore IS a real
        # requirement section. If it has no req_id_label, emit WARN-NO-REQ-ID
        # and include it as a prose-format group. Never silently skip.
        if candidate_req_id is None:
            warnings.append({
                "code": "WARN-NO-REQ-ID",
                "section_path": section_path,
                "section_semantic_type": sst,
                "message": (
                    f"No req_id_label block found in section {section_path}. "
                    "Group included with candidate_req_id=None."
                ),
            })

        groups.append(RequirementGroup(
            section_path=section_path,
            section_semantic_type=sst,
            section_title=section_title,
            candidate_req_id=candidate_req_id,
            blocks=section_blocks,
            format=None,
        ))

    # ── Step 5 — Boundary divergence check ────────────────────────────────
    parent_paths: set[str] = set()
    for g in groups:
        parts = g.section_path.split(".")
        for depth in range(1, len(parts)):
            parent_paths.add(".".join(parts[:depth]))

    for parent_path in sorted(parent_paths):
        prefix = parent_path + "."
        sub_groups = [g for g in groups if g.section_path.startswith(prefix)]
        if not sub_groups:
            continue

        sub_group_count = len(sub_groups)

        distinct_ids: set[str] = set()
        for g in sub_groups:
            for b in g.blocks:
                cid = b.get("candidate_req_id")
                if cid is not None:
                    distinct_ids.add(cid)
        candidate_id_count = len(distinct_ids)

        if abs(sub_group_count - candidate_id_count) > 1:
            warn = {
                "code": "WARN-MISSING-REQUIREMENT-BOUNDARIES",
                "section_path": parent_path,
                "sub_group_count": sub_group_count,
                "candidate_id_count": candidate_id_count,
                "message": (
                    f"Sub-group count ({sub_group_count}) differs from distinct "
                    f"candidate_req_id count ({candidate_id_count}) by more than 1 "
                    f"in parent section {parent_path}. Possible boundary bleed."
                ),
            }
            warnings.append(warn)

    # ── Audit stats ───────────────────────────────────────────────────────
    by_sst: dict[str, int] = {sst: 0 for sst in PROCESSABLE}
    for g in groups:
        if g.section_semantic_type in by_sst:
            by_sst[g.section_semantic_type] += 1

    warn_no_req_id_count = sum(
        1 for w in warnings if w["code"] == "WARN-NO-REQ-ID"
    )
    warn_missing_boundaries_count = sum(
        1 for w in warnings if w["code"] == "WARN-MISSING-REQUIREMENT-BOUNDARIES"
    )
    warn_sst_recovered_count = sum(
        1 for w in warnings if w["code"] == "WARN-SST-RECOVERED"
    )

    audit_stats: dict = {
        "total_groups": len(groups),
        "by_semantic_type": {
            "functional_requirements":  by_sst.get("functional_requirements", 0),
            "performance_requirements": by_sst.get("performance_requirements", 0),
            "interface_requirements":   by_sst.get("interface_requirements", 0),
            "design_constraints":       by_sst.get("design_constraints", 0),
            "quality_attributes":       by_sst.get("quality_attributes", 0),
        },
        "warn_no_req_id_count":          warn_no_req_id_count,
        "warn_missing_boundaries_count": warn_missing_boundaries_count,
        "warn_sst_recovered_count":      warn_sst_recovered_count,
        "warnings":                      warnings,
    }

    return groups, audit_stats