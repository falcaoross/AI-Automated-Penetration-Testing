from __future__ import annotations

"""cru_validator.py — Layer 3: Validate every CRU against the contract schema.

Single responsibility: inspect each CRU produced by cru_builder and verify it
satisfies every mandatory-field and referential-integrity rule. Validation
failures mark the CRU as invalid=True and write a structured audit flag.
The CRU is still included in the output — downstream layers decide whether to
skip invalid CRUs. Nothing is silently dropped here.

Validation rules (in evaluation order):
  V1  cru_id is non-empty and unique across all CRUs in the batch.
  V2  parent_requirement_id exists in the known req_id set from Layer 2.
  V3  actor is non-null and non-empty string.
  V4  action is non-null, OR the CRU was explicitly produced as a
      null-action placeholder (i.e. WARN-NO-ACTION-CRU was raised for its
      parent). Null-action placeholders are valid — just incomplete.
  V5  confidence is one of: high, medium, low.
  V6  traceability.section_path is a valid section path present in the
      Layer 1 skeleton.
  V7  type is one of the known CRU type vocabulary.

Rule V4 nuance: a CRU with action=None is NOT marked invalid if it came
from a requirement with an empty actions list (expected edge case: QR6,
QR10, QR11). It IS marked invalid if action=None on a CRU whose parent
had a non-empty actions list — which would indicate a builder bug.
"""

import re
from dataclasses import dataclass
from typing import Optional

from cru_builder import CRU


# ---------------------------------------------------------------------------
# Allowed vocabularies
# ---------------------------------------------------------------------------

_VALID_CONFIDENCES: frozenset[str] = frozenset({"high", "medium", "low"})

_VALID_TYPES: frozenset[str] = frozenset({
    "functional",
    "performance",
    "constraint",
    "reliability",
    "security",
    "portability",
    "testability",
    "usability",
    "availability",
    "maintainability",
    "quality",
})

# Section path must be dot-separated numeric tokens, e.g. "3.2.1.7"
_SECTION_PATH_RE = re.compile(r"^\d+(\.\d+)*$")


# ---------------------------------------------------------------------------
# Audit flag helper
# ---------------------------------------------------------------------------

def _vflag(
    code: str,
    cru_id: str,
    parent_req_id: str,
    rule: str,
    message: str,
) -> dict:
    return {
        "code":           code,
        "cru_id":         cru_id,
        "parent_req_id":  parent_req_id,
        "rule":           rule,
        "message":        message,
        "severity":       "error",
    }


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    total:          int
    invalid_count:  int
    flags:          list[dict]

    @property
    def clean(self) -> bool:
        return self.invalid_count == 0


# ---------------------------------------------------------------------------
# Individual rule checkers
# ---------------------------------------------------------------------------

def _check_v1_uniqueness(
    crus: list[CRU],
) -> tuple[set[str], list[dict]]:
    """
    V1: cru_id must be unique across the entire batch.
    Returns (duplicate_ids, flags).
    """
    seen: set[str] = set()
    duplicates: set[str] = set()
    flags: list[dict] = []

    for cru in crus:
        if cru.cru_id in seen:
            duplicates.add(cru.cru_id)
        seen.add(cru.cru_id)

    for dup in sorted(duplicates):
        flags.append(_vflag(
            "ERR-DUPLICATE-CRU-ID",
            dup,
            "UNKNOWN",
            "V1",
            f"cru_id '{dup}' appears more than once in the CRU batch. "
            "cru_id must be globally unique.",
        ))

    return duplicates, flags


def _check_v2_parent_exists(
    cru: CRU,
    known_req_ids: set[str],
) -> Optional[dict]:
    """V2: parent_requirement_id must exist in the Layer 2 req_id set."""
    if cru.parent_requirement_id not in known_req_ids:
        return _vflag(
            "ERR-UNKNOWN-PARENT-REQ-ID",
            cru.cru_id,
            cru.parent_requirement_id,
            "V2",
            f"parent_requirement_id '{cru.parent_requirement_id}' does not exist "
            "in the Layer 2 requirements list.",
        )
    return None


def _check_v3_actor(cru: CRU) -> Optional[dict]:
    """V3: actor must be non-null and non-empty."""
    if not cru.actor or not cru.actor.strip():
        return _vflag(
            "ERR-MISSING-ACTOR",
            cru.cru_id,
            cru.parent_requirement_id,
            "V3",
            "actor is null or empty. Every CRU must have an actor.",
        )
    return None


def _check_v4_action(
    cru: CRU,
    null_action_parents: set[str],
) -> Optional[dict]:
    """
    V4: action=None is valid only if this CRU's parent was in the
    null_action_parents set (i.e. WARN-NO-ACTION-CRU was raised for it).
    If action=None on a parent that had actions, that is a builder bug.
    """
    if cru.action is None and cru.parent_requirement_id not in null_action_parents:
        return _vflag(
            "ERR-UNEXPECTED-NULL-ACTION",
            cru.cru_id,
            cru.parent_requirement_id,
            "V4",
            "action is null but parent requirement was not flagged as having "
            "an empty actions list. This indicates a cru_builder defect.",
        )
    return None


def _check_v5_confidence(cru: CRU) -> Optional[dict]:
    """V5: confidence must be one of high / medium / low."""
    if cru.confidence not in _VALID_CONFIDENCES:
        return _vflag(
            "ERR-INVALID-CONFIDENCE",
            cru.cru_id,
            cru.parent_requirement_id,
            "V5",
            f"confidence '{cru.confidence}' is not in the allowed vocabulary "
            f"{sorted(_VALID_CONFIDENCES)}.",
        )
    return None


def _check_v6_traceability(
    cru: CRU,
    skeleton_paths: set[str],
) -> Optional[dict]:
    """
    V6: traceability.section_path must be present in the Layer 1 skeleton.
    Also validates that section_path is well-formed (dot-separated numerics).
    Skeleton paths may be empty (no skeleton passed) — skip check in that case.
    """
    section_path = (cru.traceability or {}).get("section_path")

    if not section_path:
        return _vflag(
            "ERR-MISSING-TRACEABILITY",
            cru.cru_id,
            cru.parent_requirement_id,
            "V6",
            "traceability.section_path is null or missing. "
            "Every CRU must be traceable to a source section.",
        )

    if not _SECTION_PATH_RE.match(section_path):
        return _vflag(
            "ERR-MALFORMED-SECTION-PATH",
            cru.cru_id,
            cru.parent_requirement_id,
            "V6",
            f"traceability.section_path '{section_path}' is not a valid "
            "dot-separated numeric path (e.g. '3.2.1.7').",
        )

    if skeleton_paths and section_path not in skeleton_paths:
        return _vflag(
            "ERR-SECTION-PATH-NOT-IN-SKELETON",
            cru.cru_id,
            cru.parent_requirement_id,
            "V6",
            f"traceability.section_path '{section_path}' does not exist in "
            "the Layer 1 skeleton. Possible traceability break.",
        )

    return None


def _check_v7_type(cru: CRU) -> Optional[dict]:
    """V7: type must be one of the known CRU type vocabulary."""
    if cru.type not in _VALID_TYPES:
        return _vflag(
            "ERR-INVALID-CRU-TYPE",
            cru.cru_id,
            cru.parent_requirement_id,
            "V7",
            f"type '{cru.type}' is not in the allowed CRU type vocabulary "
            f"{sorted(_VALID_TYPES)}.",
        )
    return None


# ---------------------------------------------------------------------------
# Core validator
# ---------------------------------------------------------------------------

def validate_crus(
    crus: list[CRU],
    builder_flags: list[dict],
    known_req_ids: set[str],
    skeleton_paths: set[str],
) -> ValidationResult:
    """
    Validate every CRU in the batch against all seven rules.

    Args:
        crus:            CRU list from cru_builder.build_crus().
        builder_flags:   Audit flags from cru_builder.build_crus(). Used to
                         identify null-action parents (V4).
        known_req_ids:   Set of req_id strings from Layer 2 requirements.
                         Used for V2 referential integrity check.
        skeleton_paths:  Set of section_path strings from Layer 1 skeleton.
                         Pass empty set to skip skeleton check (V6 partial).

    Returns:
        ValidationResult with invalid_count, flags, and clean property.
        CRUs with failures are mutated in-place: invalid=True.
    """
    all_flags: list[dict] = []

    # Build null-action parent set from builder flags (V4 reference)
    null_action_parents: set[str] = {
        f["req_id"]
        for f in builder_flags
        if f.get("code") == "WARN-NO-ACTION-CRU"
    }

    # V1: batch-level uniqueness check
    duplicate_ids, v1_flags = _check_v1_uniqueness(crus)
    all_flags.extend(v1_flags)

    # Per-CRU checks
    for cru in crus:
        cru_flags: list[dict] = []

        # Skip per-CRU checks for duplicate IDs — already flagged at batch level
        if cru.cru_id in duplicate_ids:
            cru.invalid = True
            continue

        flag = _check_v2_parent_exists(cru, known_req_ids)
        if flag:
            cru_flags.append(flag)

        flag = _check_v3_actor(cru)
        if flag:
            cru_flags.append(flag)

        flag = _check_v4_action(cru, null_action_parents)
        if flag:
            cru_flags.append(flag)

        flag = _check_v5_confidence(cru)
        if flag:
            cru_flags.append(flag)

        flag = _check_v6_traceability(cru, skeleton_paths)
        if flag:
            cru_flags.append(flag)

        flag = _check_v7_type(cru)
        if flag:
            cru_flags.append(flag)

        if cru_flags:
            cru.invalid = True
            all_flags.extend(cru_flags)

    invalid_count = sum(1 for c in crus if c.invalid)

    return ValidationResult(
        total=len(crus),
        invalid_count=invalid_count,
        flags=all_flags,
    )