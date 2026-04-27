"""
Stage 2B - Structured Requirement Assembly
==========================================
Reads paragraph-level requirement candidates from Stage 2A output
(requirements_extracted.json) and assembles them into logical, structured
requirement objects grouped by section_path.

NON-NEGOTIABLE PRINCIPLES:
  - Never re-detect requirements; trust Stage 2A completely
  - Never read Stage 1 outputs
  - Never modify paragraph text
  - Never cross section boundaries when grouping
  - Never create CRUs or test cases
  - Full traceability preserved via source_ref on every output record

Outputs:
  requirements_extracted_grouped.json        - structured requirement objects
  requirements_extracted_grouped_audit.json  - grouping audit log

Author: Autopilot-QA Stage 2B
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ===========================================================================
# CONFIGURATION
# ===========================================================================

INPUT_PATH    = "../Requirement_Analysis/output/requirements_extracted.json"
OUTPUT_DIR    = "../Requirement_Analysis/output"
OUTPUT_GROUPED = os.path.join(OUTPUT_DIR, "requirements_extracted_grouped.json")
OUTPUT_AUDIT   = os.path.join(OUTPUT_DIR, "requirements_extracted_grouped_audit.json")

# Minimum paragraph confidence score to attach as supporting detail
# (when strictly_qualifies == false)
SUPPORT_CONFIDENCE_THRESHOLD = 0.45

# Roles that qualify a non-anchor paragraph for attachment to the current group
ATTACHABLE_ROLES = {
    "input_definition",
    "system_behavior",
    "output_definition",
    "constraint",
}

# Confidence grade ordering
GRADE_HIGH   = 0.75
GRADE_MEDIUM = 0.45

# ===========================================================================
# BOOTSTRAP
# ===========================================================================

if not os.path.exists(INPUT_PATH):
    raise FileNotFoundError(
        f"❌ Stage 2A output not found at: {INPUT_PATH}\n"
        "Run requirement_understanding_engine.py first."
    )

with open(INPUT_PATH, "r", encoding="utf-8") as fh:
    stage2a: Dict[str, Any] = json.load(fh)

print(" Loaded requirements_extracted.json")
print(f"   doc_id           : {stage2a['metadata']['doc_id']}")
print(f"   source_file      : {stage2a['metadata']['source_file']}")
print(f"   total_candidates : {stage2a['metadata']['total_candidates']}")


# ===========================================================================
# UTILITIES
# ===========================================================================

def _stable_requirement_id(doc_id: str, section_path: str, anchor_para_id: str) -> str:
    """
    Deterministic requirement ID derived from doc_id + section_path + anchor_para_id.
    Same inputs always produce the same ID - stable across re-runs.
    """
    raw = f"{doc_id}|{section_path}|{anchor_para_id}"
    digest = hashlib.sha1(raw.encode()).hexdigest()[:10]
    return f"REQ-{digest}"


def _derive_title(raw_text: str, max_words: int = 12) -> str:
    """
    Derive a short title from the anchor paragraph's raw_text.
    Takes the first sentence (up to max_words words).
    Does NOT paraphrase - trims only.
    """
    # Take the first sentence or the first line, whichever is shorter
    first_sentence = re.split(r'(?<=[.!?])\s+', raw_text.strip())[0]
    first_line = raw_text.strip().split("\n")[0]
    candidate = first_sentence if len(first_sentence) <= len(first_line) else first_line

    words = candidate.split()
    if len(words) <= max_words:
        return candidate.strip()
    return " ".join(words[:max_words]).strip() + "..."


def _aggregate_confidence(para_confidences: List[Dict]) -> Dict[str, Any]:
    """
    Aggregate paragraph-level confidence scores into a group-level score.
    Strategy: weighted average where strictly-qualifying paragraphs
    contribute full weight and supporting paragraphs contribute half weight.
    Grade is derived from the aggregated score.
    """
    if not para_confidences:
        return {"score": 0.01, "grade": "LOW"}

    total_weight = 0.0
    weighted_sum = 0.0
    for pc in para_confidences:
        score = pc.get("score", 0.0)
        weight = 1.0 if pc.get("strictly_qualifies", False) else 0.5
        weighted_sum += score * weight
        total_weight += weight

    agg_score = round(weighted_sum / total_weight, 4) if total_weight > 0 else 0.01
    agg_score = min(max(agg_score, 0.01), 0.99)

    if agg_score >= GRADE_HIGH:
        grade = "HIGH"
    elif agg_score >= GRADE_MEDIUM:
        grade = "MEDIUM"
    else:
        grade = "LOW"

    return {"score": agg_score, "grade": grade}


# Section header label artifacts that must be stripped from description text
_SECTION_LABEL_RE = re.compile(
    r'^\s*(?:Inputs|Outputs|System\s+Behavior|Constraints)\s*:\s*$',
    re.IGNORECASE | re.MULTILINE,
)

# Semantic role -> slot name mapping (used by slot population rules)
_SLOT_ROLE_MAP: Dict[str, str] = {
    "input_definition": "inputs",
    "system_behavior":  "system_behavior",
    "output_definition": "outputs",
    "constraint":       "constraints",
}

# Roles that are purely structural / normative and never populate a semantic slot
_NON_SLOT_ROLES = {"requirement_statement", "narrative"}


def _strip_section_labels(text: str) -> str:
    """Remove bare section header labels ('Inputs:', 'Outputs:', etc.) from text."""
    cleaned = _SECTION_LABEL_RE.sub("", text)
    # Collapse runs of blank lines produced by removal
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    return cleaned.strip()


def _classify_paragraph_for_slots(
    para: Dict,
) -> Tuple[Optional[str], bool]:
    """
    Determine how a paragraph participates in slot population.

    Returns:
      (slot_name, is_slot_eligible)

    RULE S1: A paragraph populates a slot ONLY IF its roles justify exactly
             one semantic slot target AND it has no other semantic role that
             would make it mixed-purpose.
    RULE S2: If a paragraph carries MULTIPLE semantic roles (roles that map
             to different slots, OR a slot role combined with
             requirement_statement), it is mixed-role -> slot_name=None,
             goes only to description.
    """
    roles = set(para.get("paragraph_roles", []))

    # Identify which slot roles this paragraph carries
    slot_roles = roles - _NON_SLOT_ROLES

    if not slot_roles:
        # Only narrative / requirement_statement - no slot
        return None, False

    # Map slot_roles to their target slots
    target_slots = {_SLOT_ROLE_MAP[r] for r in slot_roles if r in _SLOT_ROLE_MAP}

    if len(target_slots) != 1:
        # Maps to zero or multiple slots -> mixed-role, no slot population
        return None, False

    # Single target slot.  Check for mixing with requirement_statement.
    if "requirement_statement" in roles:
        # Mixed with a normative statement -> goes only to description
        return None, False

    return target_slots.pop(), True


def _build_slots_and_description(
    group: List[Dict],
    anchor: Dict,
) -> Dict[str, Optional[str]]:
    """
    Populate semantic slots and description from the group following rules S1-S4.

    Returns a dict with keys:
      description, inputs, system_behavior, outputs, constraints
    """
    slot_parts: Dict[str, List[str]] = {
        "inputs": [],
        "system_behavior": [],
        "outputs": [],
        "constraints": [],
    }
    description_parts: List[str] = []

    for para in group:
        slot_name, is_slot_eligible = _classify_paragraph_for_slots(para)
        raw = para["raw_text"].strip()

        if is_slot_eligible and slot_name:
            # RULE S1: single-slot paragraph -> populate that slot only
            slot_parts[slot_name].append(raw)
            # Does NOT go into description (RULE S4: exclude slot paragraphs)
        else:
            # Mixed-role or narrative/normative paragraph -> description only
            # RULE S4: strip embedded section header labels
            cleaned = _strip_section_labels(raw)
            if cleaned:
                description_parts.append(cleaned)

    # RULE S3: null if no content for a slot
    result: Dict[str, Optional[str]] = {
        "description": "\n\n".join(description_parts) if description_parts else None,
        "inputs":          "\n".join(slot_parts["inputs"])          or None,
        "system_behavior": "\n".join(slot_parts["system_behavior"]) or None,
        "outputs":         "\n".join(slot_parts["outputs"])         or None,
        "constraints":     "\n".join(slot_parts["constraints"])     or None,
    }
    return result


# ===========================================================================
# GROUPER
# ===========================================================================

class SectionGrouper:
    """
    Groups paragraph candidates within a single section_path into logical
    requirement groups.

    Grouping algorithm (per section_path):
      1. Scan paragraphs in document order (preserved from Stage 2A input order).
      2. When a paragraph with strictly_qualifies == true is found:
         - flush the current group (if any)
         - start a new group with this paragraph as anchor
      3. For subsequent non-anchor paragraphs in the same section:
         - attach if: paragraph_roles ∩ ATTACHABLE_ROLES is non-empty
           OR strictly_qualifies == false AND confidence.score ≥ threshold
         - stop attaching (flush group) when: another anchor is found,
           section_path changes (handled externally), or role is purely narrative
      4. At end of section: flush any open group.

    A paragraph is purely narrative if paragraph_roles == ["narrative"].
    No paragraph belongs to more than one group.
    """

    def group_section(
        self,
        section_path: str,
        candidates: List[Dict],
    ) -> Tuple[List[List[Dict]], List[Dict]]:
        """
        Returns:
          groups   - list of groups; each group is an ordered list of candidate dicts
          orphans  - candidates not assigned to any group
        """
        groups: List[List[Dict]] = []
        orphans: List[Dict] = []

        current_group: List[Dict] = []

        def flush():
            nonlocal current_group
            if current_group:
                groups.append(current_group)
                current_group = []

        for candidate in candidates:
            roles = set(candidate.get("paragraph_roles", []))
            strictly = candidate.get("strictly_qualifies", False)
            conf_score = (candidate.get("confidence") or {}).get("score", 0.0)
            is_purely_narrative = (roles == {"narrative"})

            if strictly:
                # New anchor -> flush previous group, start fresh
                flush()
                current_group = [candidate]
                continue

            # Non-anchor paragraph
            if not current_group:
                # No open group; orphan unless we want to start one
                # (non-anchor paragraphs cannot start a group)
                orphans.append({
                    "para_id": candidate["para_id"],
                    "section_path": section_path,
                    "reason": "non_anchor_paragraph_with_no_open_group",
                })
                continue

            if is_purely_narrative:
                # Purely narrative paragraph closes the group
                flush()
                orphans.append({
                    "para_id": candidate["para_id"],
                    "section_path": section_path,
                    "reason": "purely_narrative_closes_group",
                })
                continue

            # Attach if roles overlap with ATTACHABLE_ROLES or confidence ≥ threshold
            has_attachable_role = bool(roles & ATTACHABLE_ROLES)
            meets_confidence = (not strictly) and (conf_score >= SUPPORT_CONFIDENCE_THRESHOLD)

            if has_attachable_role or meets_confidence:
                current_group.append(candidate)
            else:
                orphans.append({
                    "para_id": candidate["para_id"],
                    "section_path": section_path,
                    "reason": "no_attachable_role_and_below_confidence_threshold",
                })

        flush()
        return groups, orphans


# ===========================================================================
# ASSEMBLER
# ===========================================================================

class RequirementAssembler:
    """
    Takes a group of paragraph candidates (already validated by SectionGrouper)
    and assembles ONE structured requirement object.
    """

    def assemble(
        self,
        group: List[Dict],
        doc_id: str,
        section_path: str,
        seq: int,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Returns:
          requirement  - structured requirement dict
          group_log    - grouping audit entry for this requirement
        """
        # Identify anchor (first strictly_qualifies == true paragraph)
        anchor = next((p for p in group if p.get("strictly_qualifies")), group[0])
        anchor_para_id = anchor["para_id"]

        req_id = _stable_requirement_id(doc_id, section_path, anchor_para_id)
        title  = _derive_title(anchor["raw_text"])

        # Semantic slot population - rules S1-S4
        slots = _build_slots_and_description(group, anchor)
        description          = slots["description"]
        inputs_text          = slots["inputs"]
        system_behavior_text = slots["system_behavior"]
        outputs_text         = slots["outputs"]
        constraints_text     = slots["constraints"]

        # Source reference
        para_ids   = [p["para_id"] for p in group]
        pages      = [p["page"] for p in group if p.get("page") is not None]
        page_range = [min(pages), max(pages)] if pages else [None, None]

        # Aggregated confidence
        para_conf_inputs = [
            {"score": (p.get("confidence") or {}).get("score", 0.0),
             "strictly_qualifies": p.get("strictly_qualifies", False)}
            for p in group
        ]
        confidence = _aggregate_confidence(para_conf_inputs)

        requirement: Dict[str, Any] = {
            "requirement_id": req_id,
            "seq": seq,
            "title": title,
            "description": description,
            "inputs": inputs_text,
            "system_behavior": system_behavior_text,
            "outputs": outputs_text,
            "constraints": constraints_text,
            "source_ref": {
                "doc_id": doc_id,
                "section_path": section_path,
                "para_ids": para_ids,
                "page_range": page_range,
            },
            "confidence": confidence,
        }

        group_log: Dict[str, Any] = {
            "requirement_id": req_id,
            "section_path": section_path,
            "anchor_para_id": anchor_para_id,
            "grouped_para_ids": para_ids,
            "group_size": len(group),
            "grouping_rationale": (
                f"Anchor: {anchor_para_id} (strictly_qualifies=true). "
                f"Attached {len(group) - 1} supporting paragraph(s) from same section "
                f"with attachable roles or confidence ≥ {SUPPORT_CONFIDENCE_THRESHOLD}."
            ),
        }

        return requirement, group_log


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def run_pipeline():
    doc_id      = stage2a["metadata"]["doc_id"]
    source_file = stage2a["metadata"]["source_file"]
    all_candidates: List[Dict] = stage2a["requirement_candidates"]

    print(f"\n Candidates loaded: {len(all_candidates)}")

    # -- Step 1: Group candidates by section_path, preserving input order -----
    print("\n Step 1: Grouping candidates by section_path...")
    section_order: List[str] = []
    by_section: Dict[str, List[Dict]] = defaultdict(list)
    for c in all_candidates:
        sp = c.get("section_path", "UNMAPPED")
        if sp not in by_section:
            section_order.append(sp)
        by_section[sp].append(c)

    print(f"   Unique sections : {len(section_order)}")

    # -- Step 2: Apply grouping rules per section ------------------------------
    print("\n Step 2: Applying grouping rules...")
    grouper = SectionGrouper()
    all_groups:  List[Tuple[str, List[Dict]]] = []  # (section_path, group)
    all_orphans: List[Dict] = []

    for sp in section_order:
        candidates_in_section = by_section[sp]
        groups, orphans = grouper.group_section(sp, candidates_in_section)
        for g in groups:
            all_groups.append((sp, g))
        all_orphans.extend(orphans)

    print(f"   Logical requirement groups : {len(all_groups)}")
    print(f"   Orphan paragraphs          : {len(all_orphans)}")

    # -- Step 3: Assemble structured requirements ------------------------------
    print("\n  Step 3: Assembling structured requirements...")
    assembler = RequirementAssembler()
    requirements: List[Dict] = []
    grouping_log: List[Dict] = []

    for seq, (sp, group) in enumerate(all_groups, start=1):
        req, log = assembler.assemble(group, doc_id, sp, seq)
        requirements.append(req)
        grouping_log.append(log)

    print(f"   Structured requirements assembled : {len(requirements)}")

    # -- Step 4: Build per-paragraph disposition map ---------------------------
    # Every Stage 2A candidate must appear exactly once: grouped or orphaned.
    para_disposition: Dict[str, str] = {}

    # Record grouped paragraphs
    for req, log in zip(requirements, grouping_log):
        req_id = req["requirement_id"]
        for pid in log["grouped_para_ids"]:
            para_disposition[pid] = f"grouped_into: {req_id}"

    # Record orphaned paragraphs
    for orphan in all_orphans:
        pid = orphan["para_id"]
        reason = orphan.get("reason", "unknown")
        para_disposition[pid] = f"orphaned: {reason}"

    # Sanity: flag any candidate_id not accounted for (should never occur)
    all_candidate_ids = [c["para_id"] for c in all_candidates]
    unaccounted = [pid for pid in all_candidate_ids if pid not in para_disposition]
    if unaccounted:
        for pid in unaccounted:
            para_disposition[pid] = "orphaned: unaccounted_by_pipeline"

    # Verify accounting invariant
    grouped_total = sum(
        1 for v in para_disposition.values() if v.startswith("grouped_into:")
    )
    orphaned_total = sum(
        1 for v in para_disposition.values() if v.startswith("orphaned:")
    )
    accounting_ok = (
        len(all_candidates) == len(para_disposition) == grouped_total + orphaned_total
    )

    # -- Step 5: Build confidence distribution summary -------------------------
    conf_dist: Dict[str, int] = defaultdict(int)
    for r in requirements:
        grade = (r.get("confidence") or {}).get("grade", "UNKNOWN")
        conf_dist[grade] += 1

    # -- Step 6: Build audit ---------------------------------------------------
    print("\n Step 6: Building outputs...")

    audit: Dict[str, Any] = {
        "audit_metadata": {
            "doc_id": doc_id,
            "source_file": source_file,
            "stage": "Stage 2B - Structured Requirement Assembly",
            "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "input_summary": {
            "total_paragraph_candidates_read": len(all_candidates),
            "unique_sections": len(section_order),
        },
        "output_summary": {
            "structured_requirements_produced": len(requirements),
            "orphan_paragraphs": len(all_orphans),
            "confidence_distribution": dict(conf_dist),
        },
        "grouping_log": grouping_log,
        "orphan_paragraphs": all_orphans,
        "paragraph_disposition": para_disposition,
        "accounting_check": {
            "total_candidates": len(all_candidates),
            "disposition_entries": len(para_disposition),
            "grouped": grouped_total,
            "orphaned": orphaned_total,
            "invariant_holds": accounting_ok,
        },
    }

    # -- Save ------------------------------------------------------------------
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(OUTPUT_GROUPED, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "metadata": {
                    "doc_id": doc_id,
                    "source_file": source_file,
                    "stage": "Stage 2B - Structured Requirement Assembly",
                    "assembly_timestamp": datetime.now(timezone.utc).isoformat(),
                    "total_requirements": len(requirements),
                    "confidence_distribution": dict(conf_dist),
                },
                "requirements": requirements,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    with open(OUTPUT_AUDIT, "w", encoding="utf-8") as fh:
        json.dump(audit, fh, ensure_ascii=False, indent=2)

    # -- Summary ---------------------------------------------------------------
    print(f"\n{'='*60}")
    print("[OK]  STAGE 2B COMPLETE")
    print(f"{'='*60}")
    print(f"Paragraph candidates read          : {len(all_candidates)}")
    print(f"Structured requirements assembled  : {len(requirements)}")
    print(f"  HIGH confidence                  : {conf_dist.get('HIGH', 0)}")
    print(f"  MEDIUM confidence                : {conf_dist.get('MEDIUM', 0)}")
    print(f"  LOW confidence                   : {conf_dist.get('LOW', 0)}")
    print(f"Orphan paragraphs                  : {len(all_orphans)}")
    print(f"\nOutputs:")
    print(f"  {OUTPUT_GROUPED}")
    print(f"  {OUTPUT_AUDIT}")
    print(f"{'='*60}")

    return requirements, audit


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    run_pipeline()