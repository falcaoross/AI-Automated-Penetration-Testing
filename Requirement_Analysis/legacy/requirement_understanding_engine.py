"""
Stage 2A – Requirement Paragraph Detection
==========================================
Single-purpose, deterministic, paragraph-level requirement candidate detection.

Reads Stage 1 output (structured_output.json) and produces ONLY:
  requirements_extracted.json  – one entry per qualifying paragraph candidate

NON-NEGOTIABLE PRINCIPLES:
  - Never group multiple paragraphs into one requirement
  - Never infer page numbers, section hierarchy, or paragraph boundaries
  - Never rewrite requirement text
  - Never construct FR/REQ-style structured requirement objects
  - All outputs are auditable and reversible

Strict qualification rule for a requirement candidate paragraph:
  1. Contains a TRUE normative modal verb: shall | must | will
  2. AND describes explicit SYSTEM behavior (system/app/service performs an action)

If a section contains ZERO paragraphs meeting this rule, its classification
is downgraded to SUPPORTING_CONTEXT.

Author: Autopilot-QA Stage 2A
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

INPUT_PATH   = "../Document_Parsing/output/structured_output.json"
OUTPUT_DIR   = "../Requirement_Analysis/output"
OUTPUT_REQS  = os.path.join(OUTPUT_DIR, "requirements_extracted.json")
OUTPUT_AUDIT = os.path.join(OUTPUT_DIR, "requirements_extraction_audit.json")

# Minimum paragraph character length to process (shorter → skipped as noise)
MIN_PARA_LENGTH = 15

# Confidence thresholds (paragraph-level only)
GRADE_HIGH   = 0.75
GRADE_MEDIUM = 0.45

# ===========================================================================
# BOOTSTRAP
# ===========================================================================

if not os.path.exists(INPUT_PATH):
    raise FileNotFoundError(
        f"❌ Stage 1 output not found at: {INPUT_PATH}\n"
        "Run document_ingestion_engine.py first."
    )

with open(INPUT_PATH, "r", encoding="utf-8") as fh:
    stage1: Dict[str, Any] = json.load(fh)

print("📄 Loaded structured_output.json")
print(f"   doc_id : {stage1['doc_metadata']['doc_id']}")
print(f"   source : {stage1['doc_metadata']['source_file']}")

# ===========================================================================
# CONSTANTS – signal sets (domain-agnostic, linguistic only)
# ===========================================================================

# TRUE normative modal verbs only (shall / must / will)
# "should", "required to", "needs to" are NOT normative for qualification purposes
TRUE_NORMATIVE_MODALS: Tuple[str, ...] = (
    r"\bshall\b",
    r"\bmust\b",
    r"\bwill\b",
)

# All modal-adjacent signals (used only for section classification density check,
# NOT for paragraph qualification)
MODAL_SIGNALS: Tuple[str, ...] = (
    r"\bshall\b", r"\bmust\b", r"\bwill\b", r"\bshould\b", r"\brequired to\b",
    r"\bis required\b", r"\bare required\b", r"\bneeds? to\b",
)

# System-behaviour verb patterns (subject-agnostic)
SYSTEM_BEHAVIOR_SIGNALS: Tuple[str, ...] = (
    r"\bvalidates?\b", r"\bauthenticates?\b", r"\bstores?\b", r"\binserts?\b",
    r"\bupdates?\b", r"\bdeletes?\b", r"\bredirects?\b", r"\bdisplays?\b",
    r"\breturns?\b", r"\bgenerates?\b", r"\bprocesses?\b", r"\bverifies?\b",
    r"\bcreates?\b", r"\bhandles?\b", r"\bsends?\b", r"\brejects?\b",
    r"\bencrypts?\b", r"\bhashes?\b", r"\bcommits?\b", r"\bqueries?\b",
    r"\blogs?\b", r"\bcaches?\b", r"\bnotifies?\b", r"\bprovides?\b",
    r"\baccepts?\b", r"\btriggers?\b", r"\binitiates?\b", r"\bterminates?\b",
    r"\ballows?\b", r"\benforces?\b", r"\bestablishes?\b", r"\bmaintains?\b",
    r"\bperforms?\b", r"\bexecutes?\b", r"\bapplies?\b", r"\bchecks?\b",
    r"\bcomputes?\b", r"\bresponds?\b", r"\bparses?\b", r"\bloads?\b",
    r"\bsupports?\b", r"\bexposes?\b", r"\brestricts?\b", r"\bpersists?\b",
)

# Input-definition signals
INPUT_SIGNALS: Tuple[str, ...] = (
    r"\binputs?\b", r"\baccepts?\b", r"\breceives?\b", r"\btakes?\b",
    r"\bprovided by\b", r"\bsupplied by\b", r"\bentered by\b",
    r"\bparameters?\b", r"\bfield[s]?\b", r"\bform[s]?\b",
)

# Output-definition signals
OUTPUT_SIGNALS: Tuple[str, ...] = (
    r"\boutputs?\b", r"\breturns?\b", r"\bdisplays?\b", r"\bshows?\b",
    r"\bgenerates?\b", r"\bproduces?\b", r"\bprovides?\b", r"\bpresents?\b",
    r"\bresponds? with\b", r"\bemits?\b",
)

# Constraint / conditional signals
CONSTRAINT_SIGNALS: Tuple[str, ...] = (
    r"\bonly if\b", r"\bif and only if\b", r"\bwhen\b", r"\bupon\b",
    r"\bunless\b", r"\bexcept\b", r"\bnot exceed\b", r"\bmust not\b",
    r"\bshall not\b", r"\bwithin\b", r"\bat most\b", r"\bat least\b",
    r"\bno more than\b", r"\bno fewer than\b", r"\blimited to\b",
    r"\bprovided that\b", r"\bsubject to\b",
)

# Sections whose names strongly suggest non-requirement content
NON_REQ_SECTION_SIGNALS: Tuple[str, ...] = (
    "introduction", "purpose", "scope", "overview", "background",
    "references", "definitions", "acronyms", "abbreviations",
    "document overview", "revision history", "table of contents",
    "assumptions", "dependencies", "glossary", "appendix",
    "acknowledgements", "preface",
)

# Sections that are typically context / informational (not normative)
CONTEXT_SECTION_SIGNALS: Tuple[str, ...] = (
    "description", "perspective", "functions", "characteristics",
    "environment", "constraints", "design", "workflow", "flow",
    "diagram", "explanation", "use case", "actors", "process",
    "data store", "entity", "component", "interaction",
)


# ===========================================================================
# UTILITIES
# ===========================================================================

def _matches_any(text: str, patterns: Tuple[str, ...]) -> List[str]:
    """Return list of pattern strings that match (case-insensitive)."""
    text_lower = text.lower()
    return [p for p in patterns if re.search(p, text_lower)]


def _has_true_normative_modal(text: str) -> bool:
    """Return True iff text contains at least one TRUE normative modal (shall/must/will)."""
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in TRUE_NORMATIVE_MODALS)


def _has_system_behavior(text: str) -> bool:
    """Return True iff text contains at least one system-behavior verb signal."""
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in SYSTEM_BEHAVIOR_SIGNALS)


def _paragraph_qualifies_as_candidate(text: str) -> bool:
    """
    Strict qualification rule:
      1. Contains a true normative modal (shall | must | will)
      2. AND describes explicit SYSTEM behavior
    Both conditions must hold.
    """
    return _has_true_normative_modal(text) and _has_system_behavior(text)


def _deterministic_para_candidate_id(doc_id: str, para_id: str) -> str:
    """
    Deterministic paragraph-level candidate ID.
    Same inputs → same ID across re-runs.
    This is NOT a final requirement ID; Stage 2B will assign those.
    """
    raw = f"{doc_id}|{para_id}"
    digest = hashlib.sha1(raw.encode()).hexdigest()[:10]
    return f"PC-{digest}"


# ===========================================================================
# STEP 1 – SECTION CLASSIFIER
# ===========================================================================

class SectionClassifier:
    """
    Classifies each unique section_path as:
      REQUIREMENT_SOURCE  – contains at least one paragraph that meets the
                            strict qualification rule (true normative modal +
                            system behavior)
      SUPPORTING_CONTEXT  – informational / descriptive; may have modal verbs
                            but ZERO paragraphs meet the strict rule
      NON_REQUIREMENT     – not a source of testable requirements

    Classification is determined AFTER paragraph-level qualification,
    so that sections with zero qualifying paragraphs are correctly
    downgraded to SUPPORTING_CONTEXT.
    """

    def classify_all(
        self,
        paragraphs: List[Dict],
    ) -> Dict[str, Dict]:
        """
        Returns a dict keyed by section_path with classification metadata.
        Two-pass:
          Pass 1: name-based hard rules (NON_REQUIREMENT / skip / unmapped)
          Pass 2: paragraph-level strict qualification to distinguish
                  REQUIREMENT_SOURCE from SUPPORTING_CONTEXT
        """
        by_section: Dict[str, List[Dict]] = defaultdict(list)
        for p in paragraphs:
            sp = p.get("section_path") or "UNMAPPED"
            by_section[sp].append(p)

        results = {}
        for section_path, paras in by_section.items():
            results[section_path] = self._classify_section(section_path, paras)

        return results

    def _classify_section(
        self, section_path: str, paras: List[Dict]
    ) -> Dict[str, Any]:
        sp_lower = section_path.lower()
        para_count = len(paras)

        # --- Hard non-requirement signals in section name ---
        non_req_matches = [s for s in NON_REQ_SECTION_SIGNALS if s in sp_lower]
        if non_req_matches:
            return {
                "classification": "NON_REQUIREMENT",
                "rationale": f"Section name matches non-requirement signals: {non_req_matches}",
                "paragraph_count": para_count,
                "qualifying_paragraph_count": 0,
            }

        # --- Skip special tokens ---
        if "__SKIP__" in section_path or section_path == "UNMAPPED":
            return {
                "classification": "NON_REQUIREMENT",
                "rationale": "Section is skipped or unmapped by Stage 1",
                "paragraph_count": para_count,
                "qualifying_paragraph_count": 0,
            }

        # --- Count paragraphs that meet the STRICT qualification rule ---
        content_paras = [
            p for p in paras
            if not p.get("is_heading") and len(p.get("text", "")) >= MIN_PARA_LENGTH
        ]

        qualifying_count = 0
        for p in content_paras:
            text = p.get("text", "")
            if _paragraph_qualifies_as_candidate(text):
                qualifying_count += 1

        # If at least one paragraph qualifies → REQUIREMENT_SOURCE
        if qualifying_count > 0:
            return {
                "classification": "REQUIREMENT_SOURCE",
                "rationale": (
                    f"{qualifying_count}/{len(content_paras)} content paragraph(s) "
                    "contain a true normative modal (shall/must/will) AND system behavior signal."
                ),
                "paragraph_count": para_count,
                "qualifying_paragraph_count": qualifying_count,
            }

        # Zero qualifying paragraphs.
        # Check if context signals appear in section name → SUPPORTING_CONTEXT
        context_matches = [s for s in CONTEXT_SECTION_SIGNALS if s in sp_lower]

        # Also check if any modal-adjacent signals appear at all (soft normative)
        modal_para_count = sum(
            1 for p in content_paras
            if _matches_any(p.get("text", ""), MODAL_SIGNALS)
        )

        if context_matches or modal_para_count > 0:
            return {
                "classification": "SUPPORTING_CONTEXT",
                "rationale": (
                    f"Section has ZERO paragraphs meeting strict qualification rule. "
                    f"Context signals in name: {context_matches}; "
                    f"modal-adjacent paragraphs: {modal_para_count}."
                ),
                "paragraph_count": para_count,
                "qualifying_paragraph_count": 0,
            }

        # Default
        return {
            "classification": "NON_REQUIREMENT",
            "rationale": "No qualifying paragraphs and no context signals detected.",
            "paragraph_count": para_count,
            "qualifying_paragraph_count": 0,
        }


# ===========================================================================
# STEP 2 – PARAGRAPH ROLE TAGGER
# ===========================================================================

class ParagraphRoleTagger:
    """
    Tags each paragraph with one or more semantic roles:
      requirement_statement – normative clause with a true modal verb (shall/must/will)
      system_behavior       – system action/response description
      input_definition      – description of inputs / parameters
      output_definition     – description of outputs / responses
      constraint            – conditional or limiting clause
      narrative             – purely descriptive / explanatory

    Multiple roles per paragraph are allowed.
    """

    def tag(
        self, para: Dict
    ) -> Tuple[List[str], Dict[str, List[str]], Optional[str]]:
        """
        Returns:
          roles          – list of assigned roles
          signal_log     – {role: [matched_signal_patterns]}
          skip_reason    – None if processed, string if skipped
        """
        text = para.get("text", "").strip()

        # Skip headings
        if para.get("is_heading"):
            return [], {}, "paragraph_is_heading"

        # Skip very short / noise paragraphs
        if len(text) < MIN_PARA_LENGTH:
            return [], {}, f"too_short ({len(text)} chars)"

        roles: List[str] = []
        signal_log: Dict[str, List[str]] = {}

        # requirement_statement – TRUE normative modals only
        modal_hits = _matches_any(text, TRUE_NORMATIVE_MODALS)
        if modal_hits:
            roles.append("requirement_statement")
            signal_log["requirement_statement"] = modal_hits

        # system_behavior
        behavior_hits = _matches_any(text, SYSTEM_BEHAVIOR_SIGNALS)
        if behavior_hits:
            roles.append("system_behavior")
            signal_log["system_behavior"] = behavior_hits[:5]

        # input_definition
        input_hits = _matches_any(text, INPUT_SIGNALS)
        if input_hits:
            roles.append("input_definition")
            signal_log["input_definition"] = input_hits

        # output_definition
        output_hits = _matches_any(text, OUTPUT_SIGNALS)
        if output_hits:
            roles.append("output_definition")
            signal_log["output_definition"] = output_hits

        # constraint
        constraint_hits = _matches_any(text, CONSTRAINT_SIGNALS)
        if constraint_hits:
            roles.append("constraint")
            signal_log["constraint"] = constraint_hits

        # narrative – fallback
        if not roles:
            roles.append("narrative")
            signal_log["narrative"] = ["no_normative_signals_detected"]

        return roles, signal_log, None


# ===========================================================================
# STEP 3 – PARAGRAPH-LEVEL CANDIDATE EMITTER
# ===========================================================================

class ParagraphCandidateEmitter:
    """
    Emits one candidate entry per paragraph that qualifies as a requirement
    candidate under the STRICT rule:
      - true normative modal (shall | must | will)
      - AND system behavior signal

    Additionally, paragraphs in REQUIREMENT_SOURCE sections that carry
    non-narrative roles (input_definition, output_definition, constraint,
    system_behavior) are emitted as SUPPORTING_CONTEXT candidates to give
    Stage 2B access to all relevant paragraph-level data.

    NON_REQUIREMENT section paragraphs are NOT emitted as candidates.

    One paragraph = one entry. No grouping. No merging.
    """

    def emit(
        self,
        paragraphs: List[Dict],
        section_classifications: Dict[str, Dict],
        tag_results: Dict[str, Tuple[List[str], Dict, Optional[str]]],
        doc_id: str,
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Returns:
          candidates – list of paragraph-level candidate dicts
          skip_log   – paragraphs skipped with reason
        """
        candidates: List[Dict] = []
        skip_log: List[Dict] = []

        eligible = [
            p for p in paragraphs
            if p.get("page_type") != "toc"
            and not p.get("is_heading")
            and p.get("section_path")
            and p["section_path"] != "__SKIP__TOC__"
        ]

        for p in eligible:
            para_id = p["para_id"]
            section_path = p.get("section_path", "UNMAPPED")
            cls_info = section_classifications.get(section_path, {})
            section_cls = cls_info.get("classification", "NON_REQUIREMENT")

            tag_result = tag_results.get(para_id)
            if tag_result is None:
                skip_log.append({
                    "para_id": para_id,
                    "section_path": section_path,
                    "reason": "not_in_tag_results",
                })
                continue

            roles, signal_log, skip_reason = tag_result

            if skip_reason:
                skip_log.append({
                    "para_id": para_id,
                    "section_path": section_path,
                    "reason": skip_reason,
                })
                continue

            text = p.get("text", "")

            # Determine whether this paragraph meets the strict qualification rule
            strictly_qualifies = _paragraph_qualifies_as_candidate(text)

            # Emit only if:
            #   (a) paragraph strictly qualifies, OR
            #   (b) paragraph is in a REQUIREMENT_SOURCE section and has
            #       at least one non-narrative role (provides supporting detail
            #       to Stage 2B for grouping purposes)
            non_narrative_roles = [r for r in roles if r != "narrative"]
            is_in_req_source = section_cls == "REQUIREMENT_SOURCE"

            if not strictly_qualifies and not (is_in_req_source and non_narrative_roles):
                skip_log.append({
                    "para_id": para_id,
                    "section_path": section_path,
                    "reason": "does_not_qualify_strict_rule_and_not_supporting_in_req_source",
                })
                continue

            # Build detection rationale
            if strictly_qualifies:
                modal_hits = _matches_any(text, TRUE_NORMATIVE_MODALS)
                behavior_hits = _matches_any(text, SYSTEM_BEHAVIOR_SIGNALS)
                detection_rationale = (
                    f"Paragraph meets strict qualification rule. "
                    f"True normative modals matched: {modal_hits}. "
                    f"System behavior signals matched: {behavior_hits[:5]}."
                )
            else:
                detection_rationale = (
                    f"Paragraph does not meet strict qualification rule but resides in "
                    f"REQUIREMENT_SOURCE section '{section_path}' and carries "
                    f"non-narrative roles {non_narrative_roles}. "
                    f"Emitted as supporting detail for Stage 2B."
                )

            # Paragraph-level confidence
            confidence = self._score_paragraph(
                strictly_qualifies=strictly_qualifies,
                roles=roles,
                section_cls=section_cls,
                text=text,
            )

            candidate_id = _deterministic_para_candidate_id(doc_id, para_id)

            candidates.append({
                "candidate_id": candidate_id,
                "doc_id": doc_id,
                "para_id": para_id,
                "page": p.get("page"),
                "section_path": section_path,
                "section_classification": section_cls,
                "raw_text": text,
                "paragraph_roles": roles,
                "paragraph_signals": signal_log,
                "strictly_qualifies": strictly_qualifies,
                "detection_rationale": detection_rationale,
                "confidence": confidence,
            })

        return candidates, skip_log

    def _score_paragraph(
        self,
        strictly_qualifies: bool,
        roles: List[str],
        section_cls: str,
        text: str,
    ) -> Dict[str, Any]:
        """
        Paragraph-level confidence score.
        Components:
          normative_modal  (0.40) – true normative modal present
          system_behavior  (0.30) – system behavior signal present
          section_source   (0.20) – section is REQUIREMENT_SOURCE
          role_diversity   (0.10) – multiple non-narrative roles
        """
        score = 0.0
        breakdown: Dict[str, float] = {}

        # Normative modal
        modal_score = 0.40 if _has_true_normative_modal(text) else 0.0
        breakdown["normative_modal"] = modal_score
        score += modal_score

        # System behavior
        behavior_score = 0.30 if _has_system_behavior(text) else 0.0
        breakdown["system_behavior"] = behavior_score
        score += behavior_score

        # Section classification
        section_score = 0.20 if section_cls == "REQUIREMENT_SOURCE" else 0.05
        breakdown["section_source"] = section_score
        score += section_score

        # Role diversity
        non_narrative = [r for r in roles if r != "narrative"]
        role_score = min(len(non_narrative) * 0.025, 0.10)
        breakdown["role_diversity"] = round(role_score, 4)
        score += role_score

        final_score = round(min(max(score, 0.01), 0.99), 4)

        if final_score >= GRADE_HIGH:
            grade = "HIGH"
        elif final_score >= GRADE_MEDIUM:
            grade = "MEDIUM"
        else:
            grade = "LOW"

        return {
            "score": final_score,
            "grade": grade,
            "breakdown": breakdown,
        }


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def run_pipeline():
    doc_metadata = stage1["doc_metadata"]
    doc_id = doc_metadata["doc_id"]
    all_paragraphs: List[Dict] = stage1["paragraphs"]
    ocr_entries: List[Dict] = stage1.get("ocr_text", [])
    ocr_pages: set = {entry["page"] for entry in ocr_entries}

    print(f"\n📊 Paragraph inventory: {len(all_paragraphs)} total")
    print(f"   OCR pages: {sorted(ocr_pages) if ocr_pages else 'none'}")

    # ── Step 1: Section Classification ──────────────────────────────────────
    print("\n🔎 Step 1: Classifying sections...")
    classifier = SectionClassifier()
    section_classifications = classifier.classify_all(all_paragraphs)

    req_src = sum(
        1 for v in section_classifications.values()
        if v["classification"] == "REQUIREMENT_SOURCE"
    )
    ctx = sum(
        1 for v in section_classifications.values()
        if v["classification"] == "SUPPORTING_CONTEXT"
    )
    non_req = sum(
        1 for v in section_classifications.values()
        if v["classification"] == "NON_REQUIREMENT"
    )
    print(f"   REQUIREMENT_SOURCE : {req_src}")
    print(f"   SUPPORTING_CONTEXT : {ctx}")
    print(f"   NON_REQUIREMENT    : {non_req}")

    # ── Step 2: Paragraph Role Tagging ──────────────────────────────────────
    print("\n🏷️  Step 2: Tagging paragraph roles...")
    tagger = ParagraphRoleTagger()
    tag_results: Dict[str, Tuple] = {}

    for para in all_paragraphs:
        roles, signal_log, skip_reason = tagger.tag(para)
        tag_results[para["para_id"]] = (roles, signal_log, skip_reason)

    tagged = sum(1 for v in tag_results.values() if v[2] is None)
    print(f"   Tagged for processing : {tagged}/{len(all_paragraphs)}")

    # ── Step 3: Paragraph Candidate Emission ────────────────────────────────
    print("\n📦 Step 3: Emitting paragraph-level requirement candidates...")
    emitter = ParagraphCandidateEmitter()
    candidates, skip_log = emitter.emit(
        all_paragraphs, section_classifications, tag_results, doc_id
    )

    strictly_qualified = sum(1 for c in candidates if c["strictly_qualifies"])
    supporting_emitted = sum(1 for c in candidates if not c["strictly_qualifies"])
    print(f"   Strictly qualified candidates : {strictly_qualified}")
    print(f"   Supporting detail candidates  : {supporting_emitted}")
    print(f"   Total candidates emitted      : {len(candidates)}")
    print(f"   Paragraphs skipped            : {len(skip_log)}")

    # ── Step 4: Confidence distribution summary ──────────────────────────────
    conf_dist: Dict[str, int] = defaultdict(int)
    for c in candidates:
        grade = (c.get("confidence") or {}).get("grade", "UNKNOWN")
        conf_dist[grade] += 1

    # ── Step 5: Build audit ──────────────────────────────────────────────────
    print("\n📝 Step 5: Building outputs...")

    # Section classification summary
    cls_summary: Dict[str, int] = defaultdict(int)
    for info in section_classifications.values():
        cls_summary[info["classification"]] += 1

    # Skip reason summary
    skip_reasons: Dict[str, int] = defaultdict(int)
    bullet_artifacts = 0
    for s in skip_log:
        reason = s["reason"]
        if reason.startswith("too_short ("):
            bullet_artifacts += 1
        else:
            skip_reasons[reason] += 1
    if bullet_artifacts:
        skip_reasons["bullet_artifacts_skipped"] = bullet_artifacts

    audit = {
        "audit_metadata": {
            "doc_id": doc_id,
            "source_file": doc_metadata["source_file"],
            "stage": "Stage 2A – Requirement Paragraph Detection",
            "audit_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "paragraph_summary": {
            "total_paragraphs": len(all_paragraphs),
            "heading_paragraphs": sum(1 for p in all_paragraphs if p.get("is_heading")),
            "toc_paragraphs": sum(1 for p in all_paragraphs if p.get("page_type") == "toc"),
            "ocr_pages": sorted(ocr_pages),
            "skipped_paragraphs": len(skip_log),
        },
        "section_classification_summary": dict(cls_summary),
        "section_classifications": {
            sp: {
                "classification": info["classification"],
                "rationale": info["rationale"],
                "paragraph_count": info["paragraph_count"],
                "qualifying_paragraph_count": info.get("qualifying_paragraph_count", 0),
            }
            for sp, info in section_classifications.items()
        },
        "extraction_summary": {
            "total_candidates_emitted": len(candidates),
            "strictly_qualified": strictly_qualified,
            "supporting_detail": supporting_emitted,
        },
        "confidence_distribution": dict(conf_dist),
        "skip_log": skip_log,
        "skip_reason_summary": dict(skip_reasons),
    }

    # ── Save ─────────────────────────────────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Strip internal signals field from output (kept in audit context only)
    output_candidates = []
    for c in candidates:
        entry = {k: v for k, v in c.items() if k != "paragraph_signals"}
        output_candidates.append(entry)

    with open(OUTPUT_REQS, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "metadata": {
                    "doc_id": doc_id,
                    "source_file": doc_metadata["source_file"],
                    "stage": "Stage 2A – Requirement Paragraph Detection",
                    "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
                    "total_candidates": len(output_candidates),
                    "strictly_qualified": strictly_qualified,
                    "confidence_distribution": dict(conf_dist),
                },
                "requirement_candidates": output_candidates,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    with open(OUTPUT_AUDIT, "w", encoding="utf-8") as fh:
        json.dump(audit, fh, ensure_ascii=False, indent=2)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("✅  STAGE 2A COMPLETE")
    print(f"{'='*60}")
    print(f"Paragraph candidates emitted   : {len(candidates)}")
    print(f"  Strictly qualified (req rule) : {strictly_qualified}")
    print(f"  Supporting detail             : {supporting_emitted}")
    print(f"  HIGH confidence               : {conf_dist.get('HIGH', 0)}")
    print(f"  MEDIUM confidence             : {conf_dist.get('MEDIUM', 0)}")
    print(f"  LOW confidence                : {conf_dist.get('LOW', 0)}")
    print(f"\nOutputs:")
    print(f"  {OUTPUT_REQS}")
    print(f"  {OUTPUT_AUDIT}")
    print(f"{'='*60}")

    return output_candidates, audit


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    run_pipeline()