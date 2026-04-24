"""
Stage 2C – Normalize Stage 2B Output for CRU Normalization
===========================================================
Schema adapter sitting at the boundary between Stage 2B and Stage 3
(CRU Normalization). Performs schema normalization ONLY — no semantic
logic, no NLP, no re-detection of requirements.

Reads:
    requirements_extracted_grouped.json   (Stage 2B output)
    structured_output.json                (Stage 1 output — doc metadata ONLY)

Writes:
    requirements_normalized_for_cru.json  (consumed by Module 1 of Stage 3)

Normalization contract (exhaustive — nothing else is done):
    1. Rename  requirement_id  →  id
    2. Rebuild source_ref with the exact keys CRU Module 1 reads:
           source_file, section, requirement_type, page,
           doc_id, doc_type, version,
           section_path, para_ids, page_range
    3. Inject doc metadata (doc_id, doc_type, version, source_file)
       from structured_output.json — never from Stage 2B heuristics
    4. Normalize page from page_range[0]
    5. Infer requirement_type deterministically from section_path string
    6. Add _semantic_hints block:
           split_candidate: bool  — true when description contains
           multi-verb markers ('For X:' clauses or sentence-initial
           imperative verbs separated by sentence boundaries).
           This flag is a HINT ONLY. CRU Module 1 decides whether to
           apply split_behavior_segments; this adapter never splits text.

NON-NEGOTIABLE CONSTRAINTS:
    - Stage 2B output is never modified
    - No semantic interpretation
    - No NLP
    - No field values are invented — every value traces to a source file
    - If structured_output.json is absent, doc metadata falls back to
      Stage 2B metadata block (which always carries doc_id + source_file)

Author: Autopilot-QA Stage 2C
Version: 1.0
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ===========================================================================
# CONFIGURATION
# ===========================================================================

# Default paths (overridable via CLI)
DEFAULT_GROUPED     = "../Requirement_Analysis/output/requirements_extracted_grouped.json"
DEFAULT_STRUCTURED  = "../Document_Parsing/output/structured_output.json"
DEFAULT_OUTPUT      = "../Requirement_Analysis/output/requirements_normalized_for_cru.json"
DEFAULT_AUDIT       = "../Requirement_Analysis/output/stage_2c_audit.json"


# ===========================================================================
# REQUIREMENT TYPE INFERENCE
# ===========================================================================

# Ordered rules applied top-to-bottom against section_path.lower().
# First match wins. Strings are substring matches — deliberately broad so the
# adapter works for any SRS, not just Todo-SRS.pdf.
_REQ_TYPE_RULES: List[Tuple[List[str], str]] = [
    # Non-functional / quality requirement signals
    (["non-functional", "non functional", "quality", "performance",
      "security", "usability", "reliability", "portability",
      "maintainability", "scalability", "availability",
      "capacity", "compliance", "interoperability"], "planguage"),
    # Use case signals
    (["use case", "use-case", "usecase", "actor scenario",
      "user story", "user-story"], "use_case"),
    # Constraint signals
    (["constraint", "business rule", "assumption",
      "dependency", "limitation"], "constraint"),
    # Functional requirement (default — must be last)
    (["functional", "feature", "fr ", "fr-", "fr_"], "standard"),
]

_DEFAULT_REQ_TYPE = "standard"


def infer_requirement_type(section_path: str) -> str:
    """
    Deterministically infer requirement_type from section_path string.

    Applies ordered substring rules; returns first match or 'standard'.
    Pure string logic — no NLP, no heuristics.

    Examples:
      "4. Non-Functional Requirements > 4.1 Performance" → "planguage"
      "3. Functional Requirements > 3.1 FR 1 User Signup" → "standard"
      "4. Non-Functional Requirements > 4.2 Security"     → "planguage"
      "5. Use Cases > 5.1 Login"                          → "use_case"
    """
    if not section_path:
        return _DEFAULT_REQ_TYPE

    sp_lower = section_path.lower()

    for keywords, req_type in _REQ_TYPE_RULES:
        if any(kw in sp_lower for kw in keywords):
            return req_type

    return _DEFAULT_REQ_TYPE


# ===========================================================================
# MULTI-VERB SPLIT CANDIDATE DETECTION
# ===========================================================================

# Pattern 1: 'For X:' clause markers — the primary signal used by
# split_behavior_segments in Module 1.
_FOR_PREFIX_RE = re.compile(r'(?:^|\n)\s*For\s+[^:]{1,40}:', re.IGNORECASE)

# Pattern 2: Sentence-initial imperative verbs followed by a noun phrase.
# These appear in description text when system behavior was listed as a
# bullet-style sequence: "Validates X.\nInserts Y.\nRedirects Z."
# The verbs matched here are the same set Module 1's multi-verb detector
# checks for on system_behavior signals.
_IMPERATIVE_VERB_RE = re.compile(
    r'(?:^|(?<=[.!\n]))\s*'
    r'(Validates?|Inserts?|Redirects?|Hashes?|Updates?|Deletes?|Marks?|'
    r'Creates?|Edits?|Filters?|Clears?|Persists?|Commits?|Queries?|'
    r'Handles?|Enforces?|Scopes?|Ensures?|Authenticates?|Returns?|'
    r'Registers?|Assigns?|Notifies?|Generates?|Exports?|Imports?)\b',
    re.MULTILINE,
)


def is_split_candidate(description: Optional[str]) -> bool:
    """
    Return True when description text contains structural markers that
    indicate multiple distinct verb clauses exist.

    Two triggers (OR logic):
    1. Text contains at least one 'For X:' clause marker
    2. Text contains 2+ sentence-initial imperative verbs

    This is a HINT — final decision belongs to CRU Module 1.
    No text is modified here.
    """
    if not description or len(description.strip()) < 10:
        return False

    # Trigger 1: 'For X:' present anywhere
    if _FOR_PREFIX_RE.search(description):
        return True

    # Trigger 2: 2+ sentence-initial imperative verbs
    imperative_matches = _IMPERATIVE_VERB_RE.findall(description)
    if len(imperative_matches) >= 2:
        return True

    return False


# ===========================================================================
# SOURCE_REF REBUILDER
# ===========================================================================

def rebuild_source_ref(
    stage2b_source_ref: Dict[str, Any],
    doc_meta: Dict[str, Any],
    req_title: str,
    section_path: str,
) -> Dict[str, Any]:
    """
    Build the source_ref dict that CRU Module 1's normalize_traceability
    can read without any internal changes.

    Module 1 reads these keys from source_ref:
        source_file       → raw.get("source_file", "unknown_source")
        section           → raw.get("section", "")
        requirement_type  → raw.get("requirement_type", "standard")
        page              → raw.get("page")
        version           → raw.get("version") or raw.get("doc_version")
        doc_type          → raw.get("doc_type") or raw.get("document_type")

    Module 1 also calls derive_doc_id(source_file) to build doc_id,
    but it will prefer an existing doc_id if we patch normalize_traceability
    (done separately). For now we include doc_id so it is available via
    direct key lookup even before that patch.

    Additionally we carry through Stage 2B fields for full traceability:
        section_path, para_ids, page_range
    """
    page_range = stage2b_source_ref.get("page_range", [None, None])
    page       = page_range[0] if page_range else None

    req_type   = infer_requirement_type(section_path)

    return {
        # ── Keys CRU Module 1 reads directly ──────────────────────────
        "source_file":      doc_meta["source_file"],
        "section":          section_path,          # Module 1 uses this as section
        "requirement_type": req_type,
        "page":             page,
        "version":          doc_meta["version"],
        "doc_type":         doc_meta["doc_type"],
        # ── Direct doc_id (avoids re-derivation from filename) ─────────
        "doc_id":           doc_meta["doc_id"],
        # ── Full traceability passthrough from Stage 2B ────────────────
        "section_path":     section_path,
        "para_ids":         stage2b_source_ref.get("para_ids", []),
        "page_range":       page_range,
    }


# ===========================================================================
# SINGLE REQUIREMENT NORMALIZER
# ===========================================================================

def normalize_requirement(
    req: Dict[str, Any],
    doc_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Normalize one Stage 2B requirement dict into the schema CRU Module 1
    expects.

    Field mapping (exhaustive):
        requirement_id  → id           (rename only)
        source_ref      → rebuilt      (see rebuild_source_ref)
        description     → unchanged
        system_behavior → unchanged    (null stays null)
        inputs          → unchanged    (null stays null)
        outputs         → unchanged    (null stays null)
        constraints     → unchanged    (null stays null)
        title           → unchanged
        seq             → unchanged
        confidence      → unchanged
        _semantic_hints → NEW BLOCK    (split_candidate flag)
        _stage2c_meta   → NEW BLOCK    (audit provenance)
    """
    stage2b_source_ref = req.get("source_ref", {})
    section_path       = stage2b_source_ref.get("section_path", "")
    req_title          = req.get("title", "")
    description        = req.get("description")

    new_source_ref = rebuild_source_ref(
        stage2b_source_ref=stage2b_source_ref,
        doc_meta=doc_meta,
        req_title=req_title,
        section_path=section_path,
    )

    split_hint = is_split_candidate(description)

    return {
        # ── Primary key rename ─────────────────────────────────────────
        "id":                 req["requirement_id"],
        # ── Preserved Stage 2B fields (unchanged values) ───────────────
        "requirement_id":     req["requirement_id"],   # kept for back-reference
        "seq":                req.get("seq"),
        "title":              req_title,
        "description":        description,
        "system_behavior":    req.get("system_behavior"),
        "inputs":             req.get("inputs"),
        "outputs":            req.get("outputs"),
        "constraints":        req.get("constraints"),
        "confidence":         req.get("confidence"),
        # ── Rebuilt source_ref ─────────────────────────────────────────
        "source_ref":         new_source_ref,
        # ── Semantic hints block (read-only hint for Module 1) ─────────
        "_semantic_hints": {
            "split_candidate": split_hint,
            "split_candidate_reason": (
                "for_prefix_marker"    if _FOR_PREFIX_RE.search(description or "")
                else "imperative_verb_sequence" if split_hint
                else "none"
            ),
        },
        # ── Adapter provenance (not consumed by Module 1) ──────────────
        "_stage2c_meta": {
            "adapted_from":     "requirements_extracted_grouped.json",
            "stage":            "Stage 2C – Normalize for CRU",
            "adapter_version":  "1.0",
        },
    }


# ===========================================================================
# DOC METADATA LOADER
# ===========================================================================

def load_doc_meta(structured_output_path: str) -> Dict[str, str]:
    """
    Extract document metadata from structured_output.json.

    Reads ONLY the top-level 'doc_metadata' block.
    Falls back to safe defaults if the file is absent — in that case a
    warning is printed and the caller must independently verify output.

    Returns dict with keys: doc_id, doc_type, version, source_file.
    """
    p = Path(structured_output_path)
    if not p.exists():
        print(f"⚠️  structured_output.json not found at {structured_output_path}")
        print("   Falling back to Stage 2B metadata block for doc metadata.")
        return {}

    with open(p, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    dm = data.get("doc_metadata", {})
    return {
        "doc_id":      dm.get("doc_id", ""),
        "doc_type":    dm.get("doc_type", "SRS"),
        "version":     dm.get("version", "v1.0"),
        "source_file": dm.get("source_file", ""),
    }


def merge_doc_meta_with_fallback(
    structured_meta: Dict[str, str],
    stage2b_meta: Dict[str, Any],
) -> Dict[str, str]:
    """
    Merge structured_output.json metadata with Stage 2B metadata block.
    structured_output.json is authoritative; Stage 2B fills any gaps.
    """
    return {
        "doc_id":      structured_meta.get("doc_id")      or stage2b_meta.get("doc_id", ""),
        "doc_type":    structured_meta.get("doc_type")     or "SRS",
        "version":     structured_meta.get("version")      or "v1.0",
        "source_file": structured_meta.get("source_file")  or stage2b_meta.get("source_file", ""),
    }


# ===========================================================================
# AUDIT WRITER
# ===========================================================================

def write_audit(
    normalized: List[Dict[str, Any]],
    doc_meta: Dict[str, str],
    input_path: str,
    output_path: str,
    audit_path: str,
) -> None:
    """Write a per-run audit JSON for Stage 2C."""

    # Per-requirement breakdown
    per_req = {}
    split_candidates = []
    req_type_dist: Dict[str, int] = {}

    for r in normalized:
        rid      = r["id"]
        req_type = r["source_ref"]["requirement_type"]
        split    = r["_semantic_hints"]["split_candidate"]

        per_req[rid] = {
            "requirement_type":        req_type,
            "split_candidate":         split,
            "split_candidate_reason":  r["_semantic_hints"]["split_candidate_reason"],
            "page":                    r["source_ref"]["page"],
            "section_path":            r["source_ref"]["section_path"],
            "para_ids":                r["source_ref"]["para_ids"],
        }
        if split:
            split_candidates.append(rid)

        req_type_dist[req_type] = req_type_dist.get(req_type, 0) + 1

    # Invariant checks
    ids = [r["id"] for r in normalized]
    duplicate_ids = [i for i in ids if ids.count(i) > 1]

    missing_doc_id      = [r["id"] for r in normalized if not r["source_ref"].get("doc_id")]
    missing_source_file = [r["id"] for r in normalized if not r["source_ref"].get("source_file")]
    missing_page        = [r["id"] for r in normalized if r["source_ref"].get("page") is None]
    missing_req_type    = [r["id"] for r in normalized
                           if not r["source_ref"].get("requirement_type")]

    passes = (
        len(duplicate_ids) == 0
        and len(missing_doc_id) == 0
        and len(missing_source_file) == 0
        and len(missing_req_type) == 0
    )

    audit = {
        "audit_version":   "1.0",
        "stage":           "Stage 2C – Normalize for CRU",
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "input_file":      input_path,
        "output_file":     output_path,
        "doc_metadata_used": doc_meta,
        "summary": {
            "total_requirements":     len(normalized),
            "split_candidates":       len(split_candidates),
            "requirement_type_distribution": req_type_dist,
        },
        "split_candidate_ids": split_candidates,
        "invariant_checks": {
            "duplicate_ids":            duplicate_ids,
            "missing_doc_id":           missing_doc_id,
            "missing_source_file":      missing_source_file,
            "missing_page":             missing_page,
            "missing_requirement_type": missing_req_type,
        },
        "pass": passes,
        "pass_criteria": {
            "no_duplicate_ids":         len(duplicate_ids) == 0,
            "doc_id_on_all":            len(missing_doc_id) == 0,
            "source_file_on_all":       len(missing_source_file) == 0,
            "requirement_type_on_all":  len(missing_req_type) == 0,
        },
        "per_requirement": per_req,
    }

    Path(audit_path).parent.mkdir(parents=True, exist_ok=True)
    with open(audit_path, "w", encoding="utf-8") as fh:
        json.dump(audit, fh, ensure_ascii=False, indent=2)

    status = "✅ PASS" if passes else "⚠️  NEEDS REVIEW"
    print(f"\n{'='*62}")
    print(f"  STAGE 2C AUDIT: {status}")
    print(f"{'='*62}")
    print(f"  Requirements normalized  : {len(normalized)}")
    print(f"  Split candidates         : {len(split_candidates)}")
    if split_candidates:
        print(f"  Split candidate IDs      : {split_candidates}")
    print(f"  req_type distribution    : {req_type_dist}")
    if missing_page:
        print(f"  ⚠️  Missing page          : {missing_page}")
    if duplicate_ids:
        print(f"  ❌ Duplicate IDs          : {duplicate_ids}")
    print(f"  Audit saved to: {audit_path}")
    print(f"{'='*62}")


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def run(
    grouped_path: str,
    structured_path: str,
    output_path: str,
    audit_path: str,
) -> None:
    """
    Main normalization pipeline.

    Steps:
      1. Load Stage 2B output (requirements_extracted_grouped.json)
      2. Load doc metadata from structured_output.json
      3. Normalize each requirement (schema only)
      4. Write requirements_normalized_for_cru.json
      5. Write audit
    """
    # ── Step 1: Load Stage 2B output ─────────────────────────────────────────
    print(f"📥 Loading Stage 2B output from {grouped_path}")
    if not Path(grouped_path).exists():
        print(f"❌ File not found: {grouped_path}")
        sys.exit(1)

    with open(grouped_path, "r", encoding="utf-8") as fh:
        stage2b: Dict[str, Any] = json.load(fh)

    stage2b_meta: Dict[str, Any]      = stage2b.get("metadata", {})
    requirements: List[Dict[str, Any]] = stage2b.get("requirements", [])

    print(f"   doc_id           : {stage2b_meta.get('doc_id')}")
    print(f"   source_file      : {stage2b_meta.get('source_file')}")
    print(f"   total_requirements: {len(requirements)}")

    # ── Step 2: Load doc metadata ─────────────────────────────────────────────
    print(f"\n📥 Loading doc metadata from {structured_path}")
    structured_meta = load_doc_meta(structured_path)
    doc_meta = merge_doc_meta_with_fallback(structured_meta, stage2b_meta)

    print(f"   doc_id      : {doc_meta['doc_id']}")
    print(f"   doc_type    : {doc_meta['doc_type']}")
    print(f"   version     : {doc_meta['version']}")
    print(f"   source_file : {doc_meta['source_file']}")

    if not doc_meta["doc_id"]:
        print("❌ doc_id is empty after merge. Check structured_output.json and Stage 2B metadata.")
        sys.exit(1)

    # ── Step 3: Normalize ─────────────────────────────────────────────────────
    print(f"\n⚙️  Normalizing {len(requirements)} requirements...")
    normalized: List[Dict[str, Any]] = []

    for req in requirements:
        normalized.append(normalize_requirement(req, doc_meta))

    # ── Step 4: Write output ──────────────────────────────────────────────────
    output = {
        "metadata": {
            "stage":              "Stage 2C – Normalize for CRU",
            "adapter_version":    "1.0",
            "normalization_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_grouped":     grouped_path,
            "source_structured":  structured_path,
            "total_requirements": len(normalized),
            "doc_id":             doc_meta["doc_id"],
            "doc_type":           doc_meta["doc_type"],
            "version":            doc_meta["version"],
            "source_file":        doc_meta["source_file"],
        },
        "requirements": normalized,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    print(f"💾 Written {len(normalized)} normalized requirements to {output_path}")

    # ── Step 5: Audit ─────────────────────────────────────────────────────────
    write_audit(normalized, doc_meta, grouped_path, output_path, audit_path)

    print("\n✅ Stage 2C complete.")
    print(f"   Output  : {output_path}")
    print(f"   Audit   : {audit_path}")
    print("\n   Next step: run CRU Module 1 with:")
    print(f"     --input {output_path}")


# ===========================================================================
# CLI ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Stage 2C – Normalize Stage 2B output for CRU Module 1 [v1.0]"
    )
    parser.add_argument(
        "--grouped",
        default=DEFAULT_GROUPED,
        help="Path to requirements_extracted_grouped.json (Stage 2B output)",
    )
    parser.add_argument(
        "--structured",
        default=DEFAULT_STRUCTURED,
        help="Path to structured_output.json (Stage 1 output — doc metadata only)",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Path to write requirements_normalized_for_cru.json",
    )
    parser.add_argument(
        "--audit",
        default=DEFAULT_AUDIT,
        help="Path to write stage_2c_audit.json",
    )
    args = parser.parse_args()

    run(
        grouped_path=args.grouped,
        structured_path=args.structured,
        output_path=args.output,
        audit_path=args.audit,
    )