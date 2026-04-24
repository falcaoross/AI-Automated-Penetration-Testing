"""
block_classifier.py — Block classification for the Autopilot-QA Ingestion Engine.

Public API: classify_blocks(blocks, document_skeleton) -> list

Receives the flat blocks list from body_extractor and fills in four fields
on every block dict (mutated in place):
  - section_semantic_type: section-level classification (functional_requirements,
                            definitions, interface_requirements, etc.)
  - structural_role:        block-level role (req_id_label, req_title_label,
                            req_desc_label, section_heading, prose_content, etc.)
  - skip:                   True if this block should be excluded from Layer 2
                            requirement extraction
  - candidate_req_id:       extracted requirement ID string for req_id_label blocks
  - candidate_dependencies: list of dependency ID strings for req_dependency_label blocks

Does NOT read any PDF file.
Does NOT create or remove blocks.
Does NOT parse full requirement objects — that is Layer 2's responsibility.
Does NOT use font size or visual features — purely text and pattern based.
"""

import re


# ---------------------------------------------------------------------------
# Compiled regex patterns — structural_role detection
# ---------------------------------------------------------------------------

REQ_ID_RE    = re.compile(r'^ID\s*:\s*\S+',      re.IGNORECASE)
TITLE_RE     = re.compile(r'^TITLE\s*:',          re.IGNORECASE)
DESC_RE      = re.compile(r'^DESC\s*:',           re.IGNORECASE)
RAT_RE       = re.compile(r'^RAT\s*:',            re.IGNORECASE)
DEP_RE       = re.compile(r'^DEP\s*:',            re.IGNORECASE)
FIT_RE       = re.compile(r'^FIT\s*:',            re.IGNORECASE)

PLANG_RE = re.compile(
    r'^(SHALL|MUST|SHOULD|MAY|WILL|TAG|AMBIGUITY|SCALE|METER|'
    r'PAST|RECORD|CURRENT|TREND|FUTURE|WISH|STAKEHOLDER|OWNER|'
    r'PRIORITY|REVISION|GIST)\s*:',
    re.IGNORECASE,
)

GHERKIN_RE = re.compile(
    r'^(Feature|Scenario|Scenario Outline|Given|When|Then|And|But|'
    r'Background|Examples)\s*[:\s]',
    re.IGNORECASE,
)

# Candidate req-id extraction
REQ_ID_EXTRACT_RE = re.compile(
    r'^ID\s*:\s*([A-Za-z0-9_\-\.]+)',
    re.IGNORECASE,
)

# Dependency extraction — matches tokens like FR1, NFR-03, UC_4.1
DEP_EXTRACT_RE = re.compile(
    r'([A-Za-z][A-Za-z0-9_\-\.]*\d)',
    re.IGNORECASE,
)

# Dependency noise words to filter out
_IGNORE_DEPS = {"none", "n/a", "na"}

# ---------------------------------------------------------------------------
# section_semantic_type — path/title patterns
# ---------------------------------------------------------------------------

_RE_INTRO       = re.compile(r'^1(\.|$)')
_RE_FUNC        = re.compile(r'^3\.2(\.|$)')
_RE_PERF        = re.compile(r'^3\.3(\.|$)')   # used only for title fallback
_RE_SYSATTR = re.compile(r'^3\.5(\.|$)')
_RE_SECT4       = re.compile(r'^4(\.|$)')
_RE_APPENDIX    = re.compile(r'^A\.')


def _classify_section(section_path: str, section_title: str) -> str:
    """Return the section_semantic_type for a given section_path / title.

    Rules are evaluated in priority order; the first match wins.
    section_title is already a plain string — lowercased internally.
    """
    if section_path == "PREAMBLE":
        return "toc"

    title = section_title.lower() if section_title else ""

    # introduction
    if _RE_INTRO.match(section_path):
        return "introduction"
    if any(kw in title for kw in
           ("introduction", "purpose", "scope", "overview", "background")):
        return "introduction"

    # definitions
    if any(kw in title for kw in
           ("definition", "acronym", "abbreviation", "glossary", "terminology")):
        return "definitions"

    # references
    if any(kw in title for kw in ("reference", "bibliography", "related document")):
        return "references"

    # design_constraints
    if any(kw in title for kw in
           ("design constraint", "standard", "regulatory", "compliance")):
        return "design_constraints"

    # system_overview
    if any(kw in title for kw in (
        "overall description", "product perspective", "product function",
        "user characteristic", "assumption", "dependency",
        "apportioning",
    )):
        return "system_overview"

    # interface_requirements
    if any(kw in title for kw in (
        "interface", "hardware interface", "software interface",
        "communications interface", "user interface",
    )):
        return "interface_requirements"

    # functional_requirements
    if _RE_FUNC.match(section_path):
        return "functional_requirements"

    # performance_requirements — path rule
    if _RE_PERF.match(section_path):
        return "performance_requirements"
    
    if _RE_SYSATTR.match(section_path):      # ← ADD THIS BLOCK
        return "quality_attributes"

    if any(kw in title for kw in
           ("functional requirement", "user class", "use case")):
        return "functional_requirements"

    # performance_requirements
    if any(kw in title for kw in
           ("performance", "speed", "capacity", "reliability", "availability")):
        return "performance_requirements"


    # quality_attributes
    if any(kw in title for kw in (
        "quality", "attribute", "maintainability", "portability",
        "security", "usability", "software system attribute",
    )):
        return "quality_attributes"

    # prioritization — checked BEFORE appendix
    if _RE_SECT4.match(section_path) or _RE_APPENDIX.match(section_path):
        return "prioritization"
    if any(kw in title for kw in
           ("prioriti", "release plan", "cost-value", "five-way", "i-star", "istar")):
        return "prioritization"

    # appendix — only if prioritization did not already fire
    if section_path.startswith("A.") or "appendix" in title:
        return "appendix"

    # parent-path inheritance fallback
    if "." in section_path:
        parent_path = section_path.rsplit(".", 1)[0]
        parent_type = _classify_section(parent_path, "")
        if parent_type != "other":
            return parent_type

    return "other"


# ---------------------------------------------------------------------------
# structural_role helper
# ---------------------------------------------------------------------------

def _classify_role(block: dict) -> str:
    """Return the structural_role for a single block."""
    btype = block["block_type"]
    text  = block["text"]

    if btype == "heading":
        return "section_heading"
    if btype == "table":
        return "table_block"

    # Labeled-field patterns (order matches spec)
    if REQ_ID_RE.match(text):   return "req_id_label"
    if TITLE_RE.match(text):    return "req_title_label"
    if DESC_RE.match(text):     return "req_desc_label"
    if RAT_RE.match(text):      return "req_rationale_label"
    if DEP_RE.match(text):      return "req_dependency_label"
    if FIT_RE.match(text):      return "req_fit_label"
    if PLANG_RE.match(text):    return "planguage_keyword"
    if GHERKIN_RE.match(text):  return "gherkin_keyword"

    return "prose_content"


# ---------------------------------------------------------------------------
# skip logic
# ---------------------------------------------------------------------------

_REQ_ROLES = frozenset({
    "req_id_label", "req_title_label", "req_desc_label",
    "req_rationale_label", "req_dependency_label", "req_fit_label",
})

_REQ_SST = frozenset({
    "functional_requirements", "interface_requirements",
    "performance_requirements", "design_constraints", "quality_attributes",
})


def _should_skip(block: dict, sst: str, role: str) -> bool:
    """Return True if this block should be excluded from Layer 2 extraction."""

    # Condition 1 — PREAMBLE
    if block["section_path"] == "PREAMBLE":
        return True

    # Condition 2 — TOC noise (non-heading blocks in toc section)
    if sst == "toc" and role != "section_heading":
        return True

    # Condition 3 — Short noise (but keep labeled req fields)
    if len(block["text"].strip()) < 8 and role not in _REQ_ROLES:
        return True

    # Condition 4 — Pure page number line
    if block["text"].strip().isdigit():
        return True

    # Condition 5 — Low-confidence heading in non-requirement section
    if (block.get("low_confidence_confirmation") is True
            and role == "section_heading"
            and sst not in _REQ_SST):
        return True

    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_blocks(blocks: list, document_skeleton: dict) -> list:
    """Classify every block dict in-place and return the same list.

    Fills:  section_semantic_type, structural_role, skip,
            candidate_req_id, candidate_dependencies

    Args:
        blocks:            List of block dicts from body_extractor.
        document_skeleton: Fully augmented skeleton (post body-scan).

    Returns:
        The same list, all four fields populated on every block.
    """
    # Cache: section_path → section_semantic_type
    sst_cache: dict[str, str] = {}

    for block in blocks:
        sec_path  = block["section_path"]
        sec_title = block.get("section_title", "")

        # ── section_semantic_type ──────────────────────────────────────────
        if sec_path not in sst_cache:
            # Use skeleton title when available (more reliable than block field)
            entry = document_skeleton.get(sec_path, {})
            title = entry.get("title", sec_title) if entry else sec_title
            sst_cache[sec_path] = _classify_section(sec_path, title)
            if entry:
                entry["section_semantic_type"] = sst_cache[sec_path]
            sst = sst_cache[sec_path]

            # If local classification falls back to "other", inherit from an
            # already-classified parent section instead of recursing without title.
            if sst == "other" and "." in sec_path:
                parent_path = sec_path.rsplit(".", 1)[0]
                cached_parent = sst_cache.get(parent_path)
                if cached_parent and cached_parent != "other":
                    sst = cached_parent
                    sst_cache[sec_path] = sst
                    entry["section_semantic_type"] = sst

        sst = sst_cache[sec_path]
        block["section_semantic_type"] = sst

        # ── structural_role ────────────────────────────────────────────────
        role = _classify_role(block)
        block["structural_role"] = role

        # ── skip ───────────────────────────────────────────────────────────
        block["skip"] = _should_skip(block, sst, role)

        # ── candidate_req_id ───────────────────────────────────────────────
        if role == "req_id_label":
            m = REQ_ID_EXTRACT_RE.match(block["text"])
            block["candidate_req_id"] = m.group(1).strip() if m else None
        else:
            block["candidate_req_id"] = None

        # ── candidate_dependencies ─────────────────────────────────────────
        if role == "req_dependency_label":
            dep_text = re.sub(r'^DEP\s*:\s*', '', block["text"], flags=re.IGNORECASE)
            raw_deps = DEP_EXTRACT_RE.findall(dep_text)
            block["candidate_dependencies"] = [
                d for d in raw_deps if d.lower() not in _IGNORE_DEPS
            ]
        else:
            block["candidate_dependencies"] = []

    return blocks
