from __future__ import annotations

"""crubuilder.py — Layer 3: Build Canonical Requirement Units (CRUs) from Layer 2 output.

Single responsibility: transform every ExtractedRequirement into one or more CRU
objects — one per discrete action. Requirements with no actions produce exactly one
CRU with action=None, flagged in the audit.

No LLM calls. No HTTP requests. Pure deterministic logic.

Design decisions:
  - CRU type is derived from section_semantic_type first, then description keywords
    for quality_attributes (which can be reliability, security, portability,
    testability, or generic quality).
  - Confidence maps directly from extraction_method, not from the Layer 2 confidence
    field, because extraction_method is the authoritative provenance signal.
  - Gherkin action lists produced by the LLM contain precondition-state strings
    (e.g. "does not have an account", "is logged in") mixed with real When-clause
    actions. These are filtered before CRU explosion so each CRU represents a
    genuine user/system action, not a precondition.
  - All Layer 2 fields that Layer 4 (chunkdomain.py) needs are carried forward
    verbatim: title, description, acceptance_criteria, scenarios, dependencies,
    outputs, traceability. Nothing is dropped.
"""

import re
from typing import Optional
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# CRU type mapping
# ---------------------------------------------------------------------------

# Primary mapping: section_semantic_type → CRU type.
# quality_attributes is resolved further by _resolve_quality_type().
_SST_TO_TYPE: dict[str, Optional[str]] = {
    "functional_requirements":  "security",
    "interface_requirements":   "security",
    "performance_requirements": "security",
    "design_constraints":       "constraint",
    "quality_attributes":       None,   # resolved by keyword scan
}

# Keyword patterns for quality_attributes sub-classification.
# Evaluated in order; first match wins.
_QUALITY_KEYWORD_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"reliab",  re.IGNORECASE), "reliability"),
    (re.compile(r"secur",   re.IGNORECASE), "security"),
    (re.compile(r"portab",  re.IGNORECASE), "portability"),
    (re.compile(r"testab",  re.IGNORECASE), "testability"),
    (re.compile(r"usab",    re.IGNORECASE), "usability"),
    (re.compile(r"availab", re.IGNORECASE), "availability"),
    (re.compile(r"perform", re.IGNORECASE), "performance"),
    (re.compile(r"maintai", re.IGNORECASE), "maintainability"),
]

# ---------------------------------------------------------------------------
# Confidence mapping
# ---------------------------------------------------------------------------

_EXTRACTION_METHOD_TO_CONFIDENCE: dict[str, str] = {
    "deterministic":  "high",
    "llm":            "medium",
    "regex_fallback": "low",
}

# ---------------------------------------------------------------------------
# Gherkin precondition-state filter
# ---------------------------------------------------------------------------

# Action strings produced from Gherkin LLM extraction contain two classes:
#   (a) Real When-clause actions: "logs in with his/her account"
#   (b) Given-clause precondition states: "does not have an account", "is logged in"
#
# Class (b) strings describe STATE, not ACTION. Including them as CRU actions
# produces untestable CRUs. Filter rule: a string is a precondition state if it
# matches any of these patterns at the start of the stripped text.
_PRECONDITION_PATTERNS: list[re.Pattern] = [
    re.compile(r"^(does not|do not|don't)\b",          re.IGNORECASE),
    re.compile(r"^(has not|have not|hasn't)\b",         re.IGNORECASE),
    re.compile(r"^is (logged|authenticated|signed)\b",  re.IGNORECASE),
    re.compile(r"^(wants? to|want to)\b",               re.IGNORECASE),
    re.compile(r"^(has|have) (lost|forgotten|no)\b",    re.IGNORECASE),
    re.compile(r"^(already|not yet)\b",                 re.IGNORECASE),
    re.compile(r"^logged in\b",                         re.IGNORECASE),
    re.compile(r"^(the )?(system|application) is\b",    re.IGNORECASE),
]


def _is_precondition_state(action: str) -> bool:
    """Return True if this action string describes a precondition state."""
    for pattern in _PRECONDITION_PATTERNS:
        if pattern.search(action.strip()):
            return True
    return False


# ---------------------------------------------------------------------------
# CRU dataclass
# ---------------------------------------------------------------------------

@dataclass
class CRU:
    """A Canonical Requirement Unit — one actor performing one action."""

    cru_id: str
    parent_requirement_id: str
    type: str
    actor: str
    action: Optional[str]
    constraint: Optional[str]
    confidence: str
    title: str
    description: Optional[str]
    acceptance_criteria: Optional[str]
    outputs: Optional[str]
    dependencies: list[str]
    scenarios: Optional[list[dict]]
    traceability: dict
    extraction_method: str
    input_format: str
    invalid: bool = False

    def to_dict(self) -> dict:
        return {
            "cru_id":                self.cru_id,
            "parent_requirement_id": self.parent_requirement_id,
            "type":                  self.type,
            "actor":                 self.actor,
            "action":                self.action,
            "constraint":            self.constraint,
            "confidence":            self.confidence,
            "title":                 self.title,
            "description":           self.description,
            "acceptance_criteria":   self.acceptance_criteria,
            "outputs":               self.outputs,
            "dependencies":          self.dependencies,
            "scenarios":             self.scenarios,
            "traceability":          self.traceability,
            "extraction_method":     self.extraction_method,
            "input_format":          self.input_format,
            "invalid":               self.invalid,
        }


# ---------------------------------------------------------------------------
# Audit flag helpers
# ---------------------------------------------------------------------------

def _make_flag(code: str, req_id: str, message: str, severity: str = "warning") -> dict:
    return {
        "code":     code,
        "req_id":   req_id,
        "message":  message,
        "severity": severity,
    }


# ---------------------------------------------------------------------------
# Type resolution
# ---------------------------------------------------------------------------

def _resolve_quality_type(
    req_id: str,
    description: Optional[str],
    title: str,
) -> tuple[str, dict]:
    """
    Resolve CRU type for quality_attributes by scanning title then description.
    Returns (cru_type, audit_flag).
    """
    search_text = f"{title} {description or ''}"
    for pattern, cru_type in _QUALITY_KEYWORD_RULES:
        if pattern.search(search_text):
            return cru_type, _make_flag(
                "INFO-TYPE-KEYWORD-MATCH",
                req_id,
                f"quality_attributes type resolved to '{cru_type}' via keyword match.",
                severity="info",
            )
    return "quality", _make_flag(
        "INFO-TYPE-DEFAULT-QUALITY",
        req_id,
        "quality_attributes type defaulted to 'quality' — no keyword matched.",
        severity="info",
    )


def _resolve_cru_type(
    req_id: str,
    section_semantic_type: str,
    description: Optional[str],
    title: str,
) -> tuple[str, Optional[dict]]:
    """Return (cru_type, optional_audit_flag) for one requirement."""
    base = _SST_TO_TYPE.get(section_semantic_type)
    if base is not None:
        return base, None
    if section_semantic_type == "quality_attributes":
        return _resolve_quality_type(req_id, description, title)
    # Unknown section_semantic_type — safe default
    return "security", None


# ---------------------------------------------------------------------------
# Action list preparation
# ---------------------------------------------------------------------------

def _prepare_actions(
    req_id: str,
    raw_actions: list[str],
    input_format: str,
) -> tuple[list[Optional[str]], list[dict]]:
    """
    Return (action_list, audit_flags).

    Gherkin format: filters precondition-state strings and emits
    INFO-GHERKIN-ACTION-FILTERED for each one removed.
    All formats: strips whitespace.
    Empty result: returns [None] and emits WARN-NO-ACTION-CRU.
    """
    flags: list[dict] = []
    actions: list[str] = [a.strip() for a in raw_actions if a and a.strip()]

    if input_format == "gherkin":
        kept: list[str] = []
        for action in actions:
            if _is_precondition_state(action):
                flags.append(_make_flag(
                    "INFO-GHERKIN-ACTION-FILTERED",
                    req_id,
                    f'Precondition-state string filtered: "{action}"',
                    severity="info",
                ))
            else:
                kept.append(action)
        actions = kept

    if not actions:
        flags.append(_make_flag(
            "WARN-NO-ACTION-CRU",
            req_id,
            "No actions found after filtering. Producing one CRU with action=null.",
            severity="warning",
        ))
        return [None], flags

    return actions, flags


# ---------------------------------------------------------------------------
# Traceability block
# ---------------------------------------------------------------------------

def _build_traceability(req: dict) -> dict:
    """
    Build the CRU traceability block from source_reference and section_path.
    The 'section' key mirrors section_path for Layer 4 compatibility
    (chunkdomain.py reads traceability.section).
    """
    source_ref = req.get("source_reference") or {}
    return {
        "section":        req.get("section_path"),
        "section_path":   req.get("section_path"),
        "source_locator": source_ref.get("source_locator"),
        "doc_id":         source_ref.get("doc_id"),
        "doc_type":       source_ref.get("doc_type"),
        "module":         source_ref.get("module"),
        "version":        source_ref.get("version"),
    }


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_crus(requirements: list[dict]) -> tuple[list[CRU], list[dict]]:
    """
    Build CRU objects from a list of ExtractedRequirement dicts.

    Args:
        requirements: The list from requirements.json["requirements"].

    Returns:
        (crus, audit_flags)
    """
    all_crus: list[CRU] = []
    all_flags: list[dict] = []

    for req in requirements:
        req_id            = req["req_id"]
        section_sem_type  = req.get("section_semantic_type", "")
        title             = req.get("title") or ""
        description       = req.get("description")
        actor             = req.get("actor") or "System"
        raw_actions       = req.get("actions") or []
        constraints       = req.get("constraints")
        acceptance_crit   = req.get("acceptance_criteria")
        outputs           = req.get("outputs")
        dependencies      = req.get("dependencies") or []
        scenarios         = req.get("scenarios")
        extraction_method = req.get("extraction_method", "")
        input_format      = req.get("input_format", "")
        layer2_confidence = req.get("confidence", "medium")

        # Resolve type
        cru_type, type_flag = _resolve_cru_type(
            req_id, section_sem_type, description, title
        )
        if type_flag:
            all_flags.append(type_flag)

        # Resolve confidence
        confidence = _EXTRACTION_METHOD_TO_CONFIDENCE.get(
            extraction_method, layer2_confidence
        )

        # Prepare actions
        actions, action_flags = _prepare_actions(req_id, raw_actions, input_format)
        all_flags.extend(action_flags)

        # Build traceability
        traceability = _build_traceability(req)

        # Explode: one CRU per action
        for i, action in enumerate(actions, start=1):
            all_crus.append(CRU(
                cru_id=f"CRU-{req_id}-{i:02d}",
                parent_requirement_id=req_id,
                type=cru_type,
                actor=actor,
                action=action,
                constraint=constraints,
                confidence=confidence,
                title=title,
                description=description,
                acceptance_criteria=acceptance_crit,
                outputs=outputs,
                dependencies=list(dependencies),
                scenarios=scenarios,
                traceability=traceability,
                extraction_method=extraction_method,
                input_format=input_format,
                invalid=False,
            ))

    return all_crus, all_flags