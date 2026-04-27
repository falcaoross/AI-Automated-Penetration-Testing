from __future__ import annotations

"""llm_extractor.py - LLM-backed extractor for prose and gherkin format groups.

Single responsibility: assemble a prompt from a RequirementGroup's blocks,
call a local Ollama instance, parse the JSON response, and return an
ExtractedRequirement.

Fixes applied:
  Bug 3 - scenarios always null: added gherkin-specific system prompt that
           includes the scenarios key, plus _normalise_scenarios() helper.
           extract_llm() now branches on group.format to use the right prompt
           and extracts scenarios when format == "gherkin".

  Bug 4 - list serialized as str: _clean_optional() now detects list values
           and joins them with "; " instead of calling str(list), which was
           producing Python repr strings like "[\'item1\', \'item2\']".
"""

import json
import re
import textwrap
from typing import Optional

import httpx

from block_grouper import RequirementGroup
from schemas import ExtractedRequirement
from utils import clean_text, build_source_reference, map_confidence


# ---------------------------------------------------------------------------
# Prompt templates - one per format
# ---------------------------------------------------------------------------

_SYSTEM_INSTRUCTION_PROSE = textwrap.dedent("""\
You are a security requirements engineering assistant.
Extract the following fields from the requirement text below to build security-aware behavioral specifications.
Return ONLY a valid JSON object - no markdown, no explanation, no code fences.

Required JSON keys (all snake_case):
  req_id                - the requirement identifier (e.g. FR1, QR3)
  title                 - short requirement title
  actor                 - who performs the action (e.g. User, System, Administrator)
  actions               - list of action strings
  description           - full requirement description
  constraints           - any constraints or limits mentioned (string or null, NOT a list)
  dependencies          - list of dependency IDs (e.g. ["FR1","FR3"]) or empty list
  acceptance_criteria   - rationale or acceptance condition (string or null, NOT a list)
  outputs               - expected outputs or wish-level targets (string or null, NOT a list)

Rules:
- Return null for any field you cannot find.
- Return [] for empty list fields.
- constraints, acceptance_criteria, and outputs MUST be a plain string or null, never a list.
- Do not add extra keys.
- req_id fallback: if no explicit ID is in the text, use the candidate_req_id hint provided below.\
""")

_SYSTEM_INSTRUCTION_GHERKIN = textwrap.dedent("""\
You are a security requirements engineering assistant.
The text below is a Gherkin-format software requirement (Given/When/Then scenarios).
Extract the following fields to build security-aware behavioral specifications. Return ONLY a valid JSON object - no markdown, no
explanation, no code fences.

Required JSON keys (all snake_case):
  req_id                - the requirement identifier from the ID line (e.g. FR22).
                          Use the candidate_req_id hint below - do NOT change it.
  title                 - short title: the text on the ID line after stripping the ID token
  actor                 - primary actor: User / Restaurant Owner / Administrator / System
  actions               - list of the main actions the actor performs across all scenarios
  description           - one-sentence summary of the overall requirement purpose
  constraints           - any preconditions or limits as a single string (or null)
  dependencies          - list of dependency IDs mentioned, or empty list
  acceptance_criteria   - single string summarising the overall acceptance condition (or null)
  outputs               - expected system outputs or results as a single string (or null)
  scenarios             - list of scenario objects extracted from the Gherkin blocks.
                          Each object must have exactly these keys:
                            name  (string - the Scenario title line)
                            given (string - the Given step text)
                            when  (string - the When step text)
                            then  (string - the Then step text)

Rules:
- Return null for any optional field you cannot find.
- Return [] for empty list fields.
- constraints, acceptance_criteria, outputs, description MUST be plain strings or null - NEVER lists.
- scenarios MUST be a JSON array of objects - one object per Scenario block in the text.
  If there are two Scenario blocks, return two objects. Do not collapse them into one.
- Do not add extra keys.
- req_id MUST match the candidate_req_id hint exactly - do not modify it.\
""")

_FENCE_RE = re.compile(r'```(?:json)?\s*', re.IGNORECASE)


# Private helpers

def _assemble_text(group: RequirementGroup) -> str:
    """Join clean text of all non-skipped blocks, separated by newlines."""
    parts = [
        clean_text(block["text"])
        for block in group.blocks
        if not block["skip"]
    ]
    return "\n".join(p for p in parts if p)


def _build_prompt(system_instruction: str, group: RequirementGroup, assembled_text: str) -> str:
    """Combine the system instruction and per-group context into one prompt."""
    context = (
        f"candidate_req_id: {group.candidate_req_id}\n"
        f"section: {group.section_path}\n"
        f"semantic_type: {group.section_semantic_type}\n"
        f"\n"
        f"Requirement text:\n"
        f"{assembled_text}"
    )
    return system_instruction + "\n\n" + context


def _normalise_actions(raw: object) -> list[str]:
    """Coerce the LLM actions value into a clean list of strings."""
    if isinstance(raw, list):
        return [clean_text(str(item)) for item in raw if item and str(item).strip()]
    if isinstance(raw, str) and raw.strip():
        return [clean_text(line) for line in raw.splitlines() if clean_text(line)]
    return []


def _normalise_dependencies(raw: object) -> list[str]:
    """Coerce the LLM dependencies value into a list of ID strings."""
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, str) and raw.strip():
        return [t.strip() for t in raw.split(",") if t.strip()]
    return []


def _normalise_scenarios(raw: object) -> Optional[list[dict]]:
    """Coerce the LLM scenarios value into a list of {name, given, when, then} dicts.

    Returns None if raw is empty, None, or not a list of dicts.
    Tolerates missing keys within each scenario object.
    """
    if not raw or not isinstance(raw, list):
        return None
    result: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        scenario = {
            "name":  _clean_optional(item.get("name"))  or "",
            "given": _clean_optional(item.get("given")) or "",
            "when":  _clean_optional(item.get("when"))  or "",
            "then":  _clean_optional(item.get("then"))  or "",
        }
        result.append(scenario)
    return result if result else None


def _clean_optional(value: object) -> Optional[str]:
    """Return a clean non-empty string, or None.

    BUG 4 FIX: if value is a list, join items with '; ' instead of calling
    str(list), which previously produced Python repr like "['a', 'b']".
    """
    if value is None:
        return None
    if isinstance(value, list):
        parts = [clean_text(str(item)) for item in value if item is not None and str(item).strip()]
        joined = "; ".join(p for p in parts if p)
        return joined if joined else None
    cleaned = clean_text(str(value))
    return cleaned if cleaned else None


# Public API

def extract_llm(
    group: RequirementGroup,
    model: str = "qwen2.5:14b-instruct",
    ollama_url: str = "http://localhost:11434",
    timeout: float = 600.0,
) -> ExtractedRequirement:
    """Extract a structured ExtractedRequirement by calling a local Ollama LLM.

    Selects the correct system prompt based on group.format:
      - "gherkin" -> _SYSTEM_INSTRUCTION_GHERKIN (includes scenarios key)
      - "prose"   -> _SYSTEM_INSTRUCTION_PROSE

    Args:
        group:       A RequirementGroup with format "prose" or "gherkin".
        model:       Ollama model name (default "qwen2.5:14b-instruct").
        ollama_url:  Base URL of the local Ollama instance.
        timeout:     HTTP request timeout in seconds.

    Returns:
        A fully populated ExtractedRequirement instance.

    Raises:
        RuntimeError:           If Ollama returns a non-200 HTTP status.
        ValueError:             If the response body is not valid JSON.
        httpx.TimeoutException: If the request exceeds the timeout.
    """
    assembled_text = _assemble_text(group)

    # BUG 3 FIX: choose prompt based on format
    system_instr = (
        _SYSTEM_INSTRUCTION_GHERKIN
        if group.format == "gherkin"
        else _SYSTEM_INSTRUCTION_PROSE
    )
    full_prompt = _build_prompt(system_instr, group, assembled_text)

    # -- Ollama API call ---------------------------------------------------
    response = httpx.post(
        f"{ollama_url}/api/generate",
        json={
            "model":  model,
            "prompt": full_prompt,
            "stream": False,
            "format": "json",
        },
        timeout=timeout,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Ollama returned HTTP {response.status_code}: {response.text}"
        )

    # -- Extract and clean the raw LLM output string -----------------------
    raw: str = response.json().get("response", "")
    raw = _FENCE_RE.sub("", raw).strip()

    # -- Parse JSON --------------------------------------------------------
    try:
        parsed: dict = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"LLM response is not valid JSON.\n"
            f"JSONDecodeError: {exc}\n"
            f"Raw output:\n{raw}"
        ) from exc

    # -- Field normalisation -----------------------------------------------
    # req_id: always use candidate_req_id as the authoritative source for
    # gherkin (LLM must not change it); accept LLM value only for prose.
    raw_req_id = parsed.get("req_id")
    if group.format == "gherkin":
        # Enforce: req_id comes from candidate_req_id, not the LLM
        req_id = group.candidate_req_id or ""
    elif raw_req_id and str(raw_req_id).strip():
        req_id = str(raw_req_id).strip()
    else:
        req_id = group.candidate_req_id or ""

    title        = clean_text(str(parsed["title"])) if parsed.get("title") else ""
    actor        = _clean_optional(parsed.get("actor")) or "System"
    actions      = _normalise_actions(parsed.get("actions"))
    description  = _clean_optional(parsed.get("description"))
    constraints  = _clean_optional(parsed.get("constraints"))
    dependencies = _normalise_dependencies(parsed.get("dependencies"))
    acceptance_criteria = _clean_optional(
        parsed.get("acceptance_criteria")
        or parsed.get("acceptancecriteria")
        or parsed.get("acceptance criteria")
    )
    if not acceptance_criteria:
        acceptance_criteria = description
    outputs      = _clean_optional(parsed.get("outputs"))

    # BUG 3 FIX: extract scenarios for gherkin format
    scenarios: Optional[list[dict]] = None
    if group.format == "gherkin":
        scenarios = _normalise_scenarios(parsed.get("scenarios"))

    # -- Confidence --------------------------------------------------------
    has_nulls = not description or not actor or not actions
    confidence = map_confidence("llm", has_nulls)

    # -- Source reference --------------------------------------------------
    if group.blocks:
        source_ref = build_source_reference(group.blocks[0])
    else:
        source_ref = {
            "doc_id":         None,
            "section_path":   group.section_path,
            "source_locator": None,
            "module":         None,
            "version":        None,
        }

    return ExtractedRequirement(
        req_id                = req_id,
        section_path          = group.section_path,
        section_semantic_type = group.section_semantic_type,
        input_format          = group.format,
        extraction_method     = "llm",
        title                 = title,
        actor                 = actor,
        actions               = actions,
        description           = description,
        constraints           = constraints,
        dependencies          = dependencies,
        acceptance_criteria   = acceptance_criteria,
        outputs               = outputs,
        planguage_table       = None,
        scenarios             = scenarios,
        confidence            = confidence,
        source_reference      = source_ref,
    )