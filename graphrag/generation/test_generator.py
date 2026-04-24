"""
graphrag/generation/test_generator.py
=======================================
FIXES IN THIS VERSION:
  [BUG-3] CRITICAL – removed the fallback-to-first-valid-chunk logic in
          repair_invalid_citations().

          Old behavior (hallucination risk):
            If a step had no valid evidence_chunk_ids, the code picked
            list(valid_chunk_ids)[0] and appended it, making unsupported
            expected results LOOK grounded after the fact.

          New behavior (no-guess rule):
            If a step has no valid evidence_chunk_ids after generation,
            it is marked unresolved=True and an OpenQuestion is recorded.
            The output carries open_questions so callers know what is
            ungrounded. Validation still runs; unresolved steps are
            surfaced as validation_errors rather than silently patched.

  Retained correctly:
    - LLM call (ollama), JSON parse, validate_generated_tests
    - _extract_valid_chunk_ids (includes parent_context ids)
    - is_valid flag on output
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set

from graphrag.generation.prompt_builder import build_test_generation_prompt
from graphrag.generation.output_validator import validate_generated_tests

import requests


def _to_dict(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        return item
    if hasattr(item, "__dict__"):
        return item.__dict__
    return {}


def _extract_valid_chunk_ids(context_pack: Dict[str, Any]) -> Set[str]:
    chunk_ids: Set[str] = set()
    for section in ("evidence_chunks", "parent_context"):
        for c in context_pack.get(section, []):
            cid = _to_dict(c).get("chunk_id")
            if cid:
                chunk_ids.add(cid)
    return chunk_ids


def _ollama_generate(prompt: str, model: str = "llama3:8b") -> str:
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.0,
            "top_p": 0.1,
            "repeat_penalty": 1.05,
            "num_predict": 512,
        },
    }
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json().get("response", "")


def call_llm(prompt: str, provider: str = "ollama") -> str:
    if provider == "ollama":
        return _ollama_generate(prompt)
    raise ValueError(f"Unsupported provider: {provider}")


def enforce_no_guess_citations(
    generated: Dict[str, Any],
    context_pack: Dict[str, Any],
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    [BUG-3] Replaces repair_invalid_citations().

    For every step:
      - Keep only chunk IDs that exist in context_pack (evidence_chunks + parent_context).
      - If none are valid → mark step as unresolved, record an OpenQuestion.
      - NEVER insert a fallback chunk ID.

    Returns (updated_generated, open_questions_list).
    """
    valid_chunk_ids = _extract_valid_chunk_ids(context_pack)
    open_questions: List[Dict[str, Any]] = []

    for tc in generated.get("test_cases", []):
        for step_idx, step in enumerate(tc.get("steps", [])):
            cited = step.get("evidence_chunk_ids", [])
            valid_cited = [cid for cid in cited if cid in valid_chunk_ids]

            if valid_cited:
                step["evidence_chunk_ids"] = valid_cited
                step["unresolved"] = False
            else:
                # [BUG-3] FIX: no fallback rewrite – mark unresolved
                step["evidence_chunk_ids"] = []
                step["unresolved"] = True
                open_questions.append({
                    "question": (
                        f"Step {step_idx + 1} of test '{tc.get('title', 'unknown')}' "
                        f"has no valid evidence chunk IDs. "
                        f"Expected result cannot be grounded without evidence."
                    ),
                    "required_for": f"step_{step_idx + 1}",
                    "chunk_ids_available": list(valid_chunk_ids),
                })

    generated["open_questions"] = open_questions
    return generated, open_questions


def generate_tests_from_context_pack(
    context_pack: Dict[str, Any],
    provider: str = "ollama",
) -> Dict[str, Any]:
    prompt = build_test_generation_prompt(context_pack)
    raw_response = call_llm(prompt, provider)

    try:
        generated = json.loads(raw_response)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM did not return valid JSON: {e}")

    # [BUG-3] enforce no-guess rule instead of repairing citations
    generated, open_questions = enforce_no_guess_citations(generated, context_pack)

    errors = validate_generated_tests(context_pack, generated)
    generated["validation_errors"] = errors
    generated["is_valid"] = len(errors) == 0 and len(open_questions) == 0

    return generated


def generate_tests_cli(context_pack_path: str, provider: str = "ollama"):
    context_pack = json.loads(Path(context_pack_path).read_text())
    generated = generate_tests_from_context_pack(context_pack, provider)
    print(json.dumps(generated, indent=2))
    print(f"\nValid: {generated['is_valid']}")
    if generated["validation_errors"]:
        print("Errors:", generated["validation_errors"])
    if generated.get("open_questions"):
        print(f"Open questions: {len(generated['open_questions'])} ungrounded steps")
    return generated
