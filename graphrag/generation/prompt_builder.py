"""
graphrag/generation/prompt_builder.py
=======================================
FIXES IN THIS VERSION:
  [BUG-3] Removed hardcoded valid_chunk_ids[0] as the example evidence target.

          Old code (line 92 equivalent):
            "evidence_chunk_ids": ["{valid_chunk_ids[0]}"]
          This forced the LLM to always cite the first chunk, even when the
          step's expected result is not grounded in it.

          New behavior:
            - If valid chunk IDs exist: prompt lists ALL valid IDs and instructs
              the model to only use IDs that actually support the step.
            - If NO valid chunk IDs exist: prompt instructs the model to output
              an "open_question" field per step instead of citing anything.
              The no-guess rule is enforced at prompt level, not just post-hoc.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List


def _chunk_summary(chunks: List[Any], max_chunks: int = 5) -> str:
    lines = []
    for chunk in chunks[:max_chunks]:
        if isinstance(chunk, dict):
            d = chunk
        elif hasattr(chunk, "__dict__"):
            d = chunk.__dict__
        else:
            continue
        cid = d.get("chunk_id", "?")
        text = (d.get("text") or "")[:180]
        lines.append(f'  chunk_id: "{cid}"\n  text: {text}')
    return "\n---\n".join(lines) if lines else "  (none)"


def build_test_generation_prompt(context_pack: Dict[str, Any]) -> str:
    # Collect valid chunk IDs
    valid_ids: List[str] = []
    for section in ("evidence_chunks", "parent_context"):
        for c in context_pack.get(section, []):
            d = c if isinstance(c, dict) else (c.__dict__ if hasattr(c, "__dict__") else {})
            cid = d.get("chunk_id")
            if cid:
                valid_ids.append(cid)
    valid_ids = list(dict.fromkeys(valid_ids))   # deduplicate, preserve order

    evidence_text = _chunk_summary(context_pack.get("evidence_chunks", []))

    # [BUG-3] FIX: zero-evidence path instructs model to produce open_question
    if not valid_ids:
        return f"""You are a QA test generator. No grounded evidence was found for this requirement.

RULE: You must NOT invent expected results. For each test step that lacks evidence, output an
open question instead of a citation.

Generate 1 test case in EXACT JSON:
{{
  "test_cases": [{{
    "title": "one sentence title",
    "steps": [{{
      "step_number": 1,
      "action": "one sentence action",
      "expected_result": null,
      "evidence_chunk_ids": [],
      "open_question": "What is the expected result? No evidence chunk was available."
    }}]
  }}]
}}

JSON ONLY. No other text."""

    # [BUG-3] FIX: valid IDs listed but NOT pre-selected — model must choose appropriately
    valid_ids_json = json.dumps(valid_ids)

    return f"""You are a QA test generator. Generate 1 test case grounded in the evidence below.

RULES:
1. evidence_chunk_ids MUST only contain IDs from VALID_CHUNK_IDS.
2. Only cite a chunk if that chunk's text actually supports the expected result.
3. If a step's expected result is not supported by any chunk, set evidence_chunk_ids to []
   and add an open_question field explaining what is missing.
4. Do NOT invent chunk IDs. Do NOT cite a chunk just because it exists.

VALID_CHUNK_IDS: {valid_ids_json}

EVIDENCE:
{evidence_text}

Output EXACT JSON:
{{
  "test_cases": [{{
    "title": "one sentence title",
    "steps": [{{
      "step_number": 1,
      "action": "one sentence action",
      "expected_result": "one sentence expected result grounded in evidence",
      "evidence_chunk_ids": ["<only IDs from VALID_CHUNK_IDS that support this step>"]
    }}]
  }}]
}}

JSON ONLY. No other text."""
