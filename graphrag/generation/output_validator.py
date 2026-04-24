from typing import Dict, Any, List, Set


def _to_dict(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        return item
    if hasattr(item, "__dict__"):
        return item.__dict__
    if isinstance(item, str):
        return {"chunk_id": item}
    return {}


def _extract_valid_chunk_ids(context_pack: Dict[str, Any]) -> Set[str]:
    chunk_ids = set()

    for c in context_pack.get("evidence_chunks", []):
        c_dict = _to_dict(c)
        chunk_id = c_dict.get("chunk_id")
        if chunk_id:
            chunk_ids.add(chunk_id)

    return chunk_ids


def validate_generated_tests(context_pack: Dict[str, Any], generated: Dict[str, Any]) -> List[str]:
    errors = []

    valid_chunk_ids = _extract_valid_chunk_ids(context_pack)

    test_cases = generated.get("test_cases")
    if not isinstance(test_cases, list) or not test_cases:
        errors.append("Output must contain a non-empty test_cases list.")
        return errors

    for i, tc in enumerate(test_cases, start=1):
        steps = tc.get("steps", [])
        if not steps:
            errors.append(f"Test case {i} has no steps.")
            continue

        for j, step in enumerate(steps, start=1):
            # [BUG-3] FIX: unresolved steps were legitimately produced by the
            # no-guess rule in test_generator.py (enforce_no_guess_citations).
            # Flagging them here as errors contradicts that contract.
            # Skip both the expected_result check and the evidence_chunk_ids
            # check when unresolved=True — the open_question already records
            # why the step is ungrounded; a duplicate validation_error is noise.
            is_unresolved = step.get("unresolved") is True

            expected_result = step.get("expected_result")
            cited_ids = step.get("evidence_chunk_ids", [])

            if not expected_result and not is_unresolved:
                errors.append(f"Test case {i}, step {j} missing expected_result.")

            if is_unresolved:
                # Legitimately unresolved — no citation required; skip checks.
                continue

            if not isinstance(cited_ids, list) or not cited_ids:
                errors.append(f"Test case {i}, step {j} must cite at least one evidence_chunk_id.")
                continue

            for cid in cited_ids:
                if cid not in valid_chunk_ids:
                    errors.append(
                        f"Test case {i}, step {j} cites invalid chunk_id: {cid}"
                    )

    return errors