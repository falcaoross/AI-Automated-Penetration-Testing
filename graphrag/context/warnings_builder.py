"""
graphrag/context/warnings_builder.py
=======================================
FIX: signature updated to match context_pack_builder.build_context_pack() call:
  build_warnings(child_chunks, anchors, required_fields,
                 fallback_triggered, fallback_reason, threshold=0.65)

Old signature was (evidence_chunks, threshold) – caused TypeError on every query.
Returns List[Warning] dataclass instances so ContextPack is typed correctly.
"""
from __future__ import annotations

from typing import Any, Dict, List

from graphrag.models.contracts import Warning


def build_warnings(
    child_chunks: List[Dict[str, Any]],
    anchors: List[Dict[str, Any]],
    required_fields: List[str],
    fallback_triggered: bool = False,
    fallback_reason: str = "",
    threshold: float = 0.65,
) -> List[Warning]:
    warnings: List[Warning] = []

    # ── No evidence at all ────────────────────────────────────────────────────
    if not child_chunks:
        warnings.append(Warning(
            type="MISSING_EVIDENCE",
            message=(
                "No evidence chunks found; downstream generation must produce "
                "open questions rather than guessed expected results."
            ),
        ))
        return warnings

    # ── Missing required fields on individual chunks ──────────────────────────
    for chunk in child_chunks:
        chunk_id = chunk.get("chunk_id") or chunk.get("node_id", "unknown")
        for f in required_fields:
            if not chunk.get(f):
                warnings.append(Warning(
                    type="MISSING_FIELD",
                    message=f"Chunk {chunk_id} is missing required field '{f}'.",
                    chunk_id=chunk_id,
                ))

    # ── Low-confidence vector hits ────────────────────────────────────────────
    for chunk in child_chunks:
        confidence = float(chunk.get("path_confidence", chunk.get("score", 0.0)))
        provenance = chunk.get("provenance", "graph")
        if provenance != "graph" and confidence < threshold:
            chunk_id = chunk.get("chunk_id") or chunk.get("node_id")
            warnings.append(Warning(
                type="LOW_CONFIDENCE_VECTOR",
                message=(
                    f"Chunk {chunk_id} came from vector fallback with "
                    f"confidence {confidence:.2f} (below threshold {threshold})."
                ),
                chunk_id=chunk_id,
            ))

    # ── Vector fallback was triggered ─────────────────────────────────────────
    if fallback_triggered:
        warnings.append(Warning(
            type="VECTOR_FALLBACK_TRIGGERED",
            message=fallback_reason or "Vector fallback was used; some evidence may not be graph-traced.",
        ))

    # ── No anchors resolved ───────────────────────────────────────────────────
    if not anchors:
        warnings.append(Warning(
            type="NO_ANCHORS",
            message="No anchor nodes could be resolved; query may be too vague or req_id not in graph.",
        ))

    return warnings