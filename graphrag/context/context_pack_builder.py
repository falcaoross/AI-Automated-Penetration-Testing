"""
graphrag/context/context_pack_builder.py
==========================================
FIXES IN THIS VERSION:
  [BUG-2] CRITICAL – false MISSING_EVIDENCE warning eliminated.

          Root cause:
            build_warnings() was called with:
              child_chunks=[n for n in evidence_nodes if n.get("provenance") == "graph"]
            But graph_retriever didn't stamp provenance on nodes (fixed in graph_retriever.py).
            Even after that fix, the right approach is to pass the already-materialized
            EvidenceChunk objects (which always have provenance set) rather than
            re-filtering raw nodes. This removes the fragile dependency entirely.

          Fix: build_warnings() now receives the materialized evidence_chunks list
          (List[EvidenceChunk]) instead of the raw evidence_nodes dict list.
          warnings_builder.py's signature already supports this because EvidenceChunk
          has .chunk_id, .provenance, and .path_confidence attributes.

Previously-correct items retained:
  - real score/confidence/provenance passed from graph_result (not hardcoded 1.0)
  - open_questions populated for zero-confidence graph nodes
  - warnings_builder and trace_paths helpers wired
  - parent context attached AFTER child selection
  - parents ≤ children cap enforced by attach_parent_context
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from graphrag.context.trace_paths import build_trace_paths
from graphrag.context.warnings_builder import build_warnings
from graphrag.models.contracts import (
    Anchor,
    ContextPack,
    EvidenceChunk,
    OpenQuestion,
    RelatedNode,
    TracePath,
    Warning,
)
from graphrag.retrieval.parent_context import attach_parent_context
from graphrag.storage.graph_store import GraphStore


def _parse_source_locator(loc_json: str) -> Dict[str, Any]:
    if not loc_json:
        return {}
    try:
        return json.loads(loc_json)
    except Exception:
        return {"raw": loc_json}


def _parse_extra(extra_json) -> dict:
    if isinstance(extra_json, dict):
        return extra_json
    try:
        return json.loads(extra_json or "{}")
    except Exception:
        return {}


def build_context_pack(
    graph_store: GraphStore,
    anchors: List[Anchor],
    graph_result: Dict[str, Any],
    k_evidence: int = 8,
    k_parent: int = 3,
) -> ContextPack:

    # ── 1. Build child evidence chunks ────────────────────────────────────────
    evidence_chunks: List[EvidenceChunk] = []
    open_questions: List[OpenQuestion] = []

    evidence_nodes = graph_result.get("evidence_nodes", [])
    for i, node in enumerate(evidence_nodes[:k_evidence]):
        if node.get("node_type") != "CHUNK":
            continue

        extra = _parse_extra(node.get("extra_json", "{}"))
        chunk_type = extra.get("chunk_type", "child")

        if chunk_type != "child":
            continue

        score = node.get("score", 0.0)
        confidence = node.get("path_confidence", 0.0)
        # provenance is now always stamped by graph_retriever ("graph")
        # or by merge_graph_and_vector ("vector") in query_graph.py
        provenance = node.get("provenance", "graph")
        needs_conf = node.get("needs_confirmation", False)

        if confidence == 0.0 and provenance == "graph":
            open_questions.append(OpenQuestion(
                question=f"No path confidence for chunk {node['node_id']}; grounding unclear.",
                required_for=node["node_id"],
                chunk_ids_available=[node["node_id"]],
            ))

        evidence_chunks.append(EvidenceChunk(
            chunk_id=node["node_id"],
            chunk_type=chunk_type,
            text=node.get("text", ""),
            doc_id=node.get("doc_id", ""),
            doc_type=node.get("doc_type", ""),        # [PARTIAL-2] now passed through
            section_path=node.get("section_path", ""),
            source_locator=_parse_source_locator(node.get("source_locator_json")),
            module=node.get("module"),
            version=node.get("version"),
            score=score,
            confidence=confidence,
            provenance=provenance,
            needs_confirmation=needs_conf,
        ))

    # ── 2. Parent context AFTER child selection ───────────────────────────────
    parent_context = attach_parent_context(
        graph_store=graph_store,
        child_chunks=evidence_chunks,
        k_parent=k_parent,
    )

    # ── 3. Trace paths ────────────────────────────────────────────────────────
    anchor_ids = [a.node_id for a in anchors]
    raw_paths = graph_result.get("trace_paths", [])
    trace_path_objs: List[TracePath] = []
    for tp in build_trace_paths(anchor_ids, raw_paths):
        trace_path_objs.append(TracePath(
            why=tp["why"],
            path=tp["path"],
            path_confidence=tp.get("path_confidence", 0.0),
        ))

    # ── 4. Related nodes ──────────────────────────────────────────────────────
    related_nodes = [
        RelatedNode(
            node_type=r["node_type"],
            node_id=r["node_id"],
            relation=r["relation"],
        )
        for r in graph_result.get("related_nodes", [])
    ]

    # ── 5. Warnings ───────────────────────────────────────────────────────────
    # [BUG-2] FIX: pass materialized EvidenceChunk objects, not raw dict nodes
    # filtered by provenance. EvidenceChunk always has .provenance set correctly.
    fallback_triggered = any(c.provenance == "vector" for c in evidence_chunks)
    fallback_reason = "vector fallback was used" if fallback_triggered else ""

    # Convert EvidenceChunk dataclass to dicts for warnings_builder
    chunk_dicts = [
        {
            "chunk_id": c.chunk_id,
            "node_id": c.chunk_id,
            "text": c.text,
            "doc_id": c.doc_id,
            "section_path": c.section_path,
            "provenance": c.provenance,
            "path_confidence": c.confidence,
            "score": c.score,
        }
        for c in evidence_chunks
    ]

    warning_objs: List[Warning] = build_warnings(
        child_chunks=chunk_dicts,           # ← materialized chunks, never empty due to filter bug
        anchors=[{"node_id": a.node_id} for a in anchors],
        required_fields=["text", "doc_id", "section_path"],
        fallback_triggered=fallback_triggered,
        fallback_reason=fallback_reason,
    )

    return ContextPack(
        anchors=anchors,
        evidence_chunks=evidence_chunks,
        parent_context=parent_context,
        trace_paths=trace_path_objs,
        related_nodes=related_nodes,
        warnings=warning_objs,
        open_questions=open_questions,
    )
