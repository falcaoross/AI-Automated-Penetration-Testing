"""
graphrag/retrieval/parent_context.py
======================================
FIXES IN THIS VERSION:
  [PARTIAL-4] Removed hardcoded score=0.8.
              Parent context score is now derived from the PARENT_OF edge confidence
              (always 1.0 for structural edges). This makes it explicit and auditable
              rather than a magic constant that can't be traced.

  [PARTIAL-2] doc_type now populated on parent EvidenceChunk from the parent node.

Previously-correct items retained:
  - Traverses reverse PARENT_OF edges only
  - Unique parents only (dedup by parent_id)
  - parents ≤ children cap (max_parents = len(child_chunks))
  - provenance="graph" on all parent context chunks
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from graphrag.models.contracts import EvidenceChunk
from graphrag.storage.graph_store import GraphStore


def _parse_source_locator(loc_json: str) -> Dict[str, Any]:
    if not loc_json:
        return {}
    try:
        return json.loads(loc_json)
    except Exception:
        return {"raw": loc_json}


def attach_parent_context(
    graph_store: GraphStore,
    child_chunks: List[EvidenceChunk],
    k_parent: int = 3,
) -> List[EvidenceChunk]:
    """
    For each child chunk, find its parent section via reverse PARENT_OF edges
    and return up to k_parent unique parent EvidenceChunks.

    Rule: parents must never outnumber children.
    """
    parent_chunks: List[EvidenceChunk] = []
    seen_parent_ids: set = set()
    max_parents = len(child_chunks)

    for child in child_chunks:
        if len(parent_chunks) >= max_parents:
            break

        # Use the chunk_id attribute (EvidenceChunk dataclass)
        child_id = child.chunk_id

        parent_edges = graph_store.get_edges_to(child_id, rel_types=["PARENT_OF"])
        parent_edges.sort(key=lambda e: e.get("confidence", 0.0), reverse=True)

        for edge in parent_edges[:k_parent]:
            if len(parent_chunks) >= max_parents:
                break

            parent_id = edge["src_id"]
            if parent_id in seen_parent_ids:
                continue

            parent_node = graph_store.get_node(parent_id)
            if not parent_node or parent_node.get("node_type") != "CHUNK":
                continue

            try:
                extra = json.loads(parent_node.get("extra_json") or "{}")
            except Exception:
                extra = {}

            # [PARTIAL-4] score derived from edge confidence, not hardcoded 0.8
            edge_confidence = edge.get("confidence", 1.0)

            parent_chunks.append(EvidenceChunk(
                chunk_id=parent_node["node_id"],
                chunk_type=extra.get("chunk_type", "parent"),
                text=parent_node.get("text", ""),
                doc_id=parent_node.get("doc_id", ""),
                doc_type=parent_node.get("doc_type", ""),   # [PARTIAL-2]
                section_path=parent_node.get("section_path", ""),
                source_locator=_parse_source_locator(parent_node.get("source_locator_json")),
                module=parent_node.get("module"),
                version=parent_node.get("version"),
                score=edge_confidence,          # structural: 1.0 for PARENT_OF
                confidence=edge_confidence,     # same: PARENT_OF is deterministic
                provenance="graph",
                needs_confirmation=False,
            ))
            seen_parent_ids.add(parent_id)

    return parent_chunks
