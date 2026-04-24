"""
graphrag/retrieval/graph_retriever.py
=======================================
FIXES IN THIS VERSION:
  [BUG-1] CRITICAL – provenance="graph" now stamped on every candidate node
          returned from graph traversal.
          
          Root cause of the false MISSING_EVIDENCE warning:
            graph_retriever returned nodes with no "provenance" key.
            context_pack_builder then filtered evidence_nodes by
            n.get("provenance") == "graph" before calling build_warnings().
            That filter returned [] (empty), which triggered MISSING_EVIDENCE
            even though evidence was present.
          
          Fix: add "provenance": "graph" to every best_candidate dict so
          downstream code can trust the key is always present.

  [PARTIAL-1] doc_type boost added alongside module/version boosts.
          Exact match on doc_type (e.g. "SRS") gets +15% score boost.

Previously-correct items retained unchanged:
  - path_confidence = PRODUCT(edge.confidence) × HOP_PENALTY^hops
  - filter boost for module (+20%) and version (+10%)
  - child-chunk-only evidence collection
  - ranking by final_score DESC before return
  - forward + reverse traversal using TASK_RELATIONS whitelist
"""
from __future__ import annotations

import json
from collections import deque
from typing import Dict, List

from graphrag.models.contracts import Anchor
from graphrag.retrieval.query_router import TASK_RELATIONS
from graphrag.storage.graph_store import GraphStore

HOP_PENALTY = 0.9


def graph_retrieve(
    graph_store: GraphStore,
    anchors: List[Anchor],
    task: str,
    max_hops: int = 2,
    filters: dict | None = None,
) -> Dict[str, List]:
    """
    Task-specific k-hop BFS with path-confidence scoring.

    Returns:
      {
        "evidence_nodes": [... with score, path_confidence, provenance, path_edges],
        "trace_paths":    [...],
        "related_nodes":  [...],
      }
    """
    filters = filters or {}
    policy = TASK_RELATIONS.get(task, {})
    forward_rels = set(policy.get("forward", []))
    reverse_rels = set(policy.get("reverse", []))

    best_score: Dict[str, float] = {}
    best_candidate: Dict[str, dict] = {}
    related: List[dict] = []
    visited_edges: set = set()

    queue: deque = deque()
    for anchor in anchors:
        queue.append((anchor.node_id, 0, 1.0, []))

    queued: set = {a.node_id for a in anchors}

    while queue:
        node_id, depth, path_conf, path_edges = queue.popleft()

        if depth > max_hops:
            continue

        node = graph_store.get_node(node_id)
        if node is None:
            continue

        # ── Score and collect child CHUNK evidence ────────────────────────────
        if node.get("node_type") == "CHUNK" and depth > 0:
            try:
                extra = json.loads(node.get("extra_json") or "{}")
            except Exception:
                extra = {}

            if extra.get("chunk_type") == "child":
                hop_penalty = HOP_PENALTY ** depth

                # Filter boosts
                boost = 1.0
                if filters.get("module") and node.get("module") == filters["module"]:
                    boost *= 1.2
                if filters.get("version") and node.get("version") == filters["version"]:
                    boost *= 1.1
                # [PARTIAL-1] doc_type boost
                if filters.get("doc_type") and node.get("doc_type") == filters["doc_type"]:
                    boost *= 1.15

                final_score = round(path_conf * hop_penalty * boost, 6)

                if final_score > best_score.get(node_id, -1):
                    best_score[node_id] = final_score
                    # [BUG-1] FIX: stamp provenance="graph" on every graph candidate
                    best_candidate[node_id] = {
                        **node,
                        "score": final_score,
                        "path_confidence": round(path_conf, 6),
                        "provenance": "graph",          # ← THIS WAS MISSING
                        "needs_confirmation": False,
                        "hops": depth,
                        "path_edges": path_edges,
                    }

        if depth >= max_hops:
            continue

        # ── Forward traversal ─────────────────────────────────────────────────
        for edge in graph_store.get_edges_from(node_id, rel_types=list(forward_rels)):
            dst_id = edge["dst_id"]
            ekey = (node_id, edge["rel_type"], dst_id)
            if ekey in visited_edges:
                continue
            visited_edges.add(ekey)

            new_conf = path_conf * edge["confidence"]
            new_path = path_edges + [{
                "src": node_id,
                "rel": edge["rel_type"],
                "dst": dst_id,
                "conf": edge["confidence"],
            }]
            if dst_id not in queued or new_conf > best_score.get(dst_id, -1):
                queued.add(dst_id)
                queue.append((dst_id, depth + 1, new_conf, new_path))

            related.append({
                "node_type": edge["dst_type"],
                "node_id": dst_id,
                "relation": edge["rel_type"],
            })

        # ── Reverse traversal ─────────────────────────────────────────────────
        for edge in graph_store.get_edges_to(node_id, rel_types=list(reverse_rels)):
            src_id = edge["src_id"]
            ekey = (src_id, edge["rel_type"], node_id, "rev")
            if ekey in visited_edges:
                continue
            visited_edges.add(ekey)

            new_conf = path_conf * edge["confidence"]
            new_path = path_edges + [{
                "src": src_id,
                "rel": edge["rel_type"],
                "dst": node_id,
                "conf": edge["confidence"],
                "direction": "reverse",
            }]
            if src_id not in queued:
                queued.add(src_id)
                queue.append((src_id, depth + 1, new_conf, new_path))

            related.append({
                "node_type": edge["src_type"],
                "node_id": src_id,
                "relation": edge["rel_type"],
            })

    # ── Rank child evidence by score DESC ─────────────────────────────────────
    ranked = sorted(best_candidate.values(), key=lambda x: x["score"], reverse=True)

    trace_paths = [
        {
            "why": f"evidence via task={task}",
            "path": c["path_edges"],
            "path_confidence": c["path_confidence"],
        }
        for c in ranked
    ]

    return {
        "evidence_nodes": ranked,
        "trace_paths": trace_paths,
        "related_nodes": related[:20],
    }
