"""
graphrag/retrieval/anchor_resolver.py
=======================================
"""
from __future__ import annotations

from typing import List

from graphrag.models.contracts import Anchor, QueryInput
from graphrag.storage.graph_store import GraphStore


def resolve_anchors(graph_store: GraphStore, query: QueryInput) -> List[Anchor]:
    # ── Direct lookup by req_id ───────────────────────────────────────────────
    if query.req_id:
        node = graph_store.get_node(query.req_id)
        if node is None:
            raise ValueError(f"Anchor node not found: {query.req_id!r}")
        return [Anchor(
            node_id=node["node_id"],
            node_type=node["node_type"],
            score=1.0,
            provenance="graph",
        )]

    # ── Vector search ─────────────────────────────────────────────────────────
    if not query.query_text:
        raise ValueError("Either req_id or query_text must be provided to resolve anchors")

    # Lazy import – SentenceTransformer not loaded unless this branch runs
    from graphrag.retrieval.vector_fallback import vector_search  # noqa: PLC0415

    # FIX: search CRU (active canonical type) + CHUNK, NOT "REQ" + "CHUNK"
    candidates = vector_search(
        graph_store=graph_store,
        query_text=query.query_text,
        filters=query.filters,
        node_types=["CRU", "CHUNK"],
        top_k=5,
    )

    # Prefer CRU anchors; fall back to CHUNK anchors
    cru_first = sorted(
        candidates,
        key=lambda x: (0 if x["node_type"] == "CRU" else 1, -x.get("score", 0.0)),
    )

    anchors = [
        Anchor(
            node_id=c["node_id"],
            node_type=c["node_type"],
            score=c.get("score", 0.0),
            provenance="vector",
        )
        for c in cru_first
    ]

    if not anchors:
        raise ValueError("No anchors resolved from query_text via vector search")

    return anchors[:5]
