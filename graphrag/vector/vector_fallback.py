"""
graphrag/vector/vector_fallback.py
=====================================
FIX SUMMARY:
  [1] Implemented real cosine-similarity search using stored float32 embeddings.
      Old version: called non-existent DuckDB similarity() function, joined wrong
      table name (nodeembeddings vs node_embeddings), referenced a non-existent
      query_embedding SQL column, and returned [] unconditionally.
  [2] SentenceTransformer model cached at module level (_MODEL singleton) so
      repeated calls within a query session don't reload weights each time.
  [3] node_types and filters parameters added to match retrieval/vector_fallback.py
      call signature (node_types=["CRU","CHUNK"], filters={module, version}).
  [4] 'REQ' removed from default node_types; 'CRU' is the active canonical type.
  [5] Returns List[dict] with node_id, node_type, score – same shape as what
      merge_graph_and_vector() and anchor_resolver expect.
  [6] Uses graph_store.get_embedding_rows() helper instead of a raw SQL query,
      fixing the column name mismatch: stored column is 'embedding_bytes',
      not 'embedding'. Raw query would have raised a BinderException.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np

from graphrag.storage.graph_store import GraphStore
from graphrag.vector.vector_index import MODEL_NAME

# ── Model singleton — loaded once per process ─────────────────────────────────
_MODEL = None


def _get_model():
    global _MODEL
    if _MODEL is None:
        from sentence_transformers import SentenceTransformer
        _MODEL = SentenceTransformer(MODEL_NAME)
    return _MODEL


# ── Core search ───────────────────────────────────────────────────────────────

def vector_fallback(
    graph_store: GraphStore,
    query_text: str,
    top_k: int = 10,
    node_types: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Cosine-similarity search over node_embeddings table.

    Returns list of dicts: {node_id, node_type, module, version, score}
    sorted by score DESC, capped at k results.

    Both query and stored embeddings are L2-normalised (done in vector_index.py),
    so cosine similarity reduces to a dot product — no division needed.
    """
    active_types = node_types or ["CRU", "CHUNK", "DEFECT", "FAILURE"]
    filters = filters or {}

    # ── 1. Encode query ───────────────────────────────────────────────────────
    model = _get_model()
    query_vec = model.encode(
        [query_text], normalize_embeddings=True
    )[0].astype(np.float32)

    # ── 2. Load stored embeddings via GraphStore helper (handles filtering) ──────
    rows = graph_store.get_embedding_rows(
        filters=filters,
        node_types=active_types,
    )

    if not rows:
        return []

    # get_embedding_rows returns all columns including embedding_bytes
    filtered = rows

    # ── 4. Vectorised cosine similarity (dot product on normalised vecs) ───────
    emb_matrix = np.stack(
        [np.frombuffer(r["embedding_bytes"], dtype=np.float32) for r in filtered]
    )                                           # shape: (N, dim)
    scores = emb_matrix @ query_vec            # shape: (N,)

    # ── 5. Rank and return top-k ──────────────────────────────────────────────
    top_indices = np.argsort(scores)[::-1][:top_k]

    results = []
    for idx in top_indices:
        row = filtered[idx]
        results.append({
            "node_id":   row["node_id"],
            "node_type": row["node_type"],
            "module":    row.get("module"),
            "version":   row.get("version"),
            "score":     float(scores[idx]),
        })

    return results