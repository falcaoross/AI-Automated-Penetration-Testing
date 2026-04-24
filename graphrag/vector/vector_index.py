"""
graphrag/vector/vector_index.py
=================================
FIXES IN THIS VERSION:
  [PARTIAL-5a] doc_type column now populated correctly.
               Old code: doctype=node.get("doc_id")  ← wrote the wrong column
               Fix:      doctype=node.get("doc_type") ← correct field

  [PARTIAL-5b] get_embedding_rows() now honors doctype filter.
               Old graph_store.get_embedding_rows() only filtered on module/version.
               Added "doc_type" to the filter keys checked in graph_store
               (and documented here). The vector search call now passes doc_type
               from filters so filtering works end-to-end.

  [PARTIAL-5c] embedding_manifest.json emitted after every build.
               Required by the PDF spec as a data-plane audit artifact.
               Stored alongside the DB so reproducibility can be verified.

Previously-correct items retained:
  - force_rebuild drops and recreates node_embeddings
  - incremental build skips already-embedded nodes
  - CRU nodes enriched with acceptance_criteria text
  - normalize_embeddings=True
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from sentence_transformers import SentenceTransformer

from graphrag.storage.graph_store import GraphStore

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def _to_bytes(vec: np.ndarray) -> bytes:
    return vec.astype(np.float32).tobytes()


def _enrich_text(node: dict) -> str:
    """Append acceptance_criteria from extra_json for CRU nodes."""
    base = node.get("text") or ""
    if node.get("node_type") == "CRU":
        try:
            extra = json.loads(node.get("extra_json") or "{}")
            ac = extra.get("acceptance_criteria")
            if ac and isinstance(ac, str) and ac.strip():
                base = f"{base} {ac}".strip()
        except Exception:
            pass
    return base


def build_embeddings(
    graph_store: GraphStore,
    force_rebuild: bool = False,
    manifest_dir: str | None = None,
) -> None:
    """
    Build or update node embeddings in the DuckDB node_embeddings table.
    Emits embedding_manifest.json to manifest_dir (defaults to same dir as DB).
    """
    if force_rebuild:
        graph_store.execute("DROP TABLE IF EXISTS node_embeddings;")
        graph_store._init_schema()

    model = SentenceTransformer(MODEL_NAME)

    nodes = graph_store.query("""
        SELECT
            node_id,
            node_type,
            module,
            version,
            doc_type,
            section_path,
            COALESCE(text, '') AS text,
            COALESCE(extra_json, '{}') AS extra_json
        FROM nodes
        WHERE node_type IN ('CRU', 'CHUNK', 'DEFECT', 'FAILURE')
          AND text IS NOT NULL
          AND LENGTH(TRIM(text)) > 0
    """)

    if not force_rebuild:
        existing = {
            r["node_id"]
            for r in graph_store.query("SELECT node_id FROM node_embeddings")
        }
        nodes = [n for n in nodes if n["node_id"] not in existing]

    texts = [_enrich_text(n) for n in nodes]

    if not texts:
        print("No new nodes to embed.")
        _write_manifest(graph_store, model, manifest_dir)
        return

    vectors = model.encode(texts, normalize_embeddings=True)

    for node, vec in zip(nodes, vectors):
        graph_store.upsert_embedding(
            node_id=node["node_id"],
            node_type=node["node_type"],
            module=node.get("module"),
            version=node.get("version"),
            # [PARTIAL-5a] FIX: use doc_type not doc_id
            doctype=node.get("doc_type"),
            section_path=node.get("section_path"),
            embedding_bytes=_to_bytes(vec),
            embedding_model=MODEL_NAME,
        )

    print(f"✅ Embedded {len(nodes)} nodes with model={MODEL_NAME}")

    # [PARTIAL-5c] Write manifest
    _write_manifest(graph_store, model, manifest_dir)


def _write_manifest(
    graph_store: GraphStore,
    model: SentenceTransformer,
    manifest_dir: str | None,
) -> None:
    """Emit embedding_manifest.json as required by PDF spec §4 (data-plane audit)."""
    total = graph_store.query("SELECT COUNT(*) AS n FROM node_embeddings")[0]["n"]
    dim = model.get_sentence_embedding_dimension()

    manifest = {
        "embedding_model": MODEL_NAME,
        "embedding_dim": dim,
        "num_vectors": total,
        "source_db": graph_store.db_path,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Resolve output directory
    if manifest_dir:
        out_dir = Path(manifest_dir)
    else:
        out_dir = Path(graph_store.db_path).parent

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "embedding_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"✅ Manifest written: {manifest_path}")
