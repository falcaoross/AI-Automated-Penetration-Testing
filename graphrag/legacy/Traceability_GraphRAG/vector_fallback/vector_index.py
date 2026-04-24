import argparse
import json
import pathlib
from datetime import datetime, timezone

import duckdb
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


_EMBEDDING_MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"


def _load_child_chunks(db_path: str) -> list[dict]:
    """Return all child CHUNK nodes as dicts with node_id and text."""
    conn = duckdb.connect(db_path, read_only=True)
    rows = conn.execute(
        """
        SELECT node_id, text
        FROM nodes
        WHERE node_type = 'CHUNK'
          AND json_extract_string(extra_json, '$.chunk_type') = 'child'
        ORDER BY node_id
        """
    ).fetchall()
    conn.close()
    return [{"node_id": row[0], "text": row[1]} for row in rows]


def _build_faiss_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    """Build a flat inner-product FAISS index over the embedding matrix."""
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)
    return index


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an offline FAISS vector index over child CHUNK nodes."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the DuckDB database file.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for chunk_index.faiss and embedding_manifest.json.",
    )
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out) / "output"
    index_path    = out_dir / "chunk_index.faiss"
    manifest_path = out_dir / "embedding_manifest.json"

    chunks = _load_child_chunks(args.db)
    if not chunks:
        raise ValueError(
            "vector_index: no child CHUNK nodes found in the graph — "
            "index cannot be built"
        )

    texts    = [c["text"] for c in chunks]
    node_ids = [c["node_id"] for c in chunks]

    model = SentenceTransformer(_EMBEDDING_MODEL_NAME)
    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=False,
        convert_to_numpy=True,
    )

    embeddings = embeddings.astype(np.float32)

    # ---- FIX: L2 normalize embeddings so IndexFlatIP == cosine similarity ----
    embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

    index = _build_faiss_index(embeddings)

    out_dir.mkdir(parents=True, exist_ok=True)

    id_map_path = out_dir / "chunk_index_ids.json"
    id_map_path.write_text(
        json.dumps(node_ids, indent=2), encoding="utf-8"
    )

    faiss.write_index(index, str(index_path))

    manifest = {
        "index_type":      "faiss",
        "node_type":       "CHUNK",
        "chunk_scope":     "child_only",
        "embedding_model": _EMBEDDING_MODEL_NAME,
        "embedding_dim":   int(embeddings.shape[1]),
        "num_vectors":     int(embeddings.shape[0]),
        "source_db":       str(pathlib.Path(args.db).resolve()),
        "created_at":      datetime.now(timezone.utc).isoformat(),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    print(
        f"Vector index built: {int(embeddings.shape[0])} vectors, "
        f"dim={int(embeddings.shape[1])}. "
        f"Written to {out_dir}"
    )


if __name__ == "__main__":
    main()