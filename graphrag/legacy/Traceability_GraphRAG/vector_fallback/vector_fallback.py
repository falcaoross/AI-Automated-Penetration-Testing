import duckdb
import json
import pathlib

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


_EMBEDDING_MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"
_model: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(_EMBEDDING_MODEL_NAME)
    return _model


def vector_fallback(
    query_text: str,
    k: int,
    confidence_threshold: float,
    vector_dir: str,
) -> dict:
    warnings: list = []
    evidence_chunks: list = []

    warnings.append({
        "type":    "VECTOR_FALLBACK_USED",
        "message": "Graph retrieval was insufficient; vector fallback was invoked.",
    })

    base = pathlib.Path(vector_dir) / "output"
    index_path    = base / "chunk_index.faiss"
    id_map_path   = base / "chunk_index_ids.json"
    manifest_path = base / "embedding_manifest.json"

    index    = faiss.read_index(str(index_path))
    node_ids = json.loads(id_map_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    db_path = manifest["source_db"]
    db_conn = duckdb.connect(db_path, read_only=True)

    model = _get_model()
    query_embedding = model.encode(
        [query_text],
        batch_size=1,
        show_progress_bar=False,
        convert_to_numpy=True,
    ).astype(np.float32)

    norm = np.linalg.norm(query_embedding, axis=1, keepdims=True)
    query_embedding = query_embedding / norm

    scores, indices = index.search(query_embedding, k)

    scores   = scores[0]
    indices  = indices[0]

    valid = [(float(s), int(i)) for s, i in zip(scores, indices) if i != -1]

    if not valid:
        warnings.append({
            "type":    "VECTOR_NO_MATCHES",
            "message": "Vector search returned no valid matches.",
        })
        return {
            "evidence_chunks":    evidence_chunks,
            "provenance":         "vector",
            "warnings":           warnings,
            "confidence_floor":   confidence_threshold,
        }

    _NODE_FIELDS = (
        "node_id", "node_type", "text", "module", "version",
        "doc_id", "doc_type", "section_path", "source_locator_json",
    )

    for score, ordinal in valid:
        node_id = node_ids[ordinal]

        row = db_conn.execute(
            f"SELECT {', '.join(_NODE_FIELDS)} FROM nodes WHERE node_id = ? LIMIT 1",
            [node_id],
        ).fetchone()

        if row is None:
            raise ValueError(
                f"vector_fallback: node_id {node_id!r} not found in graph store"
            )

        node_data = dict(zip(_NODE_FIELDS, row))

        for field in _NODE_FIELDS:
            if node_data.get(field) is None:
                raise ValueError(
                    f"vector_fallback: required field {field!r} is None "
                    f"for node_id {node_id!r}"
                )

        record = {
            "node_id":             node_data["node_id"],
            "node_type":           node_data["node_type"],
            "text":                node_data["text"],
            "module":              node_data["module"],
            "version":             node_data["version"],
            "doc_id":              node_data["doc_id"],
            "doc_type":            node_data["doc_type"],
            "section_path":        node_data["section_path"],
            "source_locator_json": node_data["source_locator_json"],
            "confidence":          score,
            "confidence_reason":   (
                f"Confidence derived from vector cosine similarity score {score:.4f} "
                "via FAISS IndexFlatIP over L2-normalized embeddings"
            ),
            "provenance":          "vector",
            "similarity_score":    score,
        }
        evidence_chunks.append(record)

        if score < confidence_threshold:
            warnings.append({
                "type":    "LOW_CONFIDENCE_VECTOR",
                "node_id": node_id,
                "score":   score,
                "message": (
                    f"Chunk {node_id!r} has score {score:.4f} below "
                    f"confidence_threshold {confidence_threshold}"
                ),
            })

    return {
        "evidence_chunks":  evidence_chunks,
        "provenance":       "vector",
        "warnings":         warnings,
        "confidence_floor": confidence_threshold,
    }