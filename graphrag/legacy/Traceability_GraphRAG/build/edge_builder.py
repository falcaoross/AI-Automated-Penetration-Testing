import json
from datetime import datetime, timezone


# ── Deterministic confidence resolver (constraint 2) ─────────────────────────
# Values are fixed by provenance tier; no inline numeric literals elsewhere.
_CONFIDENCE_BY_PROVENANCE = {
    "document_structure": 1.0,   # PARENT_OF: deterministic section hierarchy
    "pipeline_lineage":   1.0,   # SUPPORTED_BY: same CRU→chunk lineage
}

# Human-readable reasons stored in extra_json.confidence_reason (constraint 1).
_REASON_BY_PROVENANCE = {
    "document_structure": (
        "Deterministic: derived from document section hierarchy in "
        "chunked_crus_with_domain.json"
    ),
    "pipeline_lineage": (
        "Deterministic: derived from CRU→chunk lineage in "
        "chunked_crus_with_domain.json"
    ),
}


def _resolve_confidence(provenance_key: str) -> float:
    return _CONFIDENCE_BY_PROVENANCE[provenance_key]


def _resolve_reason(provenance_key: str) -> str:
    return _REASON_BY_PROVENANCE[provenance_key]


def build_parent_of_edges(graph_store, chunked_crus_path: str) -> dict:
    """Insert PARENT_OF edges (parent CHUNK → child CHUNK) for every section."""
    with open(chunked_crus_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    chunks = data.get("chunks")
    if chunks is None:
        raise ValueError("build_parent_of_edges: top-level 'chunks' array is missing")

    build_ts = datetime.now(timezone.utc)

    edges_created: int = 0
    warnings: list = []
    skipped_edges: list = []

    # Group chunk entries by (doc_id, section_path).
    # doc_id is read from the REQ node of the first CRU in each chunk.
    section_groups: dict = {}  # (doc_id, section_path) -> list of chunk entries

    for chunk_entry in chunks:
        chunk_id = chunk_entry.get("chunk_id", "<unknown>")
        ctx = f"build_parent_of_edges(chunk_id={chunk_id!r})"

        traceability = chunk_entry.get("traceability") or {}
        sections = traceability.get("sections")
        if not sections:
            raise ValueError(f"{ctx}: traceability.sections[] is missing or empty")

        section_path = sections[0] if len(sections) == 1 else " | ".join(sections)

        cru_ids = chunk_entry.get("cru_ids") or []
        if not cru_ids:
            raise ValueError(f"{ctx}: cru_ids[] is missing or empty")

        first_cru_id = cru_ids[0]
        req_node = graph_store.get_node(first_cru_id)
        if req_node is None:
            raise ValueError(
                f"{ctx}: REQ node {first_cru_id!r} not found in Graph Store"
            )

        doc_id = req_node["doc_id"]
        key = (doc_id, section_path)
        section_groups.setdefault(key, []).append(chunk_entry)

    for (doc_id, section_path), entries in section_groups.items():
        ctx = f"build_parent_of_edges(doc_id={doc_id!r}, section_path={section_path!r})"

        rows = graph_store._conn.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'CHUNK'
              AND doc_id = ?
              AND section_path = ?
              AND json_extract_string(extra_json, '$.chunk_type') = 'parent'
            """,
            [doc_id, section_path],
        ).fetchall()

        # PARENT_OF is skipped when no explicit parent CHUNK node exists in the
        # Graph Store. Parent nodes are never synthesized here; they must have
        # been written by node_builder.py from upstream artifacts. If the upstream
        # pipeline produced zero parent nodes for this section, there is no heading
        # text available and no edge can be created deterministically.
        if len(rows) == 0:
            skipped_edges.append({
                "rel_type":     "PARENT_OF",
                "doc_id":       doc_id,
                "section_path": section_path,
                "reason":       "no explicit parent CHUNK node found in Graph Store",
            })
            continue

        if len(rows) != 1:
            raise ValueError(
                f"{ctx}: expected exactly 1 parent CHUNK for "
                f"doc_id={doc_id!r} section_path={section_path!r}, "
                f"found {len(rows)}"
            )

        parent_node_id = rows[0][0]

        child_rows = graph_store._conn.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'CHUNK'
              AND doc_id = ?
              AND section_path = ?
              AND json_extract_string(extra_json, '$.chunk_type') = 'child'
            """,
            [doc_id, section_path],
        ).fetchall()

        child_node_ids = [r[0] for r in child_rows]

        provenance = "document_structure"
        for child_node_id in child_node_ids:
            graph_store.insert_edge({
                "src_id":            parent_node_id,
                "src_type":          "CHUNK",
                "rel_type":          "PARENT_OF",
                "dst_id":            child_node_id,
                "dst_type":          "CHUNK",
                "confidence":        _resolve_confidence(provenance),
                "evidence_chunk_id": None,
                "extra_json":        json.dumps(
                    {"confidence_reason": _resolve_reason(provenance)}
                ),
                "created_at":        build_ts,
            })
            edges_created += 1

    return {
        "edges_created": edges_created,
        "warnings":      warnings,
        "skipped_edges": skipped_edges,
    }


def build_supported_by_edges(graph_store, cru_units_path: str, chunked_crus_path: str) -> dict:
    """Insert SUPPORTED_BY edges (REQ → child CHUNK) for every CRU."""
    with open(cru_units_path, "r", encoding="utf-8") as fh:
        cru_data = json.load(fh)

    crus = cru_data.get("crus")
    if crus is None:
        raise ValueError("build_supported_by_edges: top-level 'crus' array is missing")

    build_ts = datetime.now(timezone.utc)

    edges_created: int = 0
    warnings: list = []
    skipped_edges: list = []

    provenance = "pipeline_lineage"

    for entry in crus:
        cru_id = entry.get("cru_id", "<unknown>")
        ctx = f"build_supported_by_edges(cru_id={cru_id!r})"

        req_node = graph_store.get_node(cru_id)
        if req_node is None:
            raise ValueError(
                f"{ctx}: REQ node {cru_id!r} not found in Graph Store"
            )

        rows = graph_store._conn.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'CHUNK'
              AND json_extract_string(extra_json, '$.chunk_type') = 'child'
              AND json_contains(
                    json_extract(extra_json, '$.source_cru_ids'),
                    json_quote(?)
                  )
            """,
            [cru_id],
        ).fetchall()

        if not rows:
            raise ValueError(
                f"{ctx}: no child CHUNK nodes found for CRU {cru_id!r} — "
                "SUPPORTED_BY edge cannot be created"
            )

        for (child_node_id,) in rows:
            graph_store.insert_edge({
                "src_id":            cru_id,
                "src_type":          "REQ",
                "rel_type":          "SUPPORTED_BY",
                "dst_id":            child_node_id,
                "dst_type":          "CHUNK",
                "confidence":        _resolve_confidence(provenance),
                "evidence_chunk_id": child_node_id,
                "extra_json":        json.dumps(
                    {"confidence_reason": _resolve_reason(provenance)}
                ),
                "created_at":        build_ts,
            })
            edges_created += 1

    # ── Orphan detection (non-blocking) ───────────────────────────────────────

    # a) REQ nodes with zero outgoing SUPPORTED_BY edges.
    req_rows = graph_store._conn.execute(
        "SELECT node_id FROM nodes WHERE node_type = 'REQ'"
    ).fetchall()

    for (req_node_id,) in req_rows:
        outgoing = graph_store._conn.execute(
            """
            SELECT 1 FROM edges
            WHERE src_id = ? AND rel_type = 'SUPPORTED_BY'
            LIMIT 1
            """,
            [req_node_id],
        ).fetchone()
        if outgoing is None:
            warnings.append({
                "type":    "ORPHAN_REQ",
                "node_id": req_node_id,
                "message": f"REQ node {req_node_id!r} has zero outgoing SUPPORTED_BY edges",
            })

    # b) CHUNK (child) nodes with zero incoming SUPPORTED_BY edges.
    child_chunk_rows = graph_store._conn.execute(
        """
        SELECT node_id FROM nodes
        WHERE node_type = 'CHUNK'
          AND json_extract_string(extra_json, '$.chunk_type') = 'child'
        """
    ).fetchall()

    for (chunk_node_id,) in child_chunk_rows:
        incoming = graph_store._conn.execute(
            """
            SELECT 1 FROM edges
            WHERE dst_id = ? AND rel_type = 'SUPPORTED_BY'
            LIMIT 1
            """,
            [chunk_node_id],
        ).fetchone()
        if incoming is None:
            warnings.append({
                "type":    "ORPHAN_CHUNK",
                "node_id": chunk_node_id,
                "message": (
                    f"child CHUNK node {chunk_node_id!r} has zero incoming "
                    "SUPPORTED_BY edges"
                ),
            })

    return {
        "edges_created": edges_created,
        "warnings":      warnings,
        "skipped_edges": skipped_edges,
    }