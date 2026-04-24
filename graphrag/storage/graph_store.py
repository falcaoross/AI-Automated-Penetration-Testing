"""
graphrag/storage/graph_store.py
================================
FIXES IN THIS VERSION (on top of previous round's fixes):
  [PARTIAL-5b] get_embedding_rows() now honors "doc_type" in filters.
               Old version only filtered on module and version; doc_type was
               silently ignored, making doc-type-constrained vector search
               unreliable.

All previous fixes retained:
  - auto-schema init, insert_node, insert_edge, node_exists, edge_exists, stats
  - confidence_reason enforcement in insert_edge
  - INFERRED_SUPPORTED_BY rejection
  - get_edges_from / get_edges_to with optional rel_types list
  - upsert_embedding with correct column names (embedding_bytes, doc_type)
  - ON CONFLICT … DO NOTHING / DO UPDATE (DuckDB syntax)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class GraphStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        if not _SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema file not found: {_SCHEMA_PATH}")
        self.conn.execute(_SCHEMA_PATH.read_text())

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------
    def insert_node(self, node: dict) -> None:
        required = {"node_id", "node_type"}
        missing = required - node.keys()
        if missing:
            raise ValueError(f"insert_node: missing required fields {missing}")

        extra = node.get("extra_json")
        if isinstance(extra, dict):
            extra = json.dumps(extra)

        self.conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, title, text, module, version, doc_id,
                 doc_type, section_path, source_locator_json, extra_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (node_id) DO UPDATE SET
                node_type           = excluded.node_type,
                title               = excluded.title,
                text                = excluded.text,
                module              = excluded.module,
                version             = excluded.version,
                doc_id              = excluded.doc_id,
                doc_type            = excluded.doc_type,
                section_path        = excluded.section_path,
                source_locator_json = excluded.source_locator_json,
                extra_json          = excluded.extra_json
            """,
            [
                node["node_id"], node["node_type"],
                node.get("title"), node.get("text"),
                node.get("module"), node.get("version"),
                node.get("doc_id"), node.get("doc_type"),
                node.get("section_path"), node.get("source_locator_json"),
                extra,
            ],
        )

    def insert_edge(self, edge: dict) -> None:
        required = {"src_id", "src_type", "rel_type", "dst_id", "dst_type", "confidence"}
        missing = required - edge.keys()
        if missing:
            raise ValueError(f"insert_edge: missing required fields {missing}")

        if edge["rel_type"] == "INFERRED_SUPPORTED_BY" and not edge.get("_allow_persist_inferred"):
            raise ValueError(
                "INFERRED_SUPPORTED_BY must not be persisted without human approval."
            )

        extra = edge.get("extra_json")
        if isinstance(extra, dict):
            if "confidence_reason" not in extra:
                raise ValueError(
                    f"insert_edge: extra_json must include 'confidence_reason' "
                    f"({edge['src_id']} --{edge['rel_type']}--> {edge['dst_id']})"
                )
            extra = json.dumps(extra)
        elif isinstance(extra, str):
            try:
                parsed = json.loads(extra)
                if "confidence_reason" not in parsed:
                    raise ValueError(
                        f"insert_edge: extra_json must include 'confidence_reason' "
                        f"({edge['src_id']} --{edge['rel_type']}--> {edge['dst_id']})"
                    )
            except json.JSONDecodeError:
                pass
        else:
            raise ValueError("insert_edge: extra_json is required and must be a dict or JSON string")

        self.conn.execute(
            """
            INSERT INTO edges
                (src_id, src_type, rel_type, dst_id, dst_type, confidence,
                 evidence_chunk_id, extra_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (src_id, rel_type, dst_id) DO NOTHING
            """,
            [
                edge["src_id"], edge["src_type"], edge["rel_type"],
                edge["dst_id"], edge["dst_type"], float(edge["confidence"]),
                edge.get("evidence_chunk_id"), extra,
            ],
        )

    # ------------------------------------------------------------------
    # Existence checks
    # ------------------------------------------------------------------
    def node_exists(self, node_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM nodes WHERE node_id = ?", [node_id]
        ).fetchone() is not None

    def edge_exists(self, src_id: str, rel_type: str, dst_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM edges WHERE src_id=? AND rel_type=? AND dst_id=?",
            [src_id, rel_type, dst_id],
        ).fetchone() is not None

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------
    def query(self, sql: str, params=None) -> List[Dict[str, Any]]:
        params = params or []
        rows = self.conn.execute(sql, params).fetchall()
        cols = [d[0] for d in self.conn.description]
        return [dict(zip(cols, row)) for row in rows]

    def execute(self, sql: str, params=None):
        self.conn.execute(sql, params or [])

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        rows = self.query("SELECT * FROM nodes WHERE node_id = ?", [node_id])
        return rows[0] if rows else None

    def get_edges_from(self, node_id: str, rel_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if rel_types:
            ph = ",".join("?" * len(rel_types))
            return self.query(
                f"SELECT * FROM edges WHERE src_id=? AND rel_type IN ({ph})",
                [node_id, *rel_types],
            )
        return self.query("SELECT * FROM edges WHERE src_id=?", [node_id])

    def get_edges_to(self, node_id: str, rel_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if rel_types:
            ph = ",".join("?" * len(rel_types))
            return self.query(
                f"SELECT * FROM edges WHERE dst_id=? AND rel_type IN ({ph})",
                [node_id, *rel_types],
            )
        return self.query("SELECT * FROM edges WHERE dst_id=?", [node_id])

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        node_rows = self.conn.execute(
            "SELECT node_type, COUNT(*) FROM nodes GROUP BY node_type"
        ).fetchall()
        edge_rows = self.conn.execute(
            "SELECT rel_type, COUNT(*) FROM edges GROUP BY rel_type"
        ).fetchall()
        emb_count = self.conn.execute(
            "SELECT COUNT(*) FROM node_embeddings"
        ).fetchone()[0]
        return {
            "nodes": {r[0]: r[1] for r in node_rows},
            "edges": {r[0]: r[1] for r in edge_rows},
            "embeddings": emb_count,
        }

    # ------------------------------------------------------------------
    # Embedding helpers
    # ------------------------------------------------------------------
    def upsert_embedding(
        self,
        node_id: str,
        node_type: str,
        module: Optional[str],
        version: Optional[str],
        doctype: Optional[str],
        section_path: Optional[str],
        embedding_bytes: bytes,
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO node_embeddings
                (node_id, node_type, module, version, doc_type, section_path,
                 embedding_model, embedding_bytes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (node_id) DO UPDATE SET
                embedding_bytes = excluded.embedding_bytes,
                embedding_model = excluded.embedding_model,
                doc_type        = excluded.doc_type
            """,
            [node_id, node_type, module, version, doctype, section_path,
             embedding_model, embedding_bytes],
        )

    def get_embedding_rows(
        self,
        filters: Optional[Dict[str, str]] = None,
        node_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        filters = filters or {}
        node_types = node_types or []
        clauses, params = [], []

        if node_types:
            ph = ",".join("?" * len(node_types))
            clauses.append(f"node_type IN ({ph})")
            params.extend(node_types)

        # [PARTIAL-5b] FIX: doc_type now included in filter keys
        for key in ("module", "version", "doc_type"):
            if key == "doc_type":
                val = filters.get("doc_type") or filters.get("doctype")
            else:
                val = filters.get(key)
            if val:
                clauses.append(f"{key} = ?")
                params.append(val)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query(f"SELECT * FROM node_embeddings {where}", params)

    # ------------------------------------------------------------------
    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
