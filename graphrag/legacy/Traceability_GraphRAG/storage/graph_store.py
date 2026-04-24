import pathlib
import duckdb


_NODE_REQUIRED = {"node_id", "node_type", "module", "version", "doc_id", "doc_type", "section_path", "created_at"}
_NODE_OPTIONAL = {"text", "source_locator_json", "extra_json"}

_EDGE_REQUIRED = {"src_id", "src_type", "rel_type", "dst_id", "dst_type", "confidence", "created_at"}
_EDGE_OPTIONAL = {"evidence_chunk_id", "extra_json"}


class GraphStore:
    """Persistence and retrieval adapter for the GraphRAG node and edge store."""

    def __init__(self, db_path: str):
        self._conn = duckdb.connect(db_path)
        _schema_path = pathlib.Path(__file__).with_name("schema.sql")
        self._conn.execute(_schema_path.read_text())

    # ── Write ──────────────────────────────────────────────────────────────────

    def insert_node(self, node: dict):
        """Insert a node row. Does nothing if node_id already exists."""
        missing = _NODE_REQUIRED - node.keys()
        if missing:
            raise ValueError(f"insert_node: missing required fields: {missing}")

        if self.node_exists(node["node_id"]):
            return

        self._conn.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, text, module, version, doc_id, doc_type,
                section_path, source_locator_json, extra_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                node["node_id"],
                node["node_type"],
                node.get("text"),
                node["module"],
                node["version"],
                node["doc_id"],
                node["doc_type"],
                node["section_path"],
                node.get("source_locator_json"),
                node.get("extra_json"),
                node["created_at"],
            ],
        )

    def insert_edge(self, edge: dict):
        """Insert an edge row. Does nothing if (src_id, rel_type, dst_id) already exists."""
        missing = _EDGE_REQUIRED - edge.keys()
        if missing:
            raise ValueError(f"insert_edge: missing required fields: {missing}")

        if self.edge_exists(edge["src_id"], edge["rel_type"], edge["dst_id"]):
            return

        self._conn.execute(
            """
            INSERT INTO edges (
                src_id, src_type, rel_type, dst_id, dst_type,
                confidence, evidence_chunk_id, extra_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                edge["src_id"],
                edge["src_type"],
                edge["rel_type"],
                edge["dst_id"],
                edge["dst_type"],
                edge["confidence"],
                edge.get("evidence_chunk_id"),
                edge.get("extra_json"),
                edge["created_at"],
            ],
        )

    # ── Read ───────────────────────────────────────────────────────────────────

    def get_node(self, node_id: str) -> dict | None:
        """Return the node row as a dict, or None if not found."""
        result = self._conn.execute(
            "SELECT * FROM nodes WHERE node_id = ?", [node_id]
        ).fetchone()
        if result is None:
            return None
        cols = [desc[0] for desc in self._conn.description]
        return dict(zip(cols, result))

    def node_exists(self, node_id: str) -> bool:
        """Return True if a node with the given node_id exists."""
        result = self._conn.execute(
            "SELECT 1 FROM nodes WHERE node_id = ? LIMIT 1", [node_id]
        ).fetchone()
        return result is not None

    def get_edges_from(self, src_id: str) -> list[dict]:
        """Return all edge rows where src_id matches."""
        rows = self._conn.execute(
            "SELECT * FROM edges WHERE src_id = ?", [src_id]
        ).fetchall()
        cols = [desc[0] for desc in self._conn.description]
        return [dict(zip(cols, row)) for row in rows]

    def get_edges_to(self, dst_id: str) -> list[dict]:
        """Return all edge rows where dst_id matches."""
        rows = self._conn.execute(
            "SELECT * FROM edges WHERE dst_id = ?", [dst_id]
        ).fetchall()
        cols = [desc[0] for desc in self._conn.description]
        return [dict(zip(cols, row)) for row in rows]

    def edge_exists(self, src_id: str, rel_type: str, dst_id: str) -> bool:
        """Return True if an edge with the given (src_id, rel_type, dst_id) exists."""
        result = self._conn.execute(
            "SELECT 1 FROM edges WHERE src_id = ? AND rel_type = ? AND dst_id = ? LIMIT 1",
            [src_id, rel_type, dst_id],
        ).fetchone()
        return result is not None