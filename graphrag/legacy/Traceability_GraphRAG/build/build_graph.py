import argparse
import json
import pathlib
from datetime import datetime, timezone

from ..storage.graph_store import GraphStore
from .node_builder import build_req_nodes, build_chunk_nodes
from .edge_builder import build_parent_of_edges, build_supported_by_edges


def _collect_graph_stats(graph_store: GraphStore) -> dict:
    """Query the graph for node and edge counts."""
    total_nodes = graph_store._conn.execute(
        "SELECT COUNT(*) FROM nodes"
    ).fetchone()[0]

    node_type_rows = graph_store._conn.execute(
        "SELECT node_type, COUNT(*) FROM nodes GROUP BY node_type ORDER BY node_type"
    ).fetchall()
    nodes_by_type = {row[0]: row[1] for row in node_type_rows}

    total_edges = graph_store._conn.execute(
        "SELECT COUNT(*) FROM edges"
    ).fetchone()[0]

    edge_type_rows = graph_store._conn.execute(
        "SELECT rel_type, COUNT(*) FROM edges GROUP BY rel_type ORDER BY rel_type"
    ).fetchall()
    edges_by_rel_type = {row[0]: row[1] for row in edge_type_rows}

    return {
        "total_nodes":       total_nodes,
        "nodes_by_type":     nodes_by_type,
        "total_edges":       total_edges,
        "edges_by_rel_type": edges_by_rel_type,
    }


def _write_report(out_dir: pathlib.Path, report: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "build_report.json"
    report_path.write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the QA-native Traceability GraphRAG graph."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the DuckDB database file.",
    )
    parser.add_argument(
        "--cru",
        required=True,
        help="Path to cru_units.json.",
    )
    parser.add_argument(
        "--chunks",
        required=True,
        help="Path to chunked_crus_with_domain.json.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for build_report.json.",
    )
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out)

    try:
        # ── Step 1: Initialize GraphStore ─────────────────────────────────────
        graph_store = GraphStore(args.db)

        # ── Step 2: Build nodes ───────────────────────────────────────────────
        build_req_nodes(graph_store, args.cru)
        build_chunk_nodes(graph_store, args.chunks)

        # ── Step 3: Build edges; capture returned metadata ────────────────────
        result_supported_by = build_supported_by_edges(graph_store, args.cru, args.chunks)
        result_parent_of    = build_parent_of_edges(graph_store, args.chunks)

        # ── Step 4: Collect graph statistics ─────────────────────────────────
        stats = _collect_graph_stats(graph_store)

        # ── Step 5: Aggregate warnings and skipped_edges from edge builder ────
        warnings = (
            result_supported_by.get("warnings", [])
            + result_parent_of.get("warnings", [])
        )
        skipped_edges = (
            result_supported_by.get("skipped_edges", [])
            + result_parent_of.get("skipped_edges", [])
        )
        edges_created = (
            result_supported_by.get("edges_created", 0)
            + result_parent_of.get("edges_created", 0)
        )

        # ── Step 6: Write build_report.json ───────────────────────────────────
        report = {
            "status":            "success",
            "timestamp_utc":     datetime.now(timezone.utc).isoformat(),
            "total_nodes":       stats["total_nodes"],
            "nodes_by_type":     stats["nodes_by_type"],
            "total_edges":       stats["total_edges"],
            "edges_by_rel_type": stats["edges_by_rel_type"],
            "edges_created":     edges_created,
            "warnings":          warnings,
            "skipped_edges":     skipped_edges,
        }
        _write_report(out_dir, report)

        # ── Step 7: Print success ─────────────────────────────────────────────
        print(f"Graph build complete. Report written to {out_dir / 'build_report.json'}")

    except Exception as exc:
        failure_report = {
            "status": "failed",
            "error":  str(exc),
        }
        _write_report(out_dir, failure_report)
        raise


if __name__ == "__main__":
    main()