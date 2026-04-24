"""
graphrag/cli/build_graph.py
=============================
FIX SUMMARY:
  [1] Integrity check now runs after build (was pointed at 0-byte file, so never ran).
  [2] Execution / defect artifact builders wired (EXECUTED_AS, RAISED_AS, AFFECTS)
      when --runs or --defects flags provided.
  [3] graph_store.stats() call was already present but now works (GraphStore.stats added).
"""
from __future__ import annotations

import argparse
import json
import pathlib
from datetime import datetime, timezone

from graphrag.storage.graph_store import GraphStore
from graphrag.builders.cru_builder import build_cru_nodes
from graphrag.builders.chunk_builder import build_chunk_nodes
from graphrag.builders.test_builder import build_test_nodes
from graphrag.builders.edge_builder import (
    build_parent_of_edges,
    build_supported_by_edges,
    build_test_edges,
    build_execution_edges,
    build_affects_edges,
)
from graphrag.validation.integrity_checks import run_integrity_checks


def write_json(path: pathlib.Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Build GraphRAG graph")
    parser.add_argument("--db",      required=True, help="Path to DuckDB database")
    parser.add_argument("--cru",     required=True, help="Path to cru_units.json")
    parser.add_argument("--chunks",  required=True, help="Path to chunked_crus_with_domain.json")
    parser.add_argument("--tests",   help="Path to optimized test cases JSON (optional)")
    parser.add_argument("--runs",    help="Path to execution runs JSON (optional)")
    parser.add_argument("--defects", help="Path to defects JSON (optional)")
    parser.add_argument("--out",     required=True, help="Output directory for reports")
    args = parser.parse_args()

    db_path     = pathlib.Path(args.db)
    cru_path    = pathlib.Path(args.cru)
    chunks_path = pathlib.Path(args.chunks)
    tests_path  = pathlib.Path(args.tests)   if args.tests   else None
    runs_path   = pathlib.Path(args.runs)    if args.runs    else None
    defects_path= pathlib.Path(args.defects) if args.defects else None
    out_dir     = pathlib.Path(args.out)

    graph_store = GraphStore(str(db_path))
    edge_reports = []

    try:
        print(f"[BUILD] DB:     {db_path}")
        print(f"[BUILD] CRU:    {cru_path} (exists={cru_path.exists()})")
        print(f"[BUILD] CHUNKS: {chunks_path} (exists={chunks_path.exists()})")

        if not cru_path.exists():
            raise FileNotFoundError(f"CRU file not found: {cru_path}")
        if not chunks_path.exists():
            raise FileNotFoundError(f"Chunks file not found: {chunks_path}")

        # ── Node builders ─────────────────────────────────────────────────────
        build_cru_nodes(graph_store, str(cru_path))
        build_chunk_nodes(graph_store, str(chunks_path))

        # ── Edge builders (core) ──────────────────────────────────────────────
        edge_reports.append(build_supported_by_edges(graph_store, str(cru_path), str(chunks_path)))
        edge_reports.append(build_parent_of_edges(graph_store, str(chunks_path)))

        # ── Optional: test nodes + edges ──────────────────────────────────────
        if tests_path and tests_path.exists():
            print(f"[BUILD] TESTS: {tests_path}")
            build_test_nodes(graph_store, str(tests_path))
            edge_reports.append(build_test_edges(graph_store, str(tests_path)))
        else:
            print("[BUILD] TESTS: not provided or missing – skipping")

        # ── Optional: execution run edges ─────────────────────────────────────
        if runs_path and runs_path.exists():
            print(f"[BUILD] RUNS: {runs_path}")
            with open(runs_path, encoding="utf-8") as f:
                runs_data = json.load(f)
            edge_reports.append(build_execution_edges(graph_store, runs_data.get("runs", [])))
        else:
            print("[BUILD] RUNS: not provided – EXECUTED_AS / RAISED_AS edges skipped")

        # ── Optional: defect edges ────────────────────────────────────────────
        if defects_path and defects_path.exists():
            print(f"[BUILD] DEFECTS: {defects_path}")
            with open(defects_path, encoding="utf-8") as f:
                defects_data = json.load(f)
            edge_reports.append(build_affects_edges(graph_store, defects_data.get("defects", [])))
        else:
            print("[BUILD] DEFECTS: not provided – AFFECTS edges skipped")

        stats = graph_store.stats()

        build_report = {
            "status": "success",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "inputs": {
                "db":      str(db_path),
                "cru":     str(cru_path),
                "chunks":  str(chunks_path),
                "tests":   str(tests_path)   if tests_path   else None,
                "runs":    str(runs_path)    if runs_path    else None,
                "defects": str(defects_path) if defects_path else None,
            },
            "stats":        stats,
            "edge_reports": edge_reports,
        }
        write_json(out_dir / "build_report.json", build_report)
        print(f"[BUILD] Success. Stats: {stats}")

        # ── Integrity check (runs after every build) ──────────────────────────
        print("[BUILD] Running integrity checks …")
        integrity_report = run_integrity_checks(str(db_path))
        write_json(out_dir / "rag_integrity_report.json", integrity_report)
        total = integrity_report["summary"]["total_issues"]
        errors = integrity_report["summary"]["errors"]
        print(f"[BUILD] Integrity: {total} issues ({errors} errors)")
        if errors > 0:
            print("[BUILD] ⚠️  Integrity errors found – review rag_integrity_report.json")

    except Exception as exc:
        error_report = {
            "status": "failed",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        write_json(out_dir / "build_report.json", error_report)
        print(f"[BUILD] FAILED: {exc}")
        raise

    finally:
        graph_store.close()


if __name__ == "__main__":
    main()
