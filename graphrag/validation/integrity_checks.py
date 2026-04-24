"""
graphrag/validation/integrity_checks.py
=========================================
FIX: Was 0 bytes. Ported from legacy/Traceability_GraphRAG/build/integrity_checks.py
and updated for the active schema (CRU instead of REQ, confidence_reason required).

Checks performed:
  ORPHAN_REQ            – CRU nodes with no SUPPORTED_BY edges
  ORPHAN_CHUNK_CHILD    – child CHUNK nodes with no incoming SUPPORTED_BY or PARENT_OF edges
  CONFIDENCE_RANGE      – any edge with confidence outside [0.0, 1.0]
  CONFIDENCE_REASON_MISSING – any edge whose extra_json lacks 'confidence_reason'
  FORBIDDEN_INFERRED_EDGE   – any persisted INFERRED_SUPPORTED_BY edge (must not exist)
  PARENT_OF_CONFIDENCE  – PARENT_OF edges should have confidence = 1.0
  PARENT_SUPPORTED_BY   – CRU→parent CHUNK SUPPORTED_BY edges (architecture violation)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

from graphrag.storage.graph_store import GraphStore


SEVERITY = {
    "ORPHAN_REQ": "warning",
    "ORPHAN_CHUNK_CHILD": "warning",
    "CONFIDENCE_RANGE": "error",
    "CONFIDENCE_REASON_MISSING": "error",
    "FORBIDDEN_INFERRED_EDGE": "error",
    "PARENT_OF_CONFIDENCE": "warning",
    "PARENT_SUPPORTED_BY": "error",
}


def _check_orphan_reqs(graph_store: GraphStore) -> list[dict]:
    issues = []
    cru_nodes = graph_store.query("SELECT node_id FROM nodes WHERE node_type = 'CRU'")
    for row in cru_nodes:
        cru_id = row["node_id"]
        edges = graph_store.get_edges_from(cru_id, rel_types=["SUPPORTED_BY"])
        if not edges:
            issues.append({
                "type": "ORPHAN_REQ",
                "severity": SEVERITY["ORPHAN_REQ"],
                "node_id": cru_id,
                "message": f"CRU node '{cru_id}' has no SUPPORTED_BY edges → no evidence chunks",
            })
    return issues


def _check_orphan_child_chunks(graph_store: GraphStore) -> list[dict]:
    issues = []
    rows = graph_store.query("""
        SELECT node_id, extra_json FROM nodes WHERE node_type = 'CHUNK'
    """)
    for row in rows:
        try:
            extra = json.loads(row.get("extra_json") or "{}")
        except Exception:
            extra = {}
        if extra.get("chunk_type") != "child":
            continue
        chunk_id = row["node_id"]
        incoming = graph_store.get_edges_to(chunk_id, rel_types=["SUPPORTED_BY", "PARENT_OF"])
        if not incoming:
            issues.append({
                "type": "ORPHAN_CHUNK_CHILD",
                "severity": SEVERITY["ORPHAN_CHUNK_CHILD"],
                "node_id": chunk_id,
                "message": f"Child CHUNK '{chunk_id}' has no incoming SUPPORTED_BY or PARENT_OF edges",
            })
    return issues


def _check_confidence_range(graph_store: GraphStore) -> list[dict]:
    issues = []
    rows = graph_store.query(
        "SELECT src_id, rel_type, dst_id, confidence FROM edges "
        "WHERE confidence < 0.0 OR confidence > 1.0"
    )
    for row in rows:
        issues.append({
            "type": "CONFIDENCE_RANGE",
            "severity": SEVERITY["CONFIDENCE_RANGE"],
            "edge": f"{row['src_id']} --{row['rel_type']}--> {row['dst_id']}",
            "confidence": row["confidence"],
            "message": f"confidence={row['confidence']} is outside [0.0, 1.0]",
        })
    return issues


def _check_confidence_reason_missing(graph_store: GraphStore) -> list[dict]:
    issues = []
    rows = graph_store.query("SELECT src_id, rel_type, dst_id, extra_json FROM edges")
    for row in rows:
        try:
            extra = json.loads(row.get("extra_json") or "{}")
        except Exception:
            extra = {}
        if "confidence_reason" not in extra:
            issues.append({
                "type": "CONFIDENCE_REASON_MISSING",
                "severity": SEVERITY["CONFIDENCE_REASON_MISSING"],
                "edge": f"{row['src_id']} --{row['rel_type']}--> {row['dst_id']}",
                "message": "extra_json missing 'confidence_reason' field",
            })
    return issues


def _check_forbidden_inferred_edges(graph_store: GraphStore) -> list[dict]:
    issues = []
    rows = graph_store.query(
        "SELECT src_id, dst_id FROM edges WHERE rel_type = 'INFERRED_SUPPORTED_BY'"
    )
    for row in rows:
        issues.append({
            "type": "FORBIDDEN_INFERRED_EDGE",
            "severity": SEVERITY["FORBIDDEN_INFERRED_EDGE"],
            "edge": f"{row['src_id']} --INFERRED_SUPPORTED_BY--> {row['dst_id']}",
            "message": "Persisted INFERRED_SUPPORTED_BY edge found. These must only exist "
                       "in-memory and require human approval before persisting.",
        })
    return issues


def _check_parent_of_confidence(graph_store: GraphStore) -> list[dict]:
    issues = []
    rows = graph_store.query(
        "SELECT src_id, dst_id, confidence FROM edges WHERE rel_type = 'PARENT_OF'"
    )
    for row in rows:
        if abs(row["confidence"] - 1.0) > 1e-6:
            issues.append({
                "type": "PARENT_OF_CONFIDENCE",
                "severity": SEVERITY["PARENT_OF_CONFIDENCE"],
                "edge": f"{row['src_id']} --PARENT_OF--> {row['dst_id']}",
                "confidence": row["confidence"],
                "message": f"PARENT_OF edge has confidence={row['confidence']} (expected 1.0 – structural)",
            })
    return issues


def _check_parent_supported_by(graph_store: GraphStore) -> list[dict]:
    """Detect CRU→parent CHUNK SUPPORTED_BY edges – architecture violation."""
    issues = []
    rows = graph_store.query(
        "SELECT src_id, dst_id FROM edges WHERE rel_type = 'SUPPORTED_BY'"
    )
    for row in rows:
        dst_node = graph_store.get_node(row["dst_id"])
        if dst_node is None:
            continue
        try:
            extra = json.loads(dst_node.get("extra_json") or "{}")
        except Exception:
            extra = {}
        if extra.get("chunk_type") == "parent":
            issues.append({
                "type": "PARENT_SUPPORTED_BY",
                "severity": SEVERITY["PARENT_SUPPORTED_BY"],
                "edge": f"{row['src_id']} --SUPPORTED_BY--> {row['dst_id']}",
                "message": f"CRU '{row['src_id']}' is SUPPORTED_BY a parent CHUNK '{row['dst_id']}'. "
                           "Parent chunks must connect via PARENT_OF only.",
            })
    return issues


def run_integrity_checks(db_path: str) -> dict:
    graph_store = GraphStore(db_path)
    try:
        all_issues = []
        all_issues += _check_orphan_reqs(graph_store)
        all_issues += _check_orphan_child_chunks(graph_store)
        all_issues += _check_confidence_range(graph_store)
        all_issues += _check_confidence_reason_missing(graph_store)
        all_issues += _check_forbidden_inferred_edges(graph_store)
        all_issues += _check_parent_of_confidence(graph_store)
        all_issues += _check_parent_supported_by(graph_store)

        errors = [i for i in all_issues if i["severity"] == "error"]
        warnings = [i for i in all_issues if i["severity"] == "warning"]

        report = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "db_path": db_path,
            "summary": {
                "total_issues": len(all_issues),
                "errors": len(errors),
                "warnings": len(warnings),
                "passed": len(all_issues) == 0,
            },
            "issues": all_issues,
        }
        return report
    finally:
        graph_store.close()


def main():
    parser = argparse.ArgumentParser(description="Run GraphRAG integrity checks")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--out", default="rag_integrity_report.json",
                        help="Output path for the JSON report")
    args = parser.parse_args()

    report = run_integrity_checks(args.db)

    Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Integrity report written to: {args.out}")
    print(f"Total issues: {report['summary']['total_issues']} "
          f"(errors={report['summary']['errors']}, warnings={report['summary']['warnings']})")

    if report["summary"]["errors"] > 0:
        print("FAILED – errors found. See report for details.")
        raise SystemExit(1)
    print("PASSED")


if __name__ == "__main__":
    main()
