import argparse
import json
import pathlib
from datetime import datetime, timezone

import duckdb


def _write_report(out_dir: pathlib.Path, report: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "rag_integrity_report.json"
    report_path.write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )


def _run_checks(conn: duckdb.DuckDBPyConnection) -> tuple[list, list]:
    errors: list = []
    warnings: list = []

    # ── A. Orphan REQ check ───────────────────────────────────────────────────
    req_rows = conn.execute(
        "SELECT node_id FROM nodes WHERE node_type = 'REQ'"
    ).fetchall()

    for (req_id,) in req_rows:
        has_edge = conn.execute(
            """
            SELECT 1 FROM edges
            WHERE src_id = ? AND rel_type = 'SUPPORTED_BY'
            LIMIT 1
            """,
            [req_id],
        ).fetchone()
        if has_edge is None:
            errors.append({
                "check":   "ORPHAN_REQ",
                "node_id": req_id,
                "message": (
                    f"REQ node {req_id!r} has no outgoing SUPPORTED_BY edge"
                ),
            })

    # ── B. Orphan CHUNK (child) check ─────────────────────────────────────────
    child_chunk_rows = conn.execute(
        """
        SELECT node_id FROM nodes
        WHERE node_type = 'CHUNK'
          AND json_extract_string(extra_json, '$.chunk_type') = 'child'
        """
    ).fetchall()

    for (chunk_id,) in child_chunk_rows:
        has_edge = conn.execute(
            """
            SELECT 1 FROM edges
            WHERE dst_id = ? AND rel_type = 'SUPPORTED_BY'
            LIMIT 1
            """,
            [chunk_id],
        ).fetchone()
        if has_edge is None:
            warnings.append({
                "check":   "ORPHAN_CHUNK_CHILD",
                "node_id": chunk_id,
                "message": (
                    f"child CHUNK node {chunk_id!r} has no incoming SUPPORTED_BY edge"
                ),
            })

    # ── C. Confidence validation ──────────────────────────────────────────────
    edge_rows = conn.execute(
        "SELECT src_id, rel_type, dst_id, confidence, extra_json FROM edges"
    ).fetchall()

    for (src_id, rel_type, dst_id, confidence, extra_json_str) in edge_rows:
        edge_ref = f"({src_id!r} --{rel_type}--> {dst_id!r})"

        if confidence is None or not (0.40 <= confidence <= 1.00):
            errors.append({
                "check":      "CONFIDENCE_RANGE",
                "edge":       edge_ref,
                "confidence": confidence,
                "message": (
                    f"Edge {edge_ref} has confidence {confidence!r} outside [0.40, 1.00]"
                ),
            })

        has_reason = False
        if extra_json_str:
            try:
                extra = json.loads(extra_json_str)
                has_reason = bool(extra.get("confidence_reason"))
            except (json.JSONDecodeError, AttributeError):
                has_reason = False

        if not has_reason:
            errors.append({
                "check":   "CONFIDENCE_REASON_MISSING",
                "edge":    edge_ref,
                "message": (
                    f"Edge {edge_ref} is missing 'confidence_reason' in extra_json"
                ),
            })

    # ── D. Forbidden inferred edges ───────────────────────────────────────────
    inferred_rows = conn.execute(
        """
        SELECT src_id, dst_id FROM edges
        WHERE rel_type = 'INFERRED_SUPPORTED_BY'
        """
    ).fetchall()

    for (src_id, dst_id) in inferred_rows:
        errors.append({
            "check":   "FORBIDDEN_INFERRED_EDGE",
            "src_id":  src_id,
            "dst_id":  dst_id,
            "message": (
                f"Persisted INFERRED_SUPPORTED_BY edge from {src_id!r} to {dst_id!r} "
                "is forbidden"
            ),
        })

    # ── E. PARENT_OF confidence ───────────────────────────────────────────────
    parent_of_rows = conn.execute(
        "SELECT src_id, dst_id, confidence FROM edges WHERE rel_type = 'PARENT_OF'"
    ).fetchall()

    for (src_id, dst_id, confidence) in parent_of_rows:
        if confidence != 1.0:
            errors.append({
                "check":      "PARENT_OF_CONFIDENCE",
                "src_id":     src_id,
                "dst_id":     dst_id,
                "confidence": confidence,
                "message": (
                    f"PARENT_OF edge ({src_id!r} --> {dst_id!r}) has confidence "
                    f"{confidence!r}; must be exactly 1.0"
                ),
            })

    return errors, warnings


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run integrity checks on the GraphRAG graph."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the DuckDB database file.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for rag_integrity_report.json.",
    )
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out)

    try:
        conn = duckdb.connect(args.db, read_only=True)

        total_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        total_edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]

        errors, warnings = _run_checks(conn)

        report = {
            "status":    "fail" if errors else "pass",
            "errors":    errors,
            "warnings":  warnings,
            "stats": {
                "total_nodes": total_nodes,
                "total_edges": total_edges,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        _write_report(out_dir, report)

        print(
            f"Integrity check {'FAILED' if errors else 'PASSED'}. "
            f"Report written to {out_dir / 'rag_integrity_report.json'}"
        )

    except Exception as exc:
        failure_report = {
            "status":    "fail",
            "errors":    [{"check": "UNEXPECTED_EXCEPTION", "message": str(exc)}],
            "warnings":  [],
            "stats":     {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        _write_report(out_dir, failure_report)
        raise


if __name__ == "__main__":
    main()