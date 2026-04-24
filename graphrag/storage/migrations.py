"""
graphrag/storage/migrations.py
================================
One-time migration to clean stale edges that were written before the
edge_builder.py fixes landed.

Fixes two integrity-check failures:

  PARENT_SUPPORTED_BY      – deletes CRU→parent CHUNK SUPPORTED_BY edges.
                             These are architecture violations; parent chunks
                             must only connect via PARENT_OF.

  CONFIDENCE_REASON_MISSING – patches edges whose extra_json lacks
                              'confidence_reason' with a sensible default
                              derived from the rel_type, so the integrity
                              checker passes without losing any edges.

Usage:
  python -m graphrag.storage.migrations --db output/graphrag.duckdb
  python -m graphrag.storage.migrations --db output/graphrag.duckdb --dry-run
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from graphrag.storage.graph_store import GraphStore


# ── Default confidence_reason per rel_type ────────────────────────────────────
# Applied only to edges that are genuinely missing the field.

_DEFAULT_REASON: dict[str, str] = {
    "SUPPORTED_BY":    "legacy_edge_pre_audit",
    "PARENT_OF":       "document_structure",
    "TESTS":           "explicit_test_to_req_id_mapping",
    "EVIDENCE_FOR":    "test_expected_result_references_chunk",
    "EXECUTED_AS":     "pipeline_lineage",
    "RAISED_AS":       "run_raised_defect_record",
    "AFFECTS":         "triage_linked_requirement",
}
_FALLBACK_REASON = "legacy_edge_pre_audit"


def _fix_parent_supported_by(graph_store: GraphStore, dry_run: bool) -> int:
    """
    Delete any SUPPORTED_BY edge whose destination is a parent CHUNK.
    Returns the count of edges removed (or that would be removed).
    """
    rows = graph_store.query(
        "SELECT src_id, dst_id FROM edges WHERE rel_type = 'SUPPORTED_BY'"
    )

    to_delete: list[tuple[str, str]] = []
    for row in rows:
        dst_node = graph_store.get_node(row["dst_id"])
        if dst_node is None:
            continue
        try:
            extra = json.loads(dst_node.get("extra_json") or "{}")
        except Exception:
            extra = {}
        if extra.get("chunk_type") == "parent":
            to_delete.append((row["src_id"], row["dst_id"]))

    if not dry_run:
        for src_id, dst_id in to_delete:
            graph_store.execute(
                "DELETE FROM edges WHERE src_id = ? AND rel_type = 'SUPPORTED_BY' AND dst_id = ?",
                [src_id, dst_id],
            )

    action = "Would remove" if dry_run else "Removed"
    print(f"  {action} {len(to_delete)} parent SUPPORTED_BY edge(s)")
    for src_id, dst_id in to_delete:
        print(f"    {src_id} --SUPPORTED_BY--> {dst_id}")
    return len(to_delete)


def _fix_confidence_reason_missing(graph_store: GraphStore, dry_run: bool) -> int:
    """
    Patch edges whose extra_json is missing 'confidence_reason'.
    Uses a rel_type-specific default; falls back to 'legacy_edge_pre_audit'.
    Returns the count of edges patched (or that would be patched).
    """
    rows = graph_store.query(
        "SELECT src_id, rel_type, dst_id, extra_json FROM edges"
    )

    patched = 0
    for row in rows:
        try:
            extra = json.loads(row.get("extra_json") or "{}")
        except Exception:
            extra = {}

        if "confidence_reason" in extra:
            continue

        reason = _DEFAULT_REASON.get(row["rel_type"], _FALLBACK_REASON)
        extra["confidence_reason"] = reason
        new_json = json.dumps(extra)

        if not dry_run:
            graph_store.execute(
                """
                UPDATE edges
                   SET extra_json = ?
                 WHERE src_id = ? AND rel_type = ? AND dst_id = ?
                """,
                [new_json, row["src_id"], row["rel_type"], row["dst_id"]],
            )

        action = "Would patch" if dry_run else "Patched"
        print(
            f"  {action} {row['src_id']} --{row['rel_type']}--> {row['dst_id']}"
            f"  (confidence_reason='{reason}')"
        )
        patched += 1

    print(f"  Total: {patched} edge(s) patched")
    return patched


def run_migrations(db_path: str, dry_run: bool = False) -> dict:
    graph_store = GraphStore(db_path)
    try:
        print(f"\n{'[DRY RUN] ' if dry_run else ''}Running migrations on: {db_path}\n")

        print("── Fix 1: Remove parent SUPPORTED_BY edges ─────────────────────────")
        removed = _fix_parent_supported_by(graph_store, dry_run)

        print("\n── Fix 2: Patch missing confidence_reason ──────────────────────────")
        patched = _fix_confidence_reason_missing(graph_store, dry_run)

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Done. "
              f"Removed={removed}, Patched={patched}")

        return {"removed_parent_supported_by": removed, "patched_confidence_reason": patched}
    finally:
        graph_store.close()


def main():
    parser = argparse.ArgumentParser(description="Run GraphRAG DB migrations")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change without writing anything",
    )
    args = parser.parse_args()
    run_migrations(args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()