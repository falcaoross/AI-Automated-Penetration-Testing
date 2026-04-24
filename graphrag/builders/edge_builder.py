"""
graphrag/builders/edge_builder.py
===================================
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict


# ── ID helpers (must match chunk_builder.py exactly) ──────────────────────────

def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _make_parent_node_id(doc_id: str, section_path: str) -> str:
    return f"P_{_sha256(doc_id + section_path)}"


def _make_child_node_id(doc_id: str, section_path: str, cru_id: str, clause_text: str) -> str:
    normalized = clause_text.lower().strip()
    return f"CH_{_sha256(doc_id + section_path + cru_id + normalized)}"


def _derive_clause_text(cru: dict) -> str:
    actor = cru.get("actor", "")
    action = cru.get("action")
    constraint = cru.get("constraint")
    if not action or str(action).strip() == "":
        action = f"must satisfy constraint: {constraint}" if constraint else "unspecified behavior"
    clause = f"{actor} {action}"
    if constraint and str(constraint).strip() and str(constraint) not in clause:
        clause = f"{clause} {constraint}"
    return clause


# ── SUPPORTED_BY (CRU → CHILD CHUNK only) ─────────────────────────────────────

def build_supported_by_edges(graph_store, cru_units_path: str, chunked_crus_path: str) -> Dict:
    """
    Insert CRU --SUPPORTED_BY--> CHILD_CHUNK edges only.
    Parent chunks are intentionally excluded – they attach via PARENT_OF.
    """
    with open(chunked_crus_path, "r", encoding="utf-8") as fh:
        chunk_data = json.load(fh)

    chunks = chunk_data.get("chunks")
    if chunks is None:
        raise ValueError("build_supported_by_edges: top-level 'chunks' array is missing")

    edges_written = 0

    for chunk_entry in chunks:
        traceability = chunk_entry.get("traceability") or {}
        sections = traceability.get("sections") or []
        doc_ids = traceability.get("doc_ids") or []
        crus = chunk_entry.get("crus", [])
        cru_ids = chunk_entry.get("cru_ids", [])

        if not cru_ids or not crus:
            continue

        first_cru_id = cru_ids[0]
        first_cru_node = graph_store.get_node(first_cru_id)
        if first_cru_node is None:
            continue

        section_path = sections[0] if sections else first_cru_node.get("section_path", "UNKNOWN")
        doc_id = doc_ids[0] if doc_ids else first_cru_node.get("doc_id", "UNKNOWN")

        for cru in crus:
            cru_id = cru.get("cru_id")
            actor = cru.get("actor")
            if not cru_id or not actor:
                continue

            clause_text = _derive_clause_text(cru)
            child_node_id = _make_child_node_id(doc_id, section_path, cru_id, clause_text)

            if graph_store.get_node(child_node_id) is None:
                continue  # child chunk not in graph yet – skip silently

            # Confidence: explicit ID match (cru_id referenced in chunk) → 0.95
            # CRU data may store confidence as a string label ("high", "medium", "low")
            _CONF_LABELS = {"high": 0.95, "medium": 0.75, "low": 0.50}
            raw_conf = cru.get("confidence")
            if raw_conf is None:
                confidence = 0.95
            elif isinstance(raw_conf, str):
                confidence = _CONF_LABELS.get(raw_conf.strip().lower(), 0.75)
            else:
                confidence = float(raw_conf)

            graph_store.insert_edge({
                "src_id": cru_id,
                "src_type": "CRU",
                "rel_type": "SUPPORTED_BY",
                "dst_id": child_node_id,
                "dst_type": "CHUNK",
                "confidence": min(max(confidence, 0.0), 1.0),
                "evidence_chunk_id": child_node_id,
                "extra_json": {
                    "kind": "child_support",
                    "confidence_reason": "explicit_cru_id_in_chunk",
                },
            })
            edges_written += 1

    return {"supported_by_edges_written": edges_written}


# ── PARENT_OF (PARENT_CHUNK → CHILD_CHUNK) ────────────────────────────────────

def build_parent_of_edges(graph_store, chunked_crus_path: str) -> Dict:
    """Hierarchy edges only – no scoring impact, deterministic confidence=1.0."""
    with open(chunked_crus_path, "r", encoding="utf-8") as fh:
        chunk_data = json.load(fh)

    chunks = chunk_data.get("chunks")
    if chunks is None:
        raise ValueError("build_parent_of_edges: top-level 'chunks' array is missing")

    edges_written = 0

    for chunk_entry in chunks:
        traceability = chunk_entry.get("traceability") or {}
        sections = traceability.get("sections") or []
        doc_ids = traceability.get("doc_ids") or []
        crus = chunk_entry.get("crus", [])
        cru_ids = chunk_entry.get("cru_ids", [])

        if not cru_ids or not crus:
            continue

        first_cru_id = cru_ids[0]
        first_cru_node = graph_store.get_node(first_cru_id)
        if first_cru_node is None:
            continue

        section_path = sections[0] if sections else first_cru_node.get("section_path", "UNKNOWN")
        doc_id = doc_ids[0] if doc_ids else first_cru_node.get("doc_id", "UNKNOWN")
        parent_node_id = _make_parent_node_id(doc_id, section_path)

        if graph_store.get_node(parent_node_id) is None:
            continue

        for cru in crus:
            cru_id = cru.get("cru_id")
            actor = cru.get("actor")
            if not cru_id or not actor:
                continue

            clause_text = _derive_clause_text(cru)
            child_node_id = _make_child_node_id(doc_id, section_path, cru_id, clause_text)

            if graph_store.get_node(child_node_id) is None:
                continue

            graph_store.insert_edge({
                "src_id": parent_node_id,
                "src_type": "CHUNK",
                "rel_type": "PARENT_OF",
                "dst_id": child_node_id,
                "dst_type": "CHUNK",
                "confidence": 1.0,
                "evidence_chunk_id": child_node_id,
                "extra_json": {
                    "kind": "hierarchy",
                    "confidence_reason": "document_structure",
                },
            })
            edges_written += 1

    return {"parent_of_edges_written": edges_written}


# ── TESTS + EVIDENCE_FOR (TEST → CRU, TEST → CHUNK) ──────────────────────────

def _get_supported_by_chunk_ids(graph_store, cru_id: str) -> list:
    """
    Return a list of CHUNK node IDs that the given CRU points to via SUPPORTED_BY edges.

    Uses GraphStore.get_edges_from(node_id, rel_types) directly — the existing
    method on graphrag/storage/graph_store.py that queries:
        SELECT * FROM edges WHERE src_id=? AND rel_type IN (?)

    Returns only dst_ids whose dst_type is "CHUNK" as a safety guard, so a
    future schema change that adds non-CHUNK SUPPORTED_BY targets cannot
    accidentally create bad EVIDENCE_FOR edges.
    """
    edges = graph_store.get_edges_from(cru_id, rel_types=["SUPPORTED_BY"])
    return [
        edge["dst_id"]
        for edge in edges
        if edge.get("dst_type") == "CHUNK" and edge.get("dst_id")
    ]

def build_test_edges(graph_store, test_file_path: str) -> Dict:
    """
    Insert:
      TEST --TESTS--> CRU
      TEST --EVIDENCE_FOR--> CHUNK  (for each evidence_chunk_id on the test)
    """
    with open(test_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    all_tests = data.get("phase1_test_cases", []) + data.get("phase2_test_cases", [])
    tests_edges = 0
    evidence_for_edges_explicit = 0
    evidence_for_edges_derived = 0
    skipped = 0

    for test in all_tests:
        test_id = test.get("test_id")
        req_id = test.get("requirement_id")

        if not test_id or not req_id:
            skipped += 1
            continue
        if not graph_store.node_exists(test_id) or not graph_store.node_exists(req_id):
            skipped += 1
            continue

        # TEST → CRU
        graph_store.insert_edge({
            "src_id": test_id,
            "src_type": "TEST",
            "rel_type": "TESTS",
            "dst_id": req_id,
            "dst_type": "CRU",
            "confidence": 1.0,
            "evidence_chunk_id": None,
            "extra_json": {
                "priority": test.get("priority"),
                "test_type": test.get("test_type"),
                "generation_phase": test.get("generation_phase"),
                "confidence_reason": "explicit_test_to_req_id_mapping",
            },
        })
        tests_edges += 1

        # ── EVIDENCE_FOR: Pass A – explicit chunk IDs from test JSON ──────────
        explicit_chunk_ids: list = test.get("evidence_chunk_ids") or []
        for chunk_id in explicit_chunk_ids:
            if not graph_store.node_exists(chunk_id):
                continue
            graph_store.insert_edge({
                "src_id": test_id,
                "src_type": "TEST",
                "rel_type": "EVIDENCE_FOR",
                "dst_id": chunk_id,
                "dst_type": "CHUNK",
                "confidence": 0.90,
                "evidence_chunk_id": chunk_id,
                "extra_json": {
                    "kind": "explicit_evidence",
                    "confidence_reason": "test_expected_result_references_chunk",
                },
            })
            evidence_for_edges_explicit += 1

        # ── EVIDENCE_FOR: Pass B – derive from CRU's SUPPORTED_BY edges ───────
        # Only runs when the test JSON did not supply explicit chunk IDs.
        # Traversal: TEST.req_id → CRU --SUPPORTED_BY--> CHUNK
        if not explicit_chunk_ids:
            derived_chunk_ids = _get_supported_by_chunk_ids(graph_store, req_id)
            for chunk_id in derived_chunk_ids:
                if not graph_store.node_exists(chunk_id):
                    continue
                graph_store.insert_edge({
                    "src_id": test_id,
                    "src_type": "TEST",
                    "rel_type": "EVIDENCE_FOR",
                    "dst_id": chunk_id,
                    "dst_type": "CHUNK",
                    "confidence": 0.80,
                    "evidence_chunk_id": chunk_id,
                    "extra_json": {
                        "kind": "derived_evidence",
                        "confidence_reason": "derived_from_requirement_supported_by",
                    },
                })
                evidence_for_edges_derived += 1

    evidence_for_total = evidence_for_edges_explicit + evidence_for_edges_derived
    print(
        f"[TEST EDGES] TESTS={tests_edges}, "
        f"EVIDENCE_FOR={evidence_for_total} "
        f"(explicit={evidence_for_edges_explicit}, derived={evidence_for_edges_derived}), "
        f"skipped={skipped}"
    )
    return {
        "tests_edges_written": tests_edges,
        "evidence_for_edges_written": evidence_for_total,
        "evidence_for_explicit": evidence_for_edges_explicit,
        "evidence_for_derived": evidence_for_edges_derived,
        "skipped": skipped,
    }


# ── EXECUTED_AS + RAISED_AS (TEST→RUN, RUN→DEFECT) ───────────────────────────

def build_execution_edges(graph_store, runs: list) -> Dict:
    """
    runs: list of dicts with:
      run_id, test_id (optional), defect_ids (optional list)
    """
    exec_edges = 0
    raised_edges = 0

    for run in runs:
        run_id = run.get("run_id")
        if not run_id:
            continue

        test_id = run.get("test_id")
        if test_id and graph_store.node_exists(test_id) and graph_store.node_exists(run_id):
            graph_store.insert_edge({
                "src_id": test_id,
                "src_type": "TEST",
                "rel_type": "EXECUTED_AS",
                "dst_id": run_id,
                "dst_type": "RUN",
                "confidence": 1.0,
                "extra_json": {
                    "kind": "execution_lineage",
                    "confidence_reason": "pipeline_lineage",
                },
            })
            exec_edges += 1

        for defect_id in run.get("defect_ids", []):
            if graph_store.node_exists(run_id) and graph_store.node_exists(defect_id):
                graph_store.insert_edge({
                    "src_id": run_id,
                    "src_type": "RUN",
                    "rel_type": "RAISED_AS",
                    "dst_id": defect_id,
                    "dst_type": "DEFECT",
                    "confidence": 1.0,
                    "extra_json": {
                        "kind": "failure_linkage",
                        "confidence_reason": "run_raised_defect_record",
                    },
                })
                raised_edges += 1

    return {"executed_as_edges": exec_edges, "raised_as_edges": raised_edges}


# ── AFFECTS (DEFECT → CRU) ────────────────────────────────────────────────────

def build_affects_edges(graph_store, defects: list) -> Dict:
    """
    defects: list of dicts with:
      defect_id, linked_req_ids (list of CRU ids)
    """
    affects_edges = 0
    for defect in defects:
        defect_id = defect.get("defect_id")
        if not defect_id:
            continue
        for req_id in defect.get("linked_req_ids", []):
            if graph_store.node_exists(defect_id) and graph_store.node_exists(req_id):
                graph_store.insert_edge({
                    "src_id": defect_id,
                    "src_type": "DEFECT",
                    "rel_type": "AFFECTS",
                    "dst_id": req_id,
                    "dst_type": "CRU",
                    "confidence": 0.85,
                    "extra_json": {
                        "kind": "defect_impact",
                        "confidence_reason": "triage_linked_requirement",
                    },
                })
                affects_edges += 1
    return {"affects_edges_written": affects_edges}