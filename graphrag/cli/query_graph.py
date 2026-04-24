"""
graphrag/cli/query_graph.py
=============================
FIXES IN THIS VERSION:
  [BUG-4] _debug_report() and _impact_report() now emit an explicit warning
          when the graph contains no RUN or DEFECT nodes, explaining that
          --runs / --defects must be provided to build_graph.py to populate
          execution-trace data. Previously the reporters returned empty
          results silently.

  [BUG-5] _acceptance_report() acceptance comparator replaces the fragile
          word-overlap heuristic (≥50 % raw word match) with embedding cosine
          similarity using the same SentenceTransformer model already in the
          stack (sentence-transformers/all-MiniLM-L6-v2).
          Thresholds: cosine ≥ 0.75 → match, ≥ 0.50 → partial, < 0.50 → missing.
          This is consistent with the rest of the retrieval pipeline and produces
          semantically meaningful verdicts rather than lexical ones.

Previously-correct fixes retained:
  - [PARTIAL-6] Normalized graph-first fusion
  - [PARTIAL-7] AcceptanceComparator structure (criterion loop, open_questions)
  - [BUG-1/2] build_embeddings manifest_dir
  - SUPPORTED_BY typo fix, open_questions in context_pack_to_dict
  - debug/impact reporter structure
  - real score/confidence/provenance from graph_result
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from graphrag.context.context_pack_builder import build_context_pack
from graphrag.generation.test_generator import generate_tests_from_context_pack
from graphrag.models.contracts import AcceptanceDecision, OpenQuestion
from graphrag.retrieval.anchor_resolver import resolve_anchors
from graphrag.retrieval.graph_retriever import graph_retrieve
from graphrag.retrieval.query_router import route_query
from graphrag.storage.graph_store import GraphStore
#NOTE: vector_fallback, vector_index, and sentence_transformers are NOT
#       imported here. They are imported lazily at the call site.


# ── Serialisation ─────────────────────────────────────────────────────────────

def to_serializable(obj):
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_serializable(v) for v in obj]
    if hasattr(obj, "__dict__"):
        return {k: to_serializable(v) for k, v in obj.__dict__.items()}
    return str(obj)


def context_pack_to_dict(pack) -> dict:
    return {
        "anchors":         [to_serializable(a) for a in pack.anchors],
        "evidence_chunks": [to_serializable(c) for c in pack.evidence_chunks],
        "parent_context":  [to_serializable(c) for c in pack.parent_context],
        "trace_paths":     [to_serializable(t) for t in pack.trace_paths],
        "related_nodes":   [to_serializable(r) for r in pack.related_nodes],
        "warnings":        to_serializable(pack.warnings),
        "open_questions":  to_serializable(pack.open_questions),
    }


# ── Vector fallback gate ───────────────────────────────────────────────────────

def should_trigger_vector_fallback(
    graph_result: dict,
    min_evidence: int = 3,
    min_avg_conf: float = 0.65,
) -> bool:
    evidence = graph_result.get("evidence_nodes", [])
    if len(evidence) < min_evidence:
        return True

    confidences = [n.get("path_confidence", 0.0) for n in evidence if "path_confidence" in n]
    if confidences:
        avg_conf = sum(confidences) / len(confidences)
        if avg_conf < min_avg_conf:
            return True

    has_supported_by = any(
        any(edge.get("rel") == "SUPPORTED_BY" for edge in p.get("path", []))
        for p in graph_result.get("trace_paths", [])
        if isinstance(p, dict)
    )
    return not has_supported_by


# ── [PARTIAL-6] Normalized graph-first fusion ─────────────────────────────────

_GRAPH_BIAS = 0.30   # added to graph node scores before merging


def merge_graph_and_vector(
    graph_store: GraphStore,
    graph_result: dict,
    vector_hits: list,
    top_k: int = 8,
) -> dict:
    """
    Graph-first fused merge.
    Graph candidates score is boosted by GRAPH_BIAS before sorting so
    graph evidence wins over similarly-scored vector evidence, but very
    high-similarity vector hits (similarity > 1 - GRAPH_BIAS) can still surface.
    """
    graph_nodes = graph_result.get("evidence_nodes", [])
    existing_ids = {n["node_id"] for n in graph_nodes}

    # Boost graph scores
    for n in graph_nodes:
        n["_adjusted_score"] = n.get("score", 0.0) + _GRAPH_BIAS

    # Add vector hits (CHUNK only, not already in graph results)
    vector_nodes = []
    for hit in vector_hits:
        if hit["node_id"] in existing_ids:
            continue
        node = graph_store.get_node(hit["node_id"])
        if not node or node.get("node_type") != "CHUNK":
            continue

        import json as _json
        try:
            extra = _json.loads(node.get("extra_json") or "{}")
        except Exception:
            extra = {}
        if extra.get("chunk_type") != "child":
            continue

        node = dict(node)
        sim = hit.get("score", 0.0)
        node["score"] = sim
        node["path_confidence"] = sim
        node["provenance"] = "vector"
        node["needs_confirmation"] = sim < 0.65
        node["_adjusted_score"] = sim   # no bias for vector
        vector_nodes.append(node)

    merged = graph_nodes + vector_nodes
    merged.sort(key=lambda n: n.get("_adjusted_score", 0.0), reverse=True)

    graph_result["evidence_nodes"] = merged[:top_k]
    return graph_result


# ── [BUG-4] Execution-node presence check helper ──────────────────────────────

def _has_execution_nodes(graph_store: GraphStore) -> bool:
    """Return True if the graph contains any RUN or DEFECT nodes."""
    stats = graph_store.stats()
    node_counts = stats.get("nodes", {})
    return bool(node_counts.get("RUN", 0) or node_counts.get("DEFECT", 0))


_EXECUTION_NODES_WARNING = (
    "No RUN or DEFECT nodes found in the graph. "
    "Execution-trace data (EXECUTED_AS, RAISED_AS, AFFECTS edges) is absent. "
    "To populate this data, supply --runs and --defects to build_graph.py and rebuild the graph."
)


# ── Task reporters ────────────────────────────────────────────────────────────

def _debug_report(context_pack_dict: dict, graph_store: GraphStore) -> dict:
    related = context_pack_dict.get("related_nodes", [])
    defects = [r for r in related if r.get("node_type") in ("DEFECT", "FAILURE", "RUN")]

    warnings = list(context_pack_dict.get("warnings", []))

    # [BUG-4] Warn explicitly when execution nodes are absent so the caller
    # understands why trace data is empty rather than getting silent emptiness.
    if not defects and not _has_execution_nodes(graph_store):
        warnings.append(_EXECUTION_NODES_WARNING)

    return {
        "task": "debug",
        "status": "ok",
        "trace_to_failure": context_pack_dict.get("trace_paths", []),
        "related_defects_and_runs": defects,
        "warnings": warnings,
        "open_questions": context_pack_dict.get("open_questions", []),
    }


def _impact_report(context_pack_dict: dict, graph_store: GraphStore) -> dict:
    affected = context_pack_dict.get("related_nodes", [])

    warnings = list(context_pack_dict.get("warnings", []))

    # [BUG-4] Same check: AFFECTS edges require DEFECT nodes built from --defects.
    if not affected and not _has_execution_nodes(graph_store):
        warnings.append(_EXECUTION_NODES_WARNING)

    return {
        "task": "impact",
        "status": "ok",
        "affected_nodes": affected,
        "evidence_chunks": context_pack_dict.get("evidence_chunks", []),
        "warnings": warnings,
        "open_questions": context_pack_dict.get("open_questions", []),
    }


# ── [BUG-5] Embedding model singleton for acceptance comparator ───────────────

_ACCEPTANCE_MODEL = None


def _get_acceptance_model():
    global _ACCEPTANCE_MODEL
    if _ACCEPTANCE_MODEL is None:
        from sentence_transformers import SentenceTransformer
        _ACCEPTANCE_MODEL = SentenceTransformer(MODEL_NAME)
    return _ACCEPTANCE_MODEL


_MATCH_THRESHOLD   = 0.75   # cosine ≥ this → match
_PARTIAL_THRESHOLD = 0.50   # cosine ≥ this → partial, else missing


# ── [PARTIAL-7] + [BUG-5] Acceptance comparator ──────────────────────────────

def _acceptance_report(
    context_pack_dict: dict,
    graph_store: GraphStore,
    anchors: list,
) -> dict:
    """
    Acceptance comparator using embedding cosine similarity.

    For each anchor CRU, retrieve its acceptance_criteria and compare each
    criterion against every evidence chunk using the SentenceTransformer model
    already present in the stack (sentence-transformers/all-MiniLM-L6-v2).

    Verdict per criterion:
      match   – cosine(criterion, chunk) ≥ 0.75 for at least one chunk
      partial – best cosine ≥ 0.50 but < 0.75
      missing – best cosine < 0.50 across all chunks
      (conflict detection is intentionally left for a future pass; semantic
       similarity alone cannot reliably detect contradiction.)

    [BUG-5] Replaces the raw word-overlap heuristic (≥50 % word match) which
    produced false matches on high-frequency words and missed paraphrased evidence.
    """
    import json as _json
    import numpy as np

    decisions: List[dict] = []
    open_questions: List[dict] = []
    evidence_chunks = context_pack_dict.get("evidence_chunks", [])
    evidence_ids = [c.get("chunk_id") for c in evidence_chunks]

    # Pre-encode all evidence chunk texts in one batch for efficiency.
    # Fall back gracefully if there are no chunks.
    chunk_texts = [c.get("text", "") for c in evidence_chunks]
    if chunk_texts:
        model = _get_acceptance_model()
        chunk_vecs = model.encode(chunk_texts, normalize_embeddings=True)  # (N, dim)
    else:
        chunk_vecs = None

    for anchor in anchors:
        node_id = anchor.node_id if hasattr(anchor, "node_id") else anchor["node_id"]
        node = graph_store.get_node(node_id)
        if not node:
            continue

        try:
            extra = _json.loads(node.get("extra_json") or "{}")
        except Exception:
            extra = {}

        criteria_raw = extra.get("acceptance_criteria")
        if not criteria_raw:
            open_questions.append({
                "question": f"No acceptance_criteria defined for anchor {node['node_id']}.",
                "required_for": node["node_id"],
                "chunk_ids_available": evidence_ids,
            })
            continue

        # Normalise criteria to a flat list of strings.
        if isinstance(criteria_raw, list):
            criteria = [str(c).strip() for c in criteria_raw if str(c).strip()]
        else:
            criteria = [c.strip() for c in str(criteria_raw).split("\n") if c.strip()]

        # Encode all criteria in one batch.
        if criteria and chunk_vecs is not None:
            model = _get_acceptance_model()
            crit_vecs = model.encode(criteria, normalize_embeddings=True)  # (M, dim)
            # Cosine similarity matrix: (M, N) — rows=criteria, cols=chunks.
            # Vectors are L2-normalised so dot product == cosine similarity.
            sim_matrix = crit_vecs @ chunk_vecs.T
        else:
            sim_matrix = None

        for c_idx, criterion in enumerate(criteria):
            supporting_ids: List[str] = []
            verdict = "missing"

            if sim_matrix is not None and len(evidence_chunks) > 0:
                sims = sim_matrix[c_idx]          # cosine scores for this criterion
                best_sim = float(np.max(sims))

                if best_sim >= _MATCH_THRESHOLD:
                    verdict = "match"
                elif best_sim >= _PARTIAL_THRESHOLD:
                    verdict = "partial"
                # else verdict stays "missing"

                # Collect all chunk IDs that clear the partial threshold.
                for ch_idx, sim in enumerate(sims):
                    if sim >= _PARTIAL_THRESHOLD:
                        cid = evidence_chunks[ch_idx].get("chunk_id")
                        if cid:
                            supporting_ids.append(cid)

                notes = (
                    f"cosine similarity: best={best_sim:.3f} "
                    f"across {len(evidence_chunks)} chunks "
                    f"(match≥{_MATCH_THRESHOLD}, partial≥{_PARTIAL_THRESHOLD})"
                )
            else:
                notes = "no evidence chunks available for comparison"

            if verdict == "missing":
                open_questions.append({
                    "question": f"No evidence found for criterion: '{criterion}'",
                    "required_for": node["node_id"],
                    "chunk_ids_available": evidence_ids,
                })

            decisions.append(AcceptanceDecision(
                criterion=criterion,
                verdict=verdict,
                evidence_chunk_ids=supporting_ids,
                notes=notes,
            ))

    return {
        "task": "acceptance_validation",
        "status": "ok",
        "decisions": [to_serializable(d) for d in decisions],
        "open_questions": open_questions,
        "warnings": context_pack_dict.get("warnings", []),
        "evidence_chunks": evidence_chunks,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Query GraphRAG graph")
    parser.add_argument("--db", required=True)
    parser.add_argument("--req-id")
    parser.add_argument("--query-text")
    parser.add_argument(
        "--task", required=True,
        choices=["test_generation", "debug", "impact", "acceptance_validation"],
    )
    parser.add_argument("--module", default="")
    parser.add_argument("--version", default="")
    parser.add_argument("--doctype", default="")
    parser.add_argument("--out", default="context_pack.json")
    parser.add_argument("--rebuild-emb", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    out_dir = str(Path(args.out).parent)
    graph_store = GraphStore(str(Path(args.db)))

    try:
        # Lazy import: only load vector/ML stack when --rebuild-emb is passed
        if args.rebuild_emb:
            from graphrag.vector.vector_index import build_embeddings  # noqa: PLC0415
            build_embeddings(graph_store, force_rebuild=True, manifest_dir=out_dir)

        payload = {
            "task": args.task,
            "req_id": args.req_id,
            "query_text": args.query_text,
            "filters": {
                "module":   args.module,
                "version":  args.version,
                "doc_type": args.doctype,
            },
            "k_evidence": 8,
            "k_parent": 3,
        }

        query = route_query(payload)
        anchors = resolve_anchors(graph_store, query)
        graph_result = graph_retrieve(
            graph_store, anchors, query.task, filters=query.filters
        )

        if should_trigger_vector_fallback(graph_result):
            # Lazy import: only load vector stack when fallback is actually needed
            from graphrag.retrieval.vector_fallback import vector_search  # noqa: PLC0415
            vector_hits = vector_search(
                graph_store=graph_store,
                query_text=query.query_text or query.req_id or "",
                filters=query.filters,
                node_types=["CHUNK", "CRU", "DEFECT", "FAILURE"],
                top_k=30,
            )
            graph_result = merge_graph_and_vector(
                graph_store, graph_result, vector_hits, top_k=query.k_evidence
            )

        context_pack = build_context_pack(
            graph_store=graph_store,
            anchors=anchors,
            graph_result=graph_result,
            k_evidence=query.k_evidence,
            k_parent=query.k_parent,
        )
        context_pack_dict = context_pack_to_dict(context_pack)

        # Route to task handler
        if args.task == "test_generation":
            try:
                generated_response = generate_tests_from_context_pack(
                    context_pack=context_pack_dict, provider="ollama"
                )
            except Exception as e:
                generated_response = {"task": args.task, "status": "generation_failed", "error": str(e)}

        elif args.task == "debug":
            # [BUG-4] pass graph_store so reporter can check for RUN/DEFECT nodes
            generated_response = _debug_report(context_pack_dict, graph_store)

        elif args.task == "impact":
            # [BUG-4] pass graph_store so reporter can check for RUN/DEFECT nodes
            generated_response = _impact_report(context_pack_dict, graph_store)

        elif args.task == "acceptance_validation":
            # [BUG-5] cosine-similarity comparator
            generated_response = _acceptance_report(context_pack_dict, graph_store, anchors)

        else:
            generated_response = {"task": args.task, "status": "unsupported_task"}

        output_data = {
            "context_pack": context_pack_dict,
            "result": to_serializable(generated_response),
        }

        if args.json:
            print(json.dumps(output_data, indent=2, default=str))

        # ALWAYS SAVE
        Path(args.out).write_text(
            json.dumps(output_data, indent=2, default=str),
            encoding="utf-8"
        )

        print(f"✅ Output saved: {args.out}")

        Path(args.out).write_text(
            json.dumps(context_pack_dict, indent=2, default=str), encoding="utf-8"
        )
        print(f"✅ Context Pack saved: {args.out}")
        print(f"Anchors:        {len(context_pack.anchors)}")
        print(f"Evidence chunks:{len(context_pack.evidence_chunks)}")
        print(f"Parent context: {len(context_pack.parent_context)}")
        print(f"Warnings:       {len(context_pack.warnings)}")
        print(f"Open questions: {len(context_pack.open_questions)}")

        print(f"\n── {args.task} Report ──")
        print(json.dumps(to_serializable(generated_response), indent=2, default=str))

    finally:
        graph_store.close()


if __name__ == "__main__":
    main()