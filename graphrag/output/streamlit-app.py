from pathlib import Path
import json
import pathlib
from collections import Counter

import duckdb
import streamlit as st

# ---------------------------------------------------------
# ROOT & HARDCODED PATHS (your exact Windows folder layout)
# ---------------------------------------------------------

ROOT = Path(r"C:/Users/Bhoomi/Autopilot-QA")

DB_PATH = ROOT / "graphrag"/ "output" / "graphrag.duckdb"
REQ_PATH = ROOT / "02_Requirement_Understanding" / "output" / "requirements.json"
CRU_PATH = ROOT / "03_CRU_Normalization" / "output" / "cru_units.json"
CHUNK_PATH = ROOT / "04_Semantic_Chunking_and_Domain_Tagging" / "output" / "chunked_crus_with_domain.json"
TEST_PATH = ROOT / "optimized_test_cases_20260403_225627.json"
VECTOR_DIR = ROOT / "graphrag" / "vector"

st.set_page_config(page_title="GraphRAG Debug Console", layout="wide")
st.title("GraphRAG Debug Console")
st.caption(
    "Streamlit-based local debugger for graph retrieval, vector fallback, and traceability inspection."
)

# -----------------
# Helper functions
# -----------------


@st.cache_data
def load_json(path: pathlib.Path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_resource
def get_conn(path_str: str):
    return duckdb.connect(path_str, read_only=True)


@st.cache_resource
@st.cache_resource
def load_vector_fallback():
    import importlib.util

    module_path = ROOT / "graphrag" / "vector" / "vector_fallback.py"
    spec = importlib.util.spec_from_file_location("local_vector_fallback", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.vector_fallback


def req_key(req):
    return req.get("req_id") or req.get("reqid") or ""


def cru_key(cru):
    return cru.get("cruid") or cru.get("cru_id") or ""


def find_requirement(reqs, req_id):
    for r in reqs:
        if req_key(r) == req_id:
            return r
    return None


def find_cru(crus, cru_id):
    for c in crus:
        if cru_key(c) == cru_id:
            return c
    return None


def graph_retrieve(req_id: str, conn, max_children: int = 8, max_parents: int = 3):
    warnings = []
    evidence_chunks = []
    parent_context = []
    trace_paths = []
    related_nodes = []

    req_node = conn.execute(
        "SELECT * FROM nodes WHERE node_id = ? LIMIT 1", [req_id]
    ).fetchone()
    if req_node is None:
        warnings.append(
            {
                "type": "MISSING_REQ_NODE",
                "message": f"REQ node {req_id} not found in graph.",
            }
        )
        return {
            "anchors": [{"node_type": "REQ", "node_id": req_id}],
            "evidence_chunks": evidence_chunks,
            "parent_context": parent_context,
            "trace_paths": trace_paths,
            "related_nodes": related_nodes,
            "warnings": warnings,
            "provenance": "graph",
        }

    child_rows = conn.execute(
        """
        SELECT n.node_id, n.text, n.doc_id, n.section_path, n.source_locator_json, e.confidence
        FROM edges e
        JOIN nodes n ON n.node_id = e.dst_id
        WHERE e.src_id = ?
          AND e.rel_type = 'SUPPORTED_BY'
          AND n.node_type = 'CHUNK'
        ORDER BY e.confidence DESC, n.node_id
        LIMIT ?
        """,
        [req_id, max_children],
    ).fetchall()

    if not child_rows:
        warnings.append(
            {
                "type": "NO_GRAPH_EVIDENCE",
                "message": f"No SUPPORTED_BY child chunks found for {req_id}.",
            }
        )

    for (
        node_id,
        text,
        doc_id,
        section_path,
        source_locator_json,
        confidence,
    ) in child_rows:
        evidence_chunks.append(
            {
                "node_id": node_id,
                "node_type": "CHUNK",
                "text": text,
                "doc_id": doc_id,
                "section_path": section_path,
                "source_locator_json": source_locator_json,
                "confidence": confidence,
                "provenance": "graph",
            }
        )
        trace_paths.append(
            [
                {"type": "REQ", "id": req_id},
                {"rel": "SUPPORTED_BY", "confidence": confidence},
                {"type": "CHUNK", "id": node_id},
            ]
        )

        parent_rows = conn.execute(
            """
            SELECT p.node_id, p.text, p.doc_id, p.section_path, p.source_locator_json, e.confidence
            FROM edges e
            JOIN nodes p ON p.node_id = e.src_id
            WHERE e.rel_type = 'PARENT_OF' AND e.dst_id = ?
            ORDER BY e.confidence DESC, p.node_id
            LIMIT ?
            """,
            [node_id, max_parents],
        ).fetchall()
        for p_id, p_text, p_doc, p_section, p_locator, p_conf in parent_rows:
            parent_context.append(
                {
                    "node_id": p_id,
                    "node_type": "CHUNK",
                    "chunk_type": "parent",
                    "text": p_text,
                    "doc_id": p_doc,
                    "section_path": p_section,
                    "source_locator_json": p_locator,
                    "confidence": p_conf,
                    "provenance": "graph",
                }
            )

    # Deduplicate parent_context by node_id
    parent_context = list({p["node_id"]: p for p in parent_context}.values())

    return {
        "anchors": [{"node_type": "REQ", "node_id": req_id}],
        "evidence_chunks": evidence_chunks,
        "parent_context": parent_context,
        "trace_paths": trace_paths,
        "related_nodes": related_nodes,
        "warnings": warnings,
        "provenance": "graph",
    }


def run_context_pack(req_id, query_text, conn, use_vector, k, threshold):
    result = graph_retrieve(req_id, conn, max_children=k, max_parents=3)
    should_fallback = use_vector and len(result["evidence_chunks"]) == 0 and query_text
    if should_fallback:
        vf = load_vector_fallback()
        vec = vf(
            query_text=query_text,
            graph_store=conn,
            k=k,
        )
 
        if isinstance(vec, dict) and "evidence_chunks" in vec:
            vec["evidence_chunks"] = [
                item for item in vec["evidence_chunks"]
                if item.get("confidence", item.get("score", 0)) >= threshold
            ]
        result["warnings"].extend(vec.get("warnings", []))
        result["evidence_chunks"].extend(vec.get("evidence_chunks", []))
        result["provenance"] = "graph+vector"
    return result


# ---------------
# Load all JSONs
# ---------------

cru_data = load_json(CRU_PATH)
chunk_data = load_json(CHUNK_PATH)
req_data = load_json(REQ_PATH)
test_data = load_json(TEST_PATH)

reqs = req_data.get("requirements", []) if isinstance(req_data, dict) else []

# CRUs can be under "cru_units" (current) or "cruunits" (older)
if isinstance(cru_data, dict):
    crus = (
        cru_data.get("cru_units")
        or cru_data.get("cruunits")
        or cru_data.get("crus")
        or []
    )
else:
    crus = []

req_options = sorted([req_key(r) for r in reqs if req_key(r)])
cru_options = sorted([cru_key(c) for c in crus if cru_key(c)])

# ----------------
# Sidebar / Debug
# ----------------

sidebar = st.sidebar



sidebar.header("Query controls")
selected_req = sidebar.selectbox("Requirement ID", [""] + req_options)
selected_cru = sidebar.selectbox("CRU ID", [""] + cru_options)
free_text = sidebar.text_input(
    "Natural language query", placeholder="e.g. valid input for destination search"
)
use_vector = sidebar.checkbox(
    "Use vector fallback when graph evidence is missing", value=True
)
k = sidebar.slider("Top-k evidence", min_value=1, max_value=15, value=8)
threshold = sidebar.slider(
    "Vector confidence threshold", min_value=0.0, max_value=1.0, value=0.65, step=0.05
)
show_raw = sidebar.checkbox("Show raw JSON", value=False)
run_now = sidebar.button("Run retrieval")

# ---------------
# Workspace status
# ---------------

summary_col, stats_col = st.columns([2, 1])
with summary_col:
    st.subheader("Workspace status")
    st.write(
        {
            "duckdb_exists": DB_PATH.exists(),
            "requirements": len(reqs),
            "crus": len(crus),
            "chunks": len(chunk_data.get("chunks", []))
            if isinstance(chunk_data, dict)
            else 0,
        }
    )
with stats_col:
    if crus:
        by_type = Counter(c.get("type", "unknown") for c in crus)
        st.bar_chart(by_type)

conn = get_conn(str(DB_PATH)) if DB_PATH.exists() else None

# ---------------
# Selected CRU / REQ
# ---------------

if selected_cru:
    cru_obj = find_cru(crus, selected_cru)
    if cru_obj:
        st.subheader("Selected CRU")
        st.write(
            {
                "cruid": cru_key(cru_obj),
                "parentrequirementid": cru_obj.get("parentrequirementid"),
                "actor": cru_obj.get("actor"),
                "type": cru_obj.get("type"),
                "sectionpath": cru_obj.get("traceability", {}).get("sectionpath")
                or cru_obj.get("traceability", {}).get("section"),
                "confidence": cru_obj.get("confidence"),
            }
        )
        st.write(cru_obj.get("action") or cru_obj.get("description"))
        if not selected_req and cru_obj.get("parentrequirementid"):
            st.info(
                f"Using parent requirement anchor: {cru_obj.get('parentrequirementid')}"
            )

if selected_req:
    req_obj = find_requirement(reqs, selected_req)
    if req_obj:
        st.subheader("Selected requirement")
        st.write(
            {
                "req_id": req_key(req_obj),
                "title": req_obj.get("title"),
                "actor": req_obj.get("actor"),
                "section_path": req_obj.get("section_path")
                or req_obj.get("sectionpath"),
                "dependencies": req_obj.get("dependencies"),
                "confidence": req_obj.get("confidence"),
            }
        )
        st.write(req_obj.get("description"))

# ---------------
# Run retrieval
# ---------------

if run_now and conn is not None:
    anchor_req = selected_req
    resolved_query = free_text.strip()

    if selected_cru:
        cru_obj = find_cru(crus, selected_cru)
        if cru_obj and not anchor_req:
            anchor_req = cru_obj.get("parentrequirementid")
        if cru_obj and not resolved_query:
            resolved_query = (
                cru_obj.get("action") or cru_obj.get("description") or selected_cru
            )

    if not resolved_query and anchor_req:
        req_obj = find_requirement(reqs, anchor_req)
        if req_obj:
            resolved_query = (
                req_obj.get("description") or req_obj.get("title") or anchor_req
            )

    if not anchor_req and not resolved_query:
        st.error("Pick a requirement ID, pick a CRU ID, or enter a query.")
    else:
        with st.spinner("Running retrieval..."):
            try:
                if anchor_req:
                    pack = run_context_pack(
                        anchor_req,
                        resolved_query or anchor_req,
                        conn,
                        use_vector,
                        k,
                        threshold,
                    )
                else:
                    vf = load_vector_fallback()
                    pack = vf(
                        query_text=resolved_query,
                        graph_store=conn,
                        k=k,
                    )
                    pack["parent_context"] = []
                    pack["trace_paths"] = []
                    pack["related_nodes"] = []
                    pack["anchors"] = []

                st.subheader("Context pack")
                st.write(
                    {
                        "provenance": pack.get("provenance"),
                        "warning_count": len(pack.get("warnings", [])),
                    }
                )

                t1, t2, t3, t4 = st.tabs(
                    ["Evidence", "Parent context", "Trace paths", "Warnings"]
                )

                with t1:
                    st.write(f"{len(pack.get('evidence_chunks', []))} evidence chunk(s)")
                    for item in pack.get("evidence_chunks", []):
                        st.markdown(f"### {item.get('node_id', 'Unknown node')}")
                        st.write(
                            {
                                "section_path": item.get("section_path"),
                                "doc_id": item.get("doc_id"),
                                "confidence": item.get("confidence"),
                                "provenance": item.get("provenance"),
                            }
                        )
                        st.write(item.get("text"))
                        if show_raw:
                            st.json(item)

                with t2:
                    st.write(
                        f"{len(pack.get('parent_context', []))} parent chunk(s)"
                    )
                    for item in pack.get("parent_context", []):
                        st.markdown(f"### {item.get('node_id', 'Unknown parent')}")
                        st.write(
                            {
                                "section_path": item.get("section_path"),
                                "doc_id": item.get("doc_id"),
                                "confidence": item.get("confidence"),
                            }
                        )
                        st.write(item.get("text"))
                        if show_raw:
                            st.json(item)

                with t3:
                    if pack.get("trace_paths"):
                        for path in pack.get("trace_paths", []):
                            st.json(path)
                    else:
                        st.info("No graph trace paths available for this retrieval.")

                with t4:
                    if pack.get("warnings"):
                        for w in pack.get("warnings", []):
                            st.warning(json.dumps(w, ensure_ascii=False, indent=2))
                    else:
                        st.success("No warnings returned.")
            except Exception as exc:
                st.exception(exc)

# ---------------
# Graph snapshot
# ---------------

st.subheader("Graph snapshot")
if conn is not None:
    node_rows = conn.execute(
        "SELECT node_type, COUNT(*) FROM nodes GROUP BY node_type ORDER BY node_type"
    ).fetchall()
    edge_rows = conn.execute(
        "SELECT rel_type, COUNT(*) FROM edges GROUP BY rel_type ORDER BY rel_type"
    ).fetchall()
    c1, c2 = st.columns(2)
    with c1:
        st.write("Nodes by type")
        st.table(node_rows)
    with c2:
        st.write("Edges by relation")
        st.table(edge_rows)
else:
    st.error("DuckDB graph not found at ROOT path.")