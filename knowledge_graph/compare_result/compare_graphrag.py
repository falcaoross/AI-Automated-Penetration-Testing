"""
compare_knowledge_graph.py  (updated - adds test_diff command)
=========================================================
Three sub-commands:

  snapshot   - capture graph + query state into a JSON file
  report     - generate HTML before/after comparison report
  test_diff  - NEW: query the DB directly, find every test case added
               between two snapshots, export CSV + detailed HTML

Usage
-----
# Capture snapshots (same as before)
python compare_knowledge_graph.py snapshot --db knowledge_graph/output/knowledge_graph.duckdb --req-id CRU-FR10-01 --label before --out comparison/
python compare_knowledge_graph.py snapshot --db knowledge_graph/output/knowledge_graph.duckdb --req-id CRU-FR10-01 --label after  --out comparison/

# Generate the before/after overview report
python compare_knowledge_graph.py report --before comparison/snapshot_before.json --after comparison/snapshot_after.json --out comparison/comparison_report.html

# NEW: dump every new test case with full details
python compare_knowledge_graph.py test_diff \
    --db      knowledge_graph/output/knowledge_graph.duckdb \
    --before  comparison/snapshot_before.json \
    --after   comparison/snapshot_after.json \
    --req-id  CRU-FR10-01 \
    --out     comparison/
    
    Produces:
      comparison/new_test_cases.csv        <- paste into Excel / paper appendix
      comparison/new_test_cases.html       <- full searchable HTML table
      comparison/test_diff_summary.json    <- machine-readable stats
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


# ==============================================================================
# SNAPSHOT
# ==============================================================================

def capture_snapshot(db_path: str, req_id: str, label: str, out_dir: str) -> dict:
    from knowledge_graph.storage.graph_store import GraphStore
    from knowledge_graph.retrieval.query_router import route_query
    from knowledge_graph.retrieval.anchor_resolver import resolve_anchors
    from knowledge_graph.retrieval.graph_retriever import graph_retrieve
    from knowledge_graph.context.context_pack_builder import build_context_pack

    print(f"[COMPARE] Capturing '{label}' snapshot ...")
    gs = GraphStore(db_path)
    try:
        stats = gs.stats()

        node_samples = {}
        for nt in stats["nodes"]:
            rows = gs.query(
                "SELECT node_id, node_type, title, text, module, version "
                "FROM nodes WHERE node_type = ? LIMIT 3", [nt])
            node_samples[nt] = rows

        edge_samples = {}
        for rt in stats["edges"]:
            rows = gs.query(
                "SELECT src_id, rel_type, dst_id, confidence, extra_json "
                "FROM edges WHERE rel_type = ? LIMIT 3", [rt])
            edge_samples[rt] = rows

        try:
            from knowledge_graph.validation.integrity_checks import run_integrity_checks
            integrity_summary = run_integrity_checks(db_path)["summary"]
        except Exception as e:
            integrity_summary = {"error": str(e)}

        query_result, context_pack_raw = {}, {}
        try:
            query = route_query({"task": "test_generation", "req_id": req_id,
                                  "query_text": None, "filters": {}, "k_evidence": 8, "k_parent": 3})
            anchors = resolve_anchors(gs, query)
            graph_result = graph_retrieve(gs, anchors, query.task, filters={})
            cp = build_context_pack(graph_store=gs, anchors=anchors,
                                     graph_result=graph_result, k_evidence=8, k_parent=3)

            def _s(o): return o.__dict__ if hasattr(o, "__dict__") else str(o)

            context_pack_raw = {
                "anchors":        [_s(a) for a in cp.anchors],
                "evidence_chunks":[_s(c) for c in cp.evidence_chunks],
                "parent_context": [_s(c) for c in cp.parent_context],
                "trace_paths":    [_s(t) for t in cp.trace_paths],
                "related_nodes":  [_s(r) for r in cp.related_nodes],
                "warnings":       [_s(w) for w in cp.warnings],
                "open_questions": [_s(q) for q in cp.open_questions],
            }
            query_result = {
                "req_id": req_id,
                "anchor_count": len(cp.anchors),
                "evidence_count": len(cp.evidence_chunks),
                "parent_count": len(cp.parent_context),
                "trace_path_count": len(cp.trace_paths),
                "related_node_count": len(cp.related_nodes),
                "warning_count": len(cp.warnings),
                "open_question_count": len(cp.open_questions),
                "avg_confidence": round(sum(c.confidence for c in cp.evidence_chunks) /
                                        max(len(cp.evidence_chunks), 1), 4),
                "avg_score": round(sum(c.score for c in cp.evidence_chunks) /
                                   max(len(cp.evidence_chunks), 1), 4),
                "warnings_text": [_s(w) for w in cp.warnings],
            }
        except Exception as e:
            query_result = {"error": str(e)}

        snapshot = {
            "label": label, "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "db_path": db_path, "req_id": req_id,
            "graph_stats": stats, "node_samples": node_samples,
            "edge_samples": edge_samples, "integrity": integrity_summary,
            "query_result": query_result, "context_pack": context_pack_raw,
        }
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        out_path = out / f"snapshot_{label}.json"
        out_path.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
        print(f"[COMPARE] Saved: {out_path}")
        print(f"[COMPARE] Nodes: {stats['nodes']}  Edges: {stats['edges']}")
        return snapshot
    finally:
        gs.close()


# ==============================================================================
# TEST DIFF  (new command)
# ==============================================================================

def run_test_diff(db_path: str, before_path: str, after_path: str,
                  req_id: str, out_dir: str) -> None:
    """
    Query the live DB for every test case linked to req_id,
    compare against the before snapshot to isolate new ones,
    and export CSV + HTML.
    """
    from knowledge_graph.storage.graph_store import GraphStore

    before = json.loads(Path(before_path).read_text())
    after  = json.loads(Path(after_path).read_text())

    before_test_ids = {
        r["node_id"]
        for r in before.get("context_pack", {}).get("related_nodes", [])
        if r.get("node_type") == "TEST"
    }

    print(f"[TEST-DIFF] Connecting to {db_path} ...")
    gs = GraphStore(db_path)
    try:
        # Pull every TEST linked to req_id via TESTS edge
        rows = gs.query("""
            SELECT
                n.node_id,
                n.title,
                n.text,
                n.module,
                n.version,
                n.extra_json,
                e.confidence   AS tests_confidence,
                e.extra_json   AS tests_extra
            FROM nodes n
            JOIN edges e ON e.src_id = n.node_id
            WHERE e.rel_type = 'TESTS'
              AND e.dst_id   = ?
              AND n.node_type = 'TEST'
            ORDER BY n.node_id
        """, [req_id])

        # Pull EVIDENCE_FOR edges for those test cases
        all_test_ids = [r["node_id"] for r in rows]
        ev_map: dict = {}
        if all_test_ids:
            ph = ",".join("?" * len(all_test_ids))
            ev_rows = gs.query(
                f"SELECT src_id, dst_id, confidence, extra_json "
                f"FROM edges WHERE rel_type='EVIDENCE_FOR' AND src_id IN ({ph})",
                all_test_ids
            )
            for ev in ev_rows:
                ev_map[ev["src_id"]] = ev

        # Pull the evidence chunk text once
        chunk_id = None
        chunk_text = ""
        cp_chunks = after.get("context_pack", {}).get("evidence_chunks", [])
        if cp_chunks:
            chunk_id = cp_chunks[0].get("chunk_id")
            chunk_text = cp_chunks[0].get("text", "")

    finally:
        gs.close()

    # Classify each test as new or existing
    all_tests = []
    for r in rows:
        try:
            extra = json.loads(r.get("extra_json") or "{}")
        except Exception:
            extra = {}
        try:
            tests_extra = json.loads(r.get("tests_extra") or "{}")
        except Exception:
            tests_extra = {}

        ev = ev_map.get(r["node_id"], {})
        try:
            ev_extra = json.loads(ev.get("extra_json") or "{}")
        except Exception:
            ev_extra = {}

        is_new = r["node_id"] not in before_test_ids
        all_tests.append({
            "test_id":           r["node_id"],
            "title":             r["title"] or "",
            "description":       r["text"] or "",
            "test_type":         extra.get("test_type") or tests_extra.get("test_type") or "-",
            "priority":          extra.get("priority")  or tests_extra.get("priority")  or "-",
            "generation_phase":  extra.get("section_path") or tests_extra.get("generation_phase") or "-",
            "module":            r["module"] or "-",
            "version":           r["version"] or "-",
            "tests_confidence":  r.get("tests_confidence", 0),
            "ev_chunk_id":       ev.get("dst_id", "-"),
            "ev_confidence":     ev.get("confidence", "-"),
            "ev_kind":           ev_extra.get("kind", "-"),
            "ev_reason":         ev_extra.get("confidence_reason", "-"),
            "is_new":            is_new,
        })

    new_tests     = [t for t in all_tests if t["is_new"]]
    existing_tests = [t for t in all_tests if not t["is_new"]]

    # -- Stats -----------------------------------------------------------------
    def _count(lst, key, val):
        return sum(1 for t in lst if str(t.get(key, "")).lower() == str(val).lower())

    summary = {
        "req_id":              req_id,
        "timestamp_utc":       datetime.now(timezone.utc).isoformat(),
        "total_tests":         len(all_tests),
        "existing_tests":      len(existing_tests),
        "new_tests":           len(new_tests),
        "new_by_type": {
            "positive":   _count(new_tests, "test_type", "positive"),
            "negative":   _count(new_tests, "test_type", "negative"),
            "edge":       _count(new_tests, "test_type", "edge"),
            "performance":_count(new_tests, "test_type", "performance"),
            "other":      sum(1 for t in new_tests
                              if t["test_type"].lower() not in
                              ("positive","negative","edge","performance")),
        },
        "new_by_priority": {
            "High":   _count(new_tests, "priority", "High"),
            "Medium": _count(new_tests, "priority", "Medium"),
            "Low":    _count(new_tests, "priority", "Low"),
        },
        "evidence_grounding": {
            "explicit": sum(1 for t in new_tests if t["ev_reason"] != "derived_from_requirement_supported_by"
                            and t["ev_reason"] != "-"),
            "derived":  sum(1 for t in new_tests if t["ev_reason"] == "derived_from_requirement_supported_by"),
            "none":     sum(1 for t in new_tests if t["ev_chunk_id"] == "-"),
        },
        "evidence_chunk_id":   chunk_id,
        "evidence_chunk_text": chunk_text,
        "before_snapshot":     before_path,
        "after_snapshot":      after_path,
    }

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # -- CSV -------------------------------------------------------------------
    csv_path = out / "new_test_cases.csv"
    fieldnames = ["test_id","title","description","test_type","priority",
                  "generation_phase","module","version","tests_confidence",
                  "ev_chunk_id","ev_confidence","ev_kind","ev_reason","is_new"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for t in all_tests:
            w.writerow({k: t[k] for k in fieldnames})

    # -- Summary JSON ----------------------------------------------------------
    json_path = out / "test_diff_summary.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # -- HTML ------------------------------------------------------------------
    html_path = out / "new_test_cases.html"
    html_path.write_text(_build_test_html(new_tests, existing_tests, summary), encoding="utf-8")

    print(f"[TEST-DIFF] Done.")
    print(f"  New tests:      {len(new_tests)}")
    print(f"  Existing tests: {len(existing_tests)}")
    print(f"  CSV:            {csv_path}")
    print(f"  HTML:           {html_path}")
    print(f"  Summary JSON:   {json_path}")


# -- HTML builder --------------------------------------------------------------

def _build_test_html(new_tests: list, existing_tests: list, summary: dict) -> str:
    req_id    = summary["req_id"]
    ts        = summary["timestamp_utc"]
    n_new     = summary["new_tests"]
    n_exist   = summary["existing_tests"]
    n_total   = summary["total_tests"]
    by_type   = summary["new_by_type"]
    by_prio   = summary["new_by_priority"]
    grounding = summary["evidence_grounding"]
    chunk_txt = (summary.get("evidence_chunk_text") or "")[:180]
    chunk_id  = summary.get("evidence_chunk_id") or "-"

    def badge(val, key="test_type"):
        val = str(val)
        colours = {
            "positive":   ("background:#dff3e3;color:#24683c",),
            "negative":   ("background:#f9dddd;color:#ac2a2a",),
            "edge":       ("background:#fff1cf;color:#94610c",),
            "performance":("background:#e6effd;color:#275b9b",),
            "High":       ("background:#e6effd;color:#275b9b",),
            "Medium":     ("background:#fff1cf;color:#94610c",),
            "Low":        ("background:#f1efe8;color:#5f5e5a",),
        }
        s = colours.get(val, ("background:#f1efe8;color:#5f5e5a",))[0]
        return f'<span style="{s};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600">{val}</span>'

    def rows_html(tests, label_class):
        if not tests:
            return f'<tr><td colspan="8" style="color:#8b949e;font-style:italic;padding:16px">No {label_class} tests.</td></tr>'
        out = ""
        for t in tests:
            new_mark = '<span style="background:rgba(63,185,80,.15);color:#3fb950;padding:2px 6px;border-radius:3px;font-size:10px;font-weight:700">NEW</span>' if t["is_new"] else ""
            ev_chip = (
                f'<span style="background:rgba(240,136,62,.15);color:#f0883e;padding:2px 6px;border-radius:3px;font-size:10px">derived</span>'
                if t["ev_reason"] == "derived_from_requirement_supported_by"
                else f'<span style="background:rgba(63,185,80,.15);color:#3fb950;padding:2px 6px;border-radius:3px;font-size:10px">explicit</span>'
                if t["ev_chunk_id"] != "-"
                else '<span style="color:#8b949e;font-size:10px">none</span>'
            )
            out += f"""<tr>
              <td style="white-space:nowrap">{new_mark} <code style="font-size:11px">{t['test_id']}</code></td>
              <td>{t['title']}</td>
              <td style="color:#8b949e;font-size:12px">{t['description'][:90]}{'...' if len(t['description'])>90 else ''}</td>
              <td>{badge(t['test_type'])}</td>
              <td>{badge(t['priority'], 'priority')}</td>
              <td style="font-size:11px;color:#8b949e">{t['generation_phase']}</td>
              <td style="font-size:11px;color:#8b949e">{t['ev_confidence']}</td>
              <td>{ev_chip}</td>
            </tr>"""
        return out

    pct_pos  = round(by_type.get("positive",0)/max(n_new,1)*100)
    pct_neg  = round(by_type.get("negative",0)/max(n_new,1)*100)
    pct_edge = round(by_type.get("edge",0)/max(n_new,1)*100)
    pct_perf = round(by_type.get("performance",0)/max(n_new,1)*100)
    pct_hi   = round(by_prio.get("High",0)/max(n_new,1)*100)
    pct_med  = round(by_prio.get("Medium",0)/max(n_new,1)*100)
    pct_low  = round(by_prio.get("Low",0)/max(n_new,1)*100)
    pct_der  = round(grounding.get("derived",0)/max(n_new,1)*100)
    pct_exp  = round(grounding.get("explicit",0)/max(n_new,1)*100)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Test Case Diff - {req_id}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
  :root {{
    --bg:#0d1117; --surface:#161b22; --surface2:#1e2530; --border:#30363d;
    --text:#e6edf3; --muted:#8b949e; --pos:#3fb950; --neg:#f85149;
    --accent:#f0883e; --info:#58a6ff; --mono:'IBM Plex Mono',monospace; --sans:'IBM Plex Sans',sans-serif;
  }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:var(--bg); color:var(--text); font-family:var(--sans); font-size:14px; line-height:1.6; }}
  .hdr {{ background:linear-gradient(135deg,#0d1117,#1a2332); border-bottom:1px solid var(--border); padding:36px 48px 28px; }}
  .hdr h1 {{ font-size:26px; font-weight:700; margin-bottom:6px; }}
  .hdr h1 span {{ color:var(--pos); }}
  .hdr p {{ color:var(--muted); font-size:13px; }}
  .req {{ display:inline-block; background:rgba(88,166,255,.15); border:1px solid rgba(88,166,255,.3);
          color:var(--info); font-family:var(--mono); font-size:12px; padding:2px 10px;
          border-radius:20px; margin-bottom:10px; }}
  .wrap {{ max-width:1400px; margin:0 auto; padding:32px 48px; }}
  .stat-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:32px; }}
  .sc {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:18px 20px; text-align:center; }}
  .sc .v {{ font-size:36px; font-weight:700; font-family:var(--mono); }}
  .sc .l {{ font-size:11px; color:var(--muted); letter-spacing:1px; text-transform:uppercase; margin-top:4px; }}
  .sc .v.pos {{ color:var(--pos); }} .sc .v.neu {{ color:var(--info); }} .sc .v.warn {{ color:var(--accent); }}
  .charts {{ display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:32px; }}
  .cc {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:18px 20px; }}
  .cc h3 {{ font-size:11px; font-weight:600; color:var(--muted); letter-spacing:1px; text-transform:uppercase; margin-bottom:14px; }}
  .bar-r {{ display:flex; align-items:center; gap:8px; margin-bottom:8px; font-size:12px; }}
  .bar-l {{ width:82px; text-align:right; color:var(--muted); font-size:11px; }}
  .bar-t {{ flex:1; height:16px; background:var(--surface2); border-radius:3px; overflow:hidden; }}
  .bar-f {{ height:100%; border-radius:3px; }}
  .bar-n {{ width:34px; text-align:right; font-size:11px; color:var(--text); }}
  .ev-box {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:18px 20px; margin-bottom:24px; }}
  .ev-box h3 {{ font-size:11px; font-weight:600; color:var(--muted); letter-spacing:1px; text-transform:uppercase; margin-bottom:12px; }}
  .ev-text {{ font-family:var(--mono); font-size:12px; color:#cdd5e0; line-height:1.55; background:var(--surface2); padding:12px 16px; border-radius:8px; margin-bottom:10px; }}
  .ev-meta {{ font-size:11px; color:var(--muted); }}
  .ev-meta code {{ background:var(--surface2); padding:1px 5px; border-radius:3px; }}
  .note {{ background:var(--surface); border-left:3px solid var(--accent); border-radius:0 8px 8px 0; padding:12px 16px; margin-bottom:24px; font-size:13px; color:#cdd5e0; }}
  .note strong {{ color:var(--accent); }}
  .sec-title {{ font-size:11px; font-weight:600; letter-spacing:2px; text-transform:uppercase;
                color:var(--muted); border-bottom:1px solid var(--border); padding-bottom:10px; margin-bottom:16px; }}
  .filter-bar {{ display:flex; gap:10px; margin-bottom:14px; flex-wrap:wrap; }}
  .filter-bar input, .filter-bar select {{
    background:var(--surface); border:1px solid var(--border); color:var(--text);
    padding:6px 12px; border-radius:8px; font-size:13px; font-family:var(--sans); outline:none; }}
  .filter-bar input:focus, .filter-bar select:focus {{ border-color:#444d56; }}
  .table-wrap {{ overflow-x:auto; border:1px solid var(--border); border-radius:12px; }}
  table {{ width:100%; border-collapse:collapse; min-width:900px; }}
  th {{ background:var(--surface2); color:var(--muted); font-size:11px; font-weight:600;
        letter-spacing:1px; text-transform:uppercase; padding:10px 14px; text-align:left;
        border-bottom:1px solid var(--border); position:sticky; top:0; z-index:1; }}
  td {{ padding:10px 14px; border-bottom:1px solid var(--border); font-size:13px; vertical-align:top; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover td {{ background:rgba(255,255,255,.02); }}
  .sec {{ margin-bottom:32px; }}
  .count-badge {{ display:inline-block; background:var(--surface2); color:var(--muted);
                  font-family:var(--mono); font-size:12px; padding:2px 8px; border-radius:6px; margin-left:8px; }}
</style>
</head>
<body>
<div class="hdr">
  <div class="req">{req_id}</div>
  <h1>Test Case <span>Diff Report</span></h1>
  <p>Generated {ts} &nbsp;·&nbsp; task: test_generation</p>
</div>
<div class="wrap">

  <div class="stat-grid">
    <div class="sc"><div class="v pos">+{n_new}</div><div class="l">New test cases</div></div>
    <div class="sc"><div class="v neu">{n_exist}</div><div class="l">Existing tests</div></div>
    <div class="sc"><div class="v neu">{n_total}</div><div class="l">Total tests</div></div>
    <div class="sc"><div class="v warn">{pct_der}%</div><div class="l">Derived grounding</div></div>
  </div>

  <div class="charts">
    <div class="cc">
      <h3>By test type</h3>
      <div class="bar-r"><div class="bar-l">positive</div><div class="bar-t"><div class="bar-f" style="width:{pct_pos}%;background:#3fb950"></div></div><div class="bar-n">{by_type.get('positive',0)}</div></div>
      <div class="bar-r"><div class="bar-l">negative</div><div class="bar-t"><div class="bar-f" style="width:{pct_neg}%;background:#f85149"></div></div><div class="bar-n">{by_type.get('negative',0)}</div></div>
      <div class="bar-r"><div class="bar-l">edge</div><div class="bar-t"><div class="bar-f" style="width:{pct_edge}%;background:#f0883e"></div></div><div class="bar-n">{by_type.get('edge',0)}</div></div>
      <div class="bar-r"><div class="bar-l">performance</div><div class="bar-t"><div class="bar-f" style="width:{pct_perf}%;background:#58a6ff"></div></div><div class="bar-n">{by_type.get('performance',0)}</div></div>
      <div class="bar-r"><div class="bar-l">other</div><div class="bar-t"><div class="bar-f" style="width:{round(by_type.get('other',0)/max(n_new,1)*100)}%;background:#8b949e"></div></div><div class="bar-n">{by_type.get('other',0)}</div></div>
    </div>
    <div class="cc">
      <h3>By priority</h3>
      <div class="bar-r"><div class="bar-l">High</div><div class="bar-t"><div class="bar-f" style="width:{pct_hi}%;background:#58a6ff"></div></div><div class="bar-n">{by_prio.get('High',0)}</div></div>
      <div class="bar-r"><div class="bar-l">Medium</div><div class="bar-t"><div class="bar-f" style="width:{pct_med}%;background:#f0883e"></div></div><div class="bar-n">{by_prio.get('Medium',0)}</div></div>
      <div class="bar-r"><div class="bar-l">Low</div><div class="bar-t"><div class="bar-f" style="width:{pct_low}%;background:#8b949e"></div></div><div class="bar-n">{by_prio.get('Low',0)}</div></div>
    </div>
    <div class="cc">
      <h3>Evidence grounding</h3>
      <div class="bar-r"><div class="bar-l">derived</div><div class="bar-t"><div class="bar-f" style="width:{pct_der}%;background:#f0883e"></div></div><div class="bar-n">{grounding.get('derived',0)}</div></div>
      <div class="bar-r"><div class="bar-l">explicit</div><div class="bar-t"><div class="bar-f" style="width:{pct_exp}%;background:#3fb950"></div></div><div class="bar-n">{grounding.get('explicit',0)}</div></div>
      <div class="bar-r"><div class="bar-l">none</div><div class="bar-t"><div class="bar-f" style="width:{round(grounding.get('none',0)/max(n_new,1)*100)}%;background:#8b949e"></div></div><div class="bar-n">{grounding.get('none',0)}</div></div>
    </div>
  </div>

  <div class="ev-box">
    <h3>Evidence chunk used by all new tests</h3>
    <div class="ev-text">{chunk_txt}{'...' if len(chunk_txt)>=180 else ''}</div>
    <div class="ev-meta">chunk_id: <code>{chunk_id}</code> &nbsp;·&nbsp; confidence: <code>0.80</code> (derived) &nbsp;·&nbsp; provenance: <code>graph</code></div>
  </div>

  <div class="note">
    <strong>Grounding note:</strong> All {grounding.get('derived',0)} new EVIDENCE_FOR edges are
    <code style="background:rgba(0,0,0,.3);padding:1px 5px;border-radius:3px">derived_from_requirement_supported_by</code>.
    The generator did not output explicit chunk IDs - grounding was inferred automatically via the
    CRU -> CHUNK -> TEST path. Upgrading to explicit citations would raise confidence from 0.80 -> 0.95.
  </div>

  <div class="sec">
    <div class="sec-title">New test cases <span class="count-badge">{n_new}</span></div>
    <div class="filter-bar">
      <input id="search" placeholder="Search title or ID..." style="flex:1;min-width:200px" oninput="filterTable()">
      <select id="f-type" onchange="filterTable()">
        <option value="">All types</option>
        <option>positive</option><option>negative</option><option>edge</option><option>performance</option>
      </select>
      <select id="f-prio" onchange="filterTable()">
        <option value="">All priorities</option>
        <option>High</option><option>Medium</option><option>Low</option>
      </select>
    </div>
    <div class="table-wrap">
      <table id="new-table">
        <thead><tr>
          <th>Test ID</th><th>Title</th><th>Description</th>
          <th>Type</th><th>Priority</th><th>Phase</th><th>Ev. conf</th><th>Grounding</th>
        </tr></thead>
        <tbody id="new-tbody">{rows_html(new_tests, 'new')}</tbody>
      </table>
    </div>
  </div>

  <div class="sec">
    <div class="sec-title">Existing test cases (unchanged) <span class="count-badge">{n_exist}</span></div>
    <div class="table-wrap" style="max-height:400px;overflow-y:auto">
      <table>
        <thead><tr>
          <th>Test ID</th><th>Title</th><th>Description</th>
          <th>Type</th><th>Priority</th><th>Phase</th><th>Ev. conf</th><th>Grounding</th>
        </tr></thead>
        <tbody>{rows_html(existing_tests, 'existing')}</tbody>
      </table>
    </div>
  </div>

</div>
<script>
function filterTable() {{
  const q    = document.getElementById('search').value.toLowerCase();
  const type = document.getElementById('f-type').value.toLowerCase();
  const prio = document.getElementById('f-prio').value.toLowerCase();
  const rows = document.querySelectorAll('#new-tbody tr');
  rows.forEach(r => {{
    const txt  = r.textContent.toLowerCase();
    const cells = r.querySelectorAll('td');
    const rowType = cells[3] ? cells[3].textContent.trim().toLowerCase() : '';
    const rowPrio = cells[4] ? cells[4].textContent.trim().toLowerCase() : '';
    const matchQ = !q    || txt.includes(q);
    const matchT = !type || rowType.includes(type);
    const matchP = !prio || rowPrio.includes(prio);
    r.style.display = (matchQ && matchT && matchP) ? '' : 'none';
  }});
}}
</script>
</body>
</html>"""


# ==============================================================================
# REPORT (overview, unchanged from before)
# ==============================================================================

def generate_report(before_path: str, after_path: str, out_path: str):
    before = json.loads(Path(before_path).read_text())
    after  = json.loads(Path(after_path).read_text())
    html   = _build_overview_html(before, after)
    Path(out_path).write_text(html, encoding="utf-8")
    print(f"[COMPARE] Overview report saved: {out_path}")


def _delta(a, b):
    try:
        d = float(b) - float(a)
        if d > 0: return f"+{d:.0f}" if d == int(d) else f"+{d:.3f}"
        if d < 0: return f"{d:.0f}" if d == int(d) else f"{d:.3f}"
        return "±0"
    except Exception:
        return "-"


def _build_overview_html(before: dict, after: dict) -> str:
    b_s = before.get("graph_stats", {})
    a_s = after.get("graph_stats",  {})
    b_q = before.get("query_result", {})
    a_q = after.get("query_result",  {})
    b_i = before.get("integrity", {})
    a_i = after.get("integrity",  {})

    all_nt = sorted(set(b_s.get("nodes",{}).keys()) | set(a_s.get("nodes",{}).keys()))
    all_et = sorted(set(b_s.get("edges",{}).keys()) | set(a_s.get("edges",{}).keys()))

    def node_rows():
        r = ""
        for t in all_nt:
            bv = b_s.get("nodes",{}).get(t,0); av = a_s.get("nodes",{}).get(t,0)
            d  = _delta(bv,av); cls = "pos" if av>bv else ("neg" if av<bv else "neu")
            r += f"<tr><td class='type'>{t}</td><td>{bv}</td><td>{av}</td><td class='{cls}'>{d}</td></tr>"
        return r

    def edge_rows():
        r = ""
        for t in all_et:
            bv = b_s.get("edges",{}).get(t,0); av = a_s.get("edges",{}).get(t,0)
            d  = _delta(bv,av); cls = "pos" if av>bv else ("neg" if av<bv else "neu")
            r += f"<tr><td class='type'>{t}</td><td>{bv}</td><td>{av}</td><td class='{cls}'>{d}</td></tr>"
        return r

    def qrows():
        keys = [("evidence_count","Evidence chunks"),("parent_count","Parent context"),
                ("trace_path_count","Trace paths"),("related_node_count","Related nodes"),
                ("warning_count","Warnings"),("open_question_count","Open questions"),
                ("avg_confidence","Avg confidence"),("avg_score","Avg score")]
        r = ""
        for key, lbl in keys:
            bv = b_q.get(key,"-"); av = a_q.get(key,"-")
            try:
                d  = _delta(float(bv),float(av))
                cls = "pos" if float(av)>float(bv) else ("neg" if float(av)<float(bv) else "neu")
            except Exception:
                d = "-"; cls = "neu"
            r += f"<tr><td class='type'>{lbl}</td><td>{bv}</td><td>{av}</td><td class='{cls}'>{d}</td></tr>"
        return r

    def chunk_cards(pack, label):
        chunks = pack.get("context_pack",{}).get("evidence_chunks",[])
        if not chunks: return f"<p class='empty'>No evidence chunks in {label} snapshot.</p>"
        cards = ""
        for c in chunks:
            conf = c.get("confidence",0); score = c.get("score",0)
            prov = c.get("provenance","graph"); text = (c.get("text") or "")[:220]
            cid  = c.get("chunk_id","?"); sec = c.get("section_path","")
            bar_w = int(float(conf)*100)
            cards += f"""<div class="chunk-card">
              <div class="chunk-header"><span class="cid">{cid[:40]}...</span>
              <span class="prov prov-{prov}">{prov}</span></div>
              <div class="chunk-sec">{sec}</div>
              <div class="chunk-text">{text}{'...' if len(c.get('text',''))>220 else ''}</div>
              <div class="chunk-meta"><span>score {score:.3f}</span>
              <span class="conf-bar-wrap"><span class="conf-bar" style="width:{bar_w}%"></span></span>
              <span>conf {conf:.3f}</span></div></div>"""
        return cards

    def warn_list(pack):
        warns = pack.get("context_pack",{}).get("warnings",[])
        if not warns: return "<p class='empty'>No warnings.</p>"
        items = ""
        for w in warns:
            wt = w.get("type","?") if isinstance(w,dict) else str(w)
            wm = w.get("message","") if isinstance(w,dict) else ""
            items += f"<div class='warn-item'><span class='warn-type'>{wt}</span> {wm}</div>"
        return items

    def findings():
        items = []
        b_t = sum(b_s.get("nodes",{}).values()); a_t = sum(a_s.get("nodes",{}).values())
        if a_t != b_t: items.append(f"<strong>Graph grew by {abs(a_t-b_t)} nodes</strong> ({b_t} -> {a_t})")
        b_e = sum(b_s.get("edges",{}).values()); a_e = sum(a_s.get("edges",{}).values())
        if a_e != b_e: items.append(f"<strong>{abs(a_e-b_e)} new edges</strong> added ({b_e} -> {a_e})")
        new_nt = set(a_s.get("nodes",{}).keys())-set(b_s.get("nodes",{}).keys())
        if new_nt: items.append(f"<strong>New node types:</strong> {', '.join(sorted(new_nt))}")
        new_et = set(a_s.get("edges",{}).keys())-set(b_s.get("edges",{}).keys())
        if new_et: items.append(f"<strong>New relation types:</strong> {', '.join(sorted(new_et))}")
        b_ev = b_q.get("evidence_count",0); a_ev = a_q.get("evidence_count",0)
        if b_ev != a_ev: items.append(f"<strong>Evidence chunks retrieved:</strong> {b_ev} -> {a_ev}")
        if not items: items = ["No significant differences in query result or evidence quality."]
        return "".join(f'<div class="finding"><strong>-></strong> {f}</div>' for f in items)

    b_ts = before.get("timestamp_utc","-"); a_ts = after.get("timestamp_utc","-")
    req  = before.get("req_id","-")

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GraphRAG Before / After - {req}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
  :root{{--bg:#0d1117;--surface:#161b22;--surface2:#1e2530;--border:#30363d;--text:#e6edf3;
    --muted:#8b949e;--before:#58a6ff;--after:#3fb950;--pos:#3fb950;--neg:#f85149;
    --neu:#8b949e;--accent:#f0883e;--mono:'IBM Plex Mono',monospace;--sans:'IBM Plex Sans',sans-serif;}}
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;line-height:1.6;}}
  .header{{background:linear-gradient(135deg,#0d1117 0%,#1a2332 100%);border-bottom:1px solid var(--border);padding:40px 48px 32px;}}
  .header h1{{font-size:28px;font-weight:700;margin-bottom:6px;}} .header h1 span{{color:var(--accent);}}
  .header p{{color:var(--muted);font-size:13px;}}
  .req-badge{{display:inline-block;background:rgba(88,166,255,.15);border:1px solid rgba(88,166,255,.3);
    color:var(--before);font-family:var(--mono);font-size:12px;padding:2px 10px;border-radius:20px;margin-bottom:12px;}}
  .container{{max-width:1280px;margin:0 auto;padding:32px 48px;}}
  .section{{margin-bottom:40px;}}
  .section-title{{font-size:11px;font-weight:600;letter-spacing:2px;text-transform:uppercase;
    color:var(--muted);border-bottom:1px solid var(--border);padding-bottom:10px;margin-bottom:20px;}}
  .timeline{{display:flex;align-items:center;gap:16px;margin-bottom:32px;background:var(--surface);
    border:1px solid var(--border);border-radius:12px;padding:20px 24px;}}
  .tl-node{{flex:1;text-align:center;}} .tl-arrow{{font-size:24px;color:var(--border);flex-shrink:0;}}
  .tl-label{{font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;margin-bottom:4px;}}
  .tl-label.before{{color:var(--before);}} .tl-label.after{{color:var(--after);}}
  .tl-ts{{font-family:var(--mono);font-size:11px;color:var(--muted);}}
  table{{width:100%;border-collapse:collapse;}}
  th{{background:var(--surface2);color:var(--muted);font-size:11px;font-weight:600;letter-spacing:1px;
    text-transform:uppercase;padding:10px 14px;text-align:left;border-bottom:1px solid var(--border);}}
  td{{padding:10px 14px;border-bottom:1px solid var(--border);font-family:var(--mono);font-size:13px;}}
  tr:last-child td{{border-bottom:none;}} tr:hover td{{background:rgba(255,255,255,.02);}}
  td.type{{font-weight:600;color:var(--text);font-family:var(--sans);}}
  .table-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;}}
  th:nth-child(2){{color:var(--before);}} th:nth-child(3){{color:var(--after);}}
  .pos{{color:var(--pos);font-weight:600;}} .neg{{color:var(--neg);font-weight:600;}} .neu{{color:var(--neu);}}
  .cols2{{display:grid;grid-template-columns:1fr 1fr;gap:20px;}}
  .col-label{{font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px;}}
  .col-label.before{{color:var(--before);}} .col-label.after{{color:var(--after);}}
  .chunk-card{{background:var(--surface2);border:1px solid var(--border);border-radius:10px;
    padding:14px 16px;margin-bottom:12px;}}
  .chunk-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;}}
  .cid{{font-family:var(--mono);font-size:11px;color:var(--muted);}}
  .prov{{font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase;padding:2px 7px;border-radius:4px;}}
  .prov-graph{{background:rgba(63,185,80,.15);color:var(--pos);}}
  .chunk-sec{{font-size:11px;color:var(--muted);margin-bottom:6px;}}
  .chunk-text{{font-size:13px;line-height:1.55;color:#cdd5e0;margin-bottom:10px;}}
  .chunk-meta{{display:flex;align-items:center;gap:10px;font-family:var(--mono);font-size:11px;color:var(--muted);}}
  .conf-bar-wrap{{flex:1;height:4px;background:var(--border);border-radius:2px;}}
  .conf-bar{{display:block;height:100%;background:var(--after);border-radius:2px;}}
  .empty{{color:var(--muted);font-style:italic;padding:12px 0;}}
  .warn-item{{display:flex;gap:10px;align-items:flex-start;padding:10px 14px;border-bottom:1px solid var(--border);}}
  .warn-item:last-child{{border-bottom:none;}}
  .warn-type{{font-family:var(--mono);font-size:11px;font-weight:600;color:var(--neg);white-space:nowrap;
    padding:1px 7px;background:rgba(248,81,73,.1);border-radius:4px;}}
  .int-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;}}
  .int-card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;text-align:center;}}
  .int-card .val{{font-size:36px;font-weight:700;font-family:var(--mono);margin-bottom:4px;}}
  .int-card .lbl{{font-size:11px;color:var(--muted);letter-spacing:1px;text-transform:uppercase;}}
  .int-card.ok .val{{color:var(--pos);}} .int-card.err .val{{color:var(--neg);}} .int-card.warn .val{{color:var(--accent);}}
  .finding{{background:var(--surface);border-left:3px solid var(--accent);border-radius:0 8px 8px 0;
    padding:14px 18px;margin-bottom:10px;font-size:13px;}}
  .finding strong{{color:var(--accent);}}
</style></head><body>
<div class="header">
  <div class="req-badge">{req}</div>
  <h1>GraphRAG <span>Before / After</span> Analysis</h1>
  <p>Research comparison - task: test_generation &nbsp;·&nbsp; req_id: {req}</p>
</div>
<div class="container">
  <div class="timeline">
    <div class="tl-node"><div class="tl-label before">Before</div><div class="tl-ts">{b_ts}</div></div>
    <div class="tl-arrow">-></div>
    <div class="tl-node"><div class="tl-label after">After</div><div class="tl-ts">{a_ts}</div></div>
  </div>
  <div class="section"><div class="section-title">Key Findings</div>{findings()}</div>
  <div class="section"><div class="section-title">Graph Node Counts</div>
    <div class="table-wrap"><table><thead><tr><th>Node Type</th><th>Before</th><th>After</th><th>Δ Delta</th></tr></thead>
    <tbody>{node_rows()}</tbody></table></div></div>
  <div class="section"><div class="section-title">Graph Edge Counts (Relations)</div>
    <div class="table-wrap"><table><thead><tr><th>Edge / Relation</th><th>Before</th><th>After</th><th>Δ Delta</th></tr></thead>
    <tbody>{edge_rows()}</tbody></table></div></div>
  <div class="section"><div class="section-title">Query Result Metrics - req_id: {req}</div>
    <div class="table-wrap"><table><thead><tr><th>Metric</th><th>Before</th><th>After</th><th>Δ Delta</th></tr></thead>
    <tbody>{qrows()}</tbody></table></div></div>
  <div class="section"><div class="section-title">Integrity Check Summary</div>
    <div class="cols2">
      <div><div class="col-label before">Before</div><div class="int-grid">
        <div class="int-card {'ok' if b_i.get('errors',0)==0 else 'err'}"><div class="val">{b_i.get('errors','-')}</div><div class="lbl">Errors</div></div>
        <div class="int-card warn"><div class="val">{b_i.get('warnings','-')}</div><div class="lbl">Warnings</div></div>
        <div class="int-card {'ok' if b_i.get('passed') else 'err'}"><div class="val">{'✓' if b_i.get('passed') else '✗'}</div><div class="lbl">Passed</div></div>
      </div></div>
      <div><div class="col-label after">After</div><div class="int-grid">
        <div class="int-card {'ok' if a_i.get('errors',0)==0 else 'err'}"><div class="val">{a_i.get('errors','-')}</div><div class="lbl">Errors</div></div>
        <div class="int-card warn"><div class="val">{a_i.get('warnings','-')}</div><div class="lbl">Warnings</div></div>
        <div class="int-card {'ok' if a_i.get('passed') else 'err'}"><div class="val">{'✓' if a_i.get('passed') else '✗'}</div><div class="lbl">Passed</div></div>
      </div></div>
    </div></div>
  <div class="section"><div class="section-title">Evidence Chunks Retrieved</div>
    <div class="cols2">
      <div><div class="col-label before">Before</div>{chunk_cards(before,'before')}</div>
      <div><div class="col-label after">After</div>{chunk_cards(after,'after')}</div>
    </div></div>
  <div class="section"><div class="section-title">Retrieval Warnings</div>
    <div class="cols2">
      <div><div class="col-label before">Before</div><div class="table-wrap">{warn_list(before)}</div></div>
      <div><div class="col-label after">After</div><div class="table-wrap">{warn_list(after)}</div></div>
    </div></div>
</div></body></html>"""


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="GraphRAG comparison tool")
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("snapshot")
    s.add_argument("--db",     required=True)
    s.add_argument("--req-id", required=True)
    s.add_argument("--label",  required=True)
    s.add_argument("--out",    default="comparison")

    r = sub.add_parser("report")
    r.add_argument("--before", required=True)
    r.add_argument("--after",  required=True)
    r.add_argument("--out",    default="comparison/comparison_report.html")

    d = sub.add_parser("test_diff",
        help="Export full list of new vs existing test cases as CSV + HTML")
    d.add_argument("--db",     required=True, help="Path to DuckDB database")
    d.add_argument("--before", required=True, help="Path to before snapshot JSON")
    d.add_argument("--after",  required=True, help="Path to after snapshot JSON")
    d.add_argument("--req-id", required=True, help="CRU req_id to analyse")
    d.add_argument("--out",    default="comparison", help="Output directory")

    args = parser.parse_args()

    if args.command == "snapshot":
        capture_snapshot(args.db, args.req_id, args.label, args.out)
    elif args.command == "report":
        generate_report(args.before, args.after, args.out)
    elif args.command == "test_diff":
        run_test_diff(args.db, args.before, args.after, args.req_id, args.out)


if __name__ == "__main__":
    main()