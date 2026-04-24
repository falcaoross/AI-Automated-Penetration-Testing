# html_report.py — Autopilot-QA CAU Layer
# Generates a single downloadable, fully offline HTML traceability report.
# Gap fix (v1.1): CRU Verdict Detail section added to each CAU card.

from __future__ import annotations

import html
import json
import logging
from pathlib import Path

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Colour scheme (coverage classification → badge colour)
# ---------------------------------------------------------------------------
BADGE_COLOURS = {
    'FULL_COVERAGE':     ('#1a7a3f', '#e6f4ec'),
    'INFERRED_PARTIAL':  ('#1565c0', '#e3f2fd'),   # blue — confident but indirect
    'PARTIAL_COVERAGE':  ('#7a5c00', '#fff8e1'),
    'FAILED_COVERAGE':   ('#b71c1c', '#fdecea'),
    'NO_TEST_CASE':      ('#4a148c', '#ede7f6'),
    'NO_CRU_MATCH':      ('#bf360c', '#fbe9e7'),
    'NOT_TESTED':        ('#37474f', '#eceff1'),
    'UNKNOWN':           ('#424242', '#f5f5f5'),
}
STATUS_COLOURS = {
    'PASS':        ('#1a7a3f', '#e6f4ec'),
    'FAIL':        ('#b71c1c', '#fdecea'),
    'PARTIAL':     ('#7a5c00', '#fff8e1'),
    'NOT_TESTED':  ('#37474f', '#eceff1'),   # Gap fix
    'INFERRED':    ('#1565c0', '#e3f2fd'),
}

# Gap fix: verdict colour scheme
VERDICT_COLOURS = {
    'MATCH':    ('#1a7a3f', '#e6f4ec'),
    'PARTIAL':  ('#7a5c00', '#fff8e1'),
    'MISSING':  ('#4a148c', '#ede7f6'),
    'CONFLICT': ('#b71c1c', '#fdecea'),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_html_report(output: dict, out_dir: Path) -> Path:
    """Render the full HTML report and write it to out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / config.HTML_FILENAME
    html_str = _render(output)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(html_str)
    logger.info("Wrote HTML report: %s", path)
    return path


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _render(output: dict) -> str:
    summary   = output.get('summary', {})
    cau_units = output.get('cau_units', [])
    gaps      = output.get('traceability_gaps', {})
    meta      = output.get('metadata', {})

    sections = [
        _head(),
        '<body>',
        _header(meta, summary),
        '<main class="container">',
        _summary_dashboard(summary),
        '<section id="cau-cards">',
        '<h2 class="section-title">CAU Traceability Cards</h2>',
    ]
    for cau in cau_units:
        sections.append(_cau_card(cau))
    sections += [
        '</section>',
        _gap_section(gaps),
        '</main>',
        _footer(),
        '</body>',
        '</html>',
    ]
    return '\n'.join(sections)


# ---------------------------------------------------------------------------
# Page skeleton
# ---------------------------------------------------------------------------

def _head() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Autopilot-QA — CAU Traceability Report</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f0f2f5; color: #1a1a2e; font-size: 14px; line-height: 1.6; }
.container { max-width: 1100px; margin: 0 auto; padding: 24px 16px 60px; }
.report-header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
  color: #fff; padding: 28px 32px; display: flex; justify-content: space-between;
  align-items: flex-end; flex-wrap: wrap; gap: 12px; }
.report-header h1 { font-size: 22px; font-weight: 700; letter-spacing: .4px; }
.report-header .meta { font-size: 12px; opacity: .75; text-align: right; }
.dashboard { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 16px; margin: 24px 0; }
.stat-card { background: #fff; border-radius: 10px; padding: 18px 20px;
  box-shadow: 0 1px 4px rgba(0,0,0,.08); }
.stat-card .val { font-size: 32px; font-weight: 800; color: #0f3460; }
.stat-card .lbl { font-size: 11px; color: #666; text-transform: uppercase;
  letter-spacing: .6px; margin-top: 4px; }
.coverage-bar-wrap { background:#fff; border-radius:10px; padding:20px 24px;
  box-shadow:0 1px 4px rgba(0,0,0,.08); margin-bottom:24px; }
.coverage-bar-wrap h3 { font-size:13px; color:#444; margin-bottom:10px; }
.bar-track { height:18px; background:#e5e7eb; border-radius:9px; overflow:hidden; }
.bar-fill  { height:100%; background:linear-gradient(90deg,#1a7a3f,#4caf80);
  border-radius:9px; transition: width .6s ease; }
.bar-label { font-size:12px; color:#555; margin-top:6px; }
.section-title { font-size:18px; font-weight:700; color:#0f3460; margin:28px 0 14px;
  padding-bottom:6px; border-bottom:2px solid #e0e4ef; }
.cau-card { background:#fff; border-radius:12px; margin-bottom:18px;
  box-shadow:0 1px 4px rgba(0,0,0,.08); overflow:hidden; }
.cau-card-header { padding:14px 20px; display:flex; justify-content:space-between;
  align-items:center; flex-wrap:wrap; gap:8px;
  border-bottom:1px solid #f0f2f5; cursor:pointer; user-select:none; }
.cau-card-header:hover { background:#f7f9ff; }
.cau-card-header .ids { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
.cau-card-header .title { font-weight:600; color:#1a1a2e; font-size:14px; }
.cau-card-body { padding:16px 20px; display:none; }
.cau-card-body.open { display:block; }
.badge { display:inline-block; padding:2px 9px; border-radius:20px; font-size:11px;
  font-weight:700; white-space:nowrap; }
.info-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:14px; }
.info-block label { font-size:10px; font-weight:700; text-transform:uppercase;
  letter-spacing:.7px; color:#888; display:block; margin-bottom:3px; }
.info-block p, .info-block ul { font-size:13px; color:#333; }
.info-block ul { padding-left:16px; }
.chain-table { width:100%; border-collapse:collapse; font-size:12.5px; margin-top:8px; }
.chain-table th { background:#f0f2f5; text-align:left; padding:6px 10px;
  font-size:11px; text-transform:uppercase; letter-spacing:.5px; color:#555; }
.chain-table td { padding:6px 10px; border-bottom:1px solid #f0f2f5; color:#333; vertical-align:top; }
.chain-table tr:last-child td { border-bottom:none; }
.chain-table tr:hover td { background:#f9fafb; }
.sub-heading { font-size:12px; font-weight:700; color:#0f3460; text-transform:uppercase;
  letter-spacing:.6px; margin:14px 0 6px; }
.coverage-box { background:#f7f9ff; border-left:4px solid #0f3460;
  border-radius:0 8px 8px 0; padding:10px 14px; margin-top:14px; font-size:13px; }
.coverage-box strong { display:block; margin-bottom:3px; }
.verdict-box { background:#fafbff; border-left:4px solid #5c6bc0;
  border-radius:0 8px 8px 0; padding:10px 14px; margin-top:10px; font-size:13px; }
.gap-card { background:#fff; border-radius:12px; margin-bottom:12px;
  box-shadow:0 1px 4px rgba(0,0,0,.08); overflow:hidden; }
.gap-card-header { background:#fff3e0; padding:12px 18px; font-weight:700;
  font-size:13px; color:#bf360c; border-bottom:1px solid #ffe0b2; }
.gap-table { width:100%; border-collapse:collapse; font-size:12.5px; }
.gap-table th { background:#fafafa; text-align:left; padding:7px 14px;
  font-size:11px; text-transform:uppercase; letter-spacing:.5px; color:#555; border-bottom:1px solid #e5e7eb; }
.gap-table td { padding:7px 14px; border-bottom:1px solid #f0f2f5; color:#333; }
.gap-table tr:last-child td { border-bottom:none; }
.report-footer { text-align:center; font-size:11px; color:#aaa; margin-top:40px; }
.toggle-arrow { font-size:18px; color:#888; transition:transform .25s; }
.toggle-arrow.open { transform:rotate(90deg); }
/* verdict summary bar */
.verdict-bar { display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }
</style>
</head>"""


def _header(meta: dict, summary: dict) -> str:
    pipeline = html.escape(meta.get('pipeline', config.PIPELINE_NAME))
    version  = html.escape(meta.get('version', config.PIPELINE_VERSION))
    rate     = summary.get('coverage_rate_percent', 0)
    return f"""<header class="report-header">
  <div>
    <h1>&#128202; {pipeline}</h1>
    <div style="font-size:13px;opacity:.8;margin-top:4px;">Traceability Report &nbsp;|&nbsp; v{version}</div>
  </div>
  <div class="meta">
    Coverage Rate: <strong style="font-size:20px;">{rate:.1f}%</strong><br>
    {summary.get('total_cau_units', 0)} CAU units
  </div>
</header>"""


def _summary_dashboard(s: dict) -> str:
    status   = s.get('uat_status_breakdown', {})
    cov      = s.get('coverage_classification', {})
    rate     = s.get('coverage_rate_percent', 0)
    verdicts = s.get('verdict_breakdown', {})

    # ── Covered count — domain-agnostic ──────────────────────────────────
    # Read which classifications count as "covered" from config at runtime.
    # This means if COVERED_CLASSIFICATIONS changes in config.py, the KPI
    # card updates automatically without touching html_report.py.
    covered_classifications: set[str] = getattr(
        config, 'COVERED_CLASSIFICATIONS', {'FULL_COVERAGE'}
    )
    covered_count = sum(
        count for label, count in cov.items()
        if label in covered_classifications
    )

    # Build human-readable breakdown of what makes up the covered count
    # e.g. "25 FULL + 3 INFERRED" — derived purely from live data + config
    covered_parts = ' + '.join(
        f'{cov[label]} {label.replace("_", " ")}'
        for label in sorted(covered_classifications)
        if cov.get(label, 0) > 0
    )

    cards_html = '\n'.join([
        _stat_card(str(s.get('total_cau_units', 0)),        'CAU Units'),
        _stat_card(str(s.get('total_crus_linked', 0)),       'CRUs Linked'),
        _stat_card(str(s.get('total_test_cases_linked', 0)), 'Test Cases Linked'),
        _stat_card(str(covered_count),                       'Covered CAUs'),
        _stat_card(str(status.get('FAIL', 0)),               'UAT FAIL'),
        _stat_card(str(status.get('PARTIAL', 0)),            'UAT PARTIAL'),
        _stat_card(str(status.get('NOT_TESTED', 0)),         'NOT TESTED'),
        _stat_card(str(s.get('uncovered_crus_count', 0)),    'Uncovered CRUs'),
    ])

    fill_width = min(int(rate), 100)

    # Coverage classification badges — one per label, colour from BADGE_COLOURS
    cov_rows = ''
    for k, v in cov.items():
        col, bg = BADGE_COLOURS.get(k, ('#424242', '#f5f5f5'))
        cov_rows += (
            f'<span class="badge" style="color:{col};background:{bg};margin-right:8px">'
            f'{html.escape(k)}: {v}</span>'
        )

    # Bar label — fully derived from live data, no hardcoded classification names
    bar_label = (
        f'{rate:.1f}% covered &nbsp;·&nbsp; '
        f'<span style="font-weight:600">{covered_parts} = '
        f'{covered_count} / {s.get("total_cau_units", 0)} CAUs</span>'
        f' &nbsp;·&nbsp; {cov_rows}'
    )

    # Verdict summary bar
    verdict_bar = ''
    if verdicts:
        verdict_bar = (
            '<div style="margin-top:16px">'
            '<strong style="font-size:12px;color:#444">CRU Verdict Summary:</strong>'
            '<div class="verdict-bar" style="margin-top:6px">'
        )
        for vk, vv in sorted(verdicts.items()):
            vc, vbg = VERDICT_COLOURS.get(vk, ('#424242', '#f5f5f5'))
            verdict_bar += (
                f'<span class="badge" style="color:{vc};background:{vbg};'
                f'font-size:12px;padding:4px 12px">'
                f'{html.escape(vk)}: {vv}</span>'
            )
        verdict_bar += '</div></div>'

    # Inferred note — only shown when any inferred classifications exist in data
    inferred_labels = [
        label for label in covered_classifications
        if label != 'FULL_COVERAGE' and cov.get(label, 0) > 0
    ]
    inferred_note = ''
    if inferred_labels:
        parts = ', '.join(
            f'{cov[label]} {label}' for label in inferred_labels
        )
        inferred_note = (
            f'<div style="margin-top:10px;font-size:11px;color:#1565c0;">'
            f'&#9432;&nbsp; {parts} — no direct UAT entry; '
            f'coverage confirmed via transitive dependency chain. '
            f'Counted toward coverage rate.'
            f'</div>'
        )

    return f"""<div class="dashboard">{cards_html}</div>
<div class="coverage-bar-wrap">
  <h3>Coverage Rate</h3>
  <div class="bar-track"><div class="bar-fill" style="width:{fill_width}%"></div></div>
  <div class="bar-label">{bar_label}</div>
  {inferred_note}
  {verdict_bar}
</div>"""


def _stat_card(value: str, label: str) -> str:
    return f"""<div class="stat-card">
  <div class="val">{html.escape(value)}</div>
  <div class="lbl">{html.escape(label)}</div>
</div>"""


def _cau_card(cau: dict) -> str:
    uat_id         = html.escape(cau.get('uat_id', ''))
    cau_id         = html.escape(cau.get('cau_id', ''))
    title          = html.escape(cau.get('title', ''))
    status         = cau.get('status', '').upper()
    coverage       = cau.get('coverage', {})
    classification = coverage.get('classification', 'UNKNOWN')
    actor          = html.escape(cau.get('actor_class', '') or '')

    s_col, s_bg = STATUS_COLOURS.get(status, ('#424242', '#f5f5f5'))
    c_col, c_bg = BADGE_COLOURS.get(classification, ('#424242', '#f5f5f5'))

    status_badge = (
        f'<span class="badge" style="color:{s_col};background:{s_bg}">{html.escape(status)}</span>'
        if status else ''
    )
    cov_badge = (
        f'<span class="badge" style="color:{c_col};background:{c_bg}">'
        f'{html.escape(classification)}</span>'
    )

    card_id   = f'card-{cau_id.replace(" ", "-")}'
    body_html = _cau_card_body(cau, coverage)

    return f"""<div class="cau-card" id="{card_id}">
  <div class="cau-card-header" onclick="toggleCard('{card_id}')">
    <div class="ids">
      <span style="font-size:11px;color:#888">{html.escape(cau_id)}</span>
      <span style="font-size:11px;color:#aaa">·</span>
      <span style="font-size:11px;color:#888">{html.escape(uat_id)}</span>
      <span class="title">{title}</span>
      {f'<span style="font-size:11px;color:#999">({actor})</span>' if actor else ''}
    </div>
    <div style="display:flex;gap:8px;align-items:center">
      {status_badge} {cov_badge}
      <span class="toggle-arrow" id="arrow-{card_id}">&#9656;</span>
    </div>
  </div>
  <div class="cau-card-body" id="body-{card_id}">
    {body_html}
  </div>
</div>"""


def _cau_card_body(cau: dict, coverage: dict) -> str:
    parts = []

    desc     = html.escape(cau.get('description') or '')
    exp      = html.escape(cau.get('expected_result') or '')
    act      = html.escape(cau.get('actual_result') or '')
    obs      = html.escape(cau.get('tester_observations') or '')
    req_ids  = ', '.join(html.escape(r or '') for r in cau.get('req_ids', []))
    preconds = cau.get('preconditions', [])
    steps    = cau.get('test_steps', [])

    parts.append('<div class="info-grid">')
    if desc:
        parts.append(f'<div class="info-block"><label>Description</label><p>{desc}</p></div>')
    if req_ids:
        parts.append(f'<div class="info-block"><label>Requirement IDs</label><p>{req_ids}</p></div>')
    if exp:
        parts.append(f'<div class="info-block"><label>Expected Result</label><p>{exp}</p></div>')
    if act:
        parts.append(f'<div class="info-block"><label>Actual Result</label><p>{act}</p></div>')
    if obs:
        parts.append(f'<div class="info-block"><label>Tester Observations</label><p>{obs}</p></div>')
    parts.append('</div>')

    if preconds:
        items = ''.join(f'<li>{html.escape(str(p))}</li>' for p in preconds)
        parts.append(f'<div class="sub-heading">Pre-conditions</div><ul style="font-size:13px;padding-left:20px">{items}</ul>')

    if steps:
        items = ''.join(f'<li>{html.escape(str(s))}</li>' for s in steps)
        parts.append(f'<div class="sub-heading">Test Steps</div><ul style="font-size:13px;padding-left:20px">{items}</ul>')

    # Linked requirements
    linked_reqs = cau.get('linked_requirements', [])
    if linked_reqs:
        parts.append('<div class="sub-heading">Linked Requirements</div>')
        parts.append(_simple_table(
            ['Req ID', 'Title', 'Section'],
            [[r.get('req_id', ''), r.get('title', ''), r.get('section_path', '')] for r in linked_reqs],
        ))

    # Linked CRUs — now includes verdict column
    linked_crus = cau.get('linked_crus', [])
    if linked_crus:
        parts.append('<div class="sub-heading">Linked CRUs</div>')
        rows = []
        for c in linked_crus:
            verdict    = c.get('verdict', '')
            vc, vbg    = VERDICT_COLOURS.get(verdict, ('#424242', '#f5f5f5'))
            verdict_badge = (
                f'<span class="badge" style="color:{vc};background:{vbg}">{html.escape(verdict)}</span>'
                if verdict else '—'
            )
            overlap = f"{c.get('overlap_ratio', 0.0):.2f}" if verdict else '—'
            rows.append([
                c.get('cru_id', ''),
                c.get('parent_requirement_id', ''),
                c.get('actor', ''),
                c.get('action', ''),
                c.get('type', ''),
                verdict_badge,   # raw HTML
                overlap,
            ])
        parts.append(_table_with_raw(
            ['CRU ID', 'Parent Req', 'Actor', 'Action', 'Type', 'Verdict', 'Overlap'],
            rows,
            raw_col_index=5,   # verdict badge column is raw HTML
        ))

    # Linked test cases
    linked_tcs = cau.get('linked_test_cases', [])
    if linked_tcs:
        parts.append('<div class="sub-heading">Linked Test Cases</div>')
        parts.append(_simple_table(
            ['Test ID', 'CRU ID', 'Type', 'Title'],
            [[t.get('test_id', ''), t.get('cru_id', ''),
              t.get('test_type', ''), t.get('test_title', '')] for t in linked_tcs],
        ))

    # ── Gap fix: CRU Verdict Detail section ──────────────────────────────
    verdicts = cau.get('cru_verdicts', [])
    if verdicts:
        parts.append('<div class="sub-heading">CRU Verdict Detail</div>')
        parts.append('<div class="verdict-box">')
        verdict_rows = []
        for v in verdicts:
            vc, vbg = VERDICT_COLOURS.get(v.get('verdict', ''), ('#424242', '#f5f5f5'))
            badge = (
                f'<span class="badge" style="color:{vc};background:{vbg}">'
                f'{html.escape(v.get("verdict", ""))}</span>'
            )
            neg_icon = '&#9888;' if v.get('negation_found') else ''
            verdict_rows.append([
                v.get('cru_id', ''),
                badge,                                           # raw HTML
                f"{v.get('overlap_ratio', 0.0):.2f}",
                html.escape(v.get('spec_field_used', '')),
                html.escape((v.get('spec_text_used') or '')[:80]),
                html.escape((v.get('evidence_text_used') or '')[:80]),
                neg_icon,                                        # raw HTML
            ])
        parts.append(_table_with_raw(
            ['CRU ID', 'Verdict', 'Overlap', 'Spec Field', 'Spec Text (80c)', 'Evidence (80c)', '&#9888;'],
            verdict_rows,
            raw_col_index=1,    # verdict badge
            raw_col_index_2=6,  # negation icon
        ))
        parts.append('</div>')

    # Coverage box
    c_col, c_bg = BADGE_COLOURS.get(coverage.get('classification', 'UNKNOWN'), ('#424242', '#f5f5f5'))
    summary_text = html.escape(coverage.get('summary', ''))
    unmatched    = ', '.join(html.escape(u) for u in coverage.get('unmatched_req_ids', []))
    parts.append(
        f'<div class="coverage-box">'
        f'<strong><span class="badge" style="color:{c_col};background:{c_bg}">'
        f'{html.escape(coverage.get("classification", ""))}</span></strong>'
        f'{summary_text}'
        + (f'<br><span style="color:#b71c1c;font-size:12px">Unmatched req_ids: {unmatched}</span>' if unmatched else '')
        + '</div>'
    )

    return '\n'.join(parts)


def _simple_table(headers: list[str], rows: list[list]) -> str:
    th_html   = ''.join(f'<th>{html.escape(h)}</th>' for h in headers)
    rows_html = ''
    for row in rows:
        cells = ''.join(
            f'<td>{html.escape(str(c) if c is not None else "")}</td>'
            for c in row
        )
        rows_html += f'<tr>{cells}</tr>'
    return (
        f'<table class="chain-table"><thead><tr>{th_html}</tr></thead>'
        f'<tbody>{rows_html}</tbody></table>'
    )


def _table_with_raw(
    headers: list[str],
    rows: list[list],
    raw_col_index: int = -1,
    raw_col_index_2: int = -1,
) -> str:
    """
    Like _simple_table but allows up to two columns to contain raw HTML
    (not escaped). Used for verdict badge and negation icon columns.
    """
    th_html   = ''.join(f'<th>{h}</th>' for h in headers)
    rows_html = ''
    for row in rows:
        cells = ''
        for idx, c in enumerate(row):
            if idx in (raw_col_index, raw_col_index_2):
                cells += f'<td>{c if c is not None else ""}</td>'
            else:
                cells += f'<td>{html.escape(str(c) if c is not None else "")}</td>'
        rows_html += f'<tr>{cells}</tr>'
    return (
        f'<table class="chain-table"><thead><tr>{th_html}</tr></thead>'
        f'<tbody>{rows_html}</tbody></table>'
    )


def _gap_section(gaps: dict) -> str:
    uncovered = gaps.get('uncovered_crus', [])
    missing   = gaps.get('missing_req_ids', [])

    parts = ['<section id="gap-report">', '<h2 class="section-title">&#9888;&#65039; Traceability Gap Report</h2>']

    if not uncovered and not missing:
        parts.append('<p style="color:#1a7a3f;font-weight:600">&#10003; No traceability gaps detected.</p>')
    else:
        if missing:
            parts.append('<div class="gap-card">')
            parts.append('<div class="gap-card-header">Missing Req IDs — Referenced in UAT but not found in CRU file</div>')
            rows_html = ''
            for g in missing:
                rows_html += (
                    f'<tr><td>{html.escape(g.get("req_id") or "")}</td>'
                    f'<td>{html.escape(g.get("referenced_in_uat") or "")}</td>'
                    f'<td>{html.escape(g.get("reason") or "")}</td></tr>'
                )
            parts.append(
                f'<table class="gap-table"><thead><tr>'
                f'<th>Req ID</th><th>Referenced In</th><th>Reason</th>'
                f'</tr></thead><tbody>{rows_html}</tbody></table>'
            )
            parts.append('</div>')

        if uncovered:
            parts.append('<div class="gap-card" style="margin-top:14px">')
            parts.append('<div class="gap-card-header">Uncovered CRUs — In CRU file but never referenced by any UAT test case</div>')
            rows_html = ''
            for g in uncovered:
                rows_html += (
                    f'<tr><td>{html.escape(g.get("cru_id") or "")}</td>'
                    f'<td>{html.escape(g.get("parent_requirement_id") or "")}</td>'
                    f'<td>{html.escape(g.get("actor") or "")}</td>'
                    f'<td>{html.escape(g.get("action") or "")}</td>'
                    f'<td>{html.escape(g.get("reason") or "")}</td></tr>'
                )
            parts.append(
                f'<table class="gap-table"><thead><tr>'
                f'<th>CRU ID</th><th>Parent Req</th><th>Actor</th><th>Action</th><th>Reason</th>'
                f'</tr></thead><tbody>{rows_html}</tbody></table>'
            )
            parts.append('</div>')

    parts.append('</section>')
    return '\n'.join(parts)


def _footer() -> str:
    return (
        '<footer class="report-footer">'
        f'Generated by {html.escape(config.PIPELINE_NAME)} v{html.escape(config.PIPELINE_VERSION)}'
        '</footer>'
        '<script>'
        'function toggleCard(id){'
        '  var body=document.getElementById("body-"+id);'
        '  var arrow=document.getElementById("arrow-"+id);'
        '  if(body.classList.contains("open")){'
        '    body.classList.remove("open");'
        '    arrow.classList.remove("open");'
        '  } else {'
        '    body.classList.add("open");'
        '    arrow.classList.add("open");'
        '  }'
        '}'
        '</script>'
    )