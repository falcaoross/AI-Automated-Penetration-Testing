# reporter.py - AI Pentest CAU Layer
# Assembles cau_output.json and prints the console summary.
# Gap fix (v1.1): verdict_breakdown added to summary (MATCH/PARTIAL/MISSING/CONFLICT counts).
# Fix (v1.2): uat_status_breakdown now derived from coverage_classification (pipeline truth),
#             not raw PDF status strings. Fully domain-agnostic - driven by config.COVERED_CLASSIFICATIONS.

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_cau_output(
    cau_units: list[dict],
    gap_report: dict,
) -> dict:
    """Assemble the top-level cau_output.json structure."""
    summary = _build_summary(cau_units, gap_report)

    output = {
        'metadata': {
            'pipeline': config.PIPELINE_NAME,
            'version':  config.PIPELINE_VERSION,
        },
        'summary':           summary,
        'cau_units':         cau_units,
        'traceability_gaps': gap_report,
    }
    return output


def write_cau_json(output: dict, out_dir: Path) -> Path:
    """Write cau_output.json to out_dir and return the path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / config.CAU_JSON_FILENAME
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(output, fh, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", path)
    return path


def print_summary(output: dict) -> None:
    """Print a concise run summary to stdout."""
    s    = output['summary']
    gaps = output['traceability_gaps']

    # All labels and sets come from config - nothing is hardcoded in this function.
    covered_label            = getattr(config, 'LABEL_COVERED', 'PASS')
    inferred_classifications = getattr(config, 'INFERRED_CLASSIFICATIONS', set())
    covered_classifications  = getattr(config, 'COVERED_CLASSIFICATIONS', set())

    print("\n" + "=" * 60)
    print(f"  AI Pentest - CAU Layer v{config.PIPELINE_VERSION}")
    print("=" * 60)
    print(f"  CAU units parsed          : {s['total_cau_units']}")
    # uat_status_breakdown is derived from coverage_classification (pipeline truth),
    # not raw PDF status. Covered bucket label = config.LABEL_COVERED.
    uat_bd = s['uat_status_breakdown']
    uat_str = ', '.join(
        f"{k}: {v}" for k, v in sorted(
            uat_bd.items(),
            key=lambda x: (0 if x[0] == covered_label else 1, x[0])
        )
    )
    print(f"  UAT status breakdown      : {{{uat_str}}}")
    print(f"  Coverage classifications  :")
    for label, count in sorted(s['coverage_classification'].items()):
        marker = ' <- inferred (transitive deps)' if label in inferred_classifications else ''
        print(f"    {label:<22}: {count}{marker}")
    print(f"  Total CRUs linked         : {s['total_crus_linked']}")
    print(f"  Total test cases linked   : {s['total_test_cases_linked']}")
    covered_names = ' + '.join(sorted(covered_classifications))
    print(f"  Coverage rate             : {s['coverage_rate_percent']:.1f}%  "
          f"({covered_names} / total)")
    print("-" * 60)
    print(f"  Uncovered CRUs            : {s['uncovered_crus_count']}")
    print(f"  Missing req_ids           : {s['missing_req_ids_count']}")
    print("-" * 60)

    # -- Verdict breakdown - driven entirely by config verdict constants ----
    # Order and labels come from config; no verdict string is hardcoded here.
    vb = s.get('verdict_breakdown', {})
    if vb:
        print(f"  Verdict breakdown (CRU)   : {vb}")
        total_verdicts = sum(vb.values()) or 1
        # Canonical order from config - rename config constants to change labels
        for label in (
            config.VERDICT_MATCH,
            config.VERDICT_PARTIAL,
            config.VERDICT_MISSING,
            config.VERDICT_CONFLICT,
        ):
            count = vb.get(label, 0)
            print(f"    {label:<9}: {count:4d}  ({count / total_verdicts * 100:.1f}%)")
        # Any extra verdict labels beyond the four config constants (future-proof)
        known = {config.VERDICT_MATCH, config.VERDICT_PARTIAL,
                 config.VERDICT_MISSING, config.VERDICT_CONFLICT}
        for label, count in vb.items():
            if label not in known:
                print(f"    {label:<9}: {count:4d}  ({count / total_verdicts * 100:.1f}%)")

    if gaps['missing_req_ids']:
        ids = ', '.join(g['req_id'] for g in gaps['missing_req_ids'])
        print(f"  -> Missing req_ids         : {ids}")

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_summary(cau_units: list[dict], gap_report: dict) -> dict:
    total = len(cau_units)

    coverage_counts: Counter = Counter()
    verdict_counts:  Counter = Counter()
    total_crus = 0
    total_tcs  = 0
    covered_count = 0

    # Covered classifications are defined entirely in config - no hardcoding here.
    covered_classifications = getattr(config, 'COVERED_CLASSIFICATIONS', set())
    # Sentinel for missing/unresolved classification or verdict values
    _unknown = getattr(config, 'LABEL_UNKNOWN', 'UNKNOWN')

    for cau in cau_units:
        cov = cau.get('coverage', {})
        classification = cov.get('classification', _unknown) or _unknown
        coverage_counts[classification] += 1

        # Stamp flat field onto CAU so per-CAU programmatic access works
        cau['coverage_classification'] = classification

        total_crus += cov.get('cru_count', 0)
        total_tcs  += cov.get('test_case_count', 0)

        if classification in covered_classifications:
            covered_count += 1

        # Aggregate verdict counts across all CRU-CAU pairs
        for v in cau.get('cru_verdicts', []):
            verdict = v.get('verdict', _unknown)
            if verdict:
                verdict_counts[verdict] += 1

    coverage_rate = (covered_count / total * 100) if total else 0.0

    # -- UAT status breakdown - derived from pipeline coverage classifications --
    # We do NOT read the raw PDF status field (which can be UNKNOWN, project-
    # specific labels, or simply absent).  Instead we map each classification
    # to a standardised outcome using config.COVERED_CLASSIFICATIONS as the
    # single source of truth - fully domain-agnostic.
    #
    #   covered classification  -> config.LABEL_COVERED  (default: 'PASS')
    #   everything else         -> kept as-is (e.g. NOT_TESTED, PARTIAL_COVERAGE ...)
    #
    # This means the terminal PASS count always equals the dashboard's
    # "covered CAUs" number, regardless of what the PDF originally said.
    _covered_label = getattr(config, 'LABEL_COVERED', 'PASS')
    uat_status: Counter = Counter()
    for label, count in coverage_counts.items():
        if label in covered_classifications:
            uat_status[_covered_label] += count
        else:
            uat_status[label] += count

    return {
        'total_cau_units':         total,
        'uat_status_breakdown':    dict(uat_status),
        'coverage_classification': dict(coverage_counts),
        'total_crus_linked':       total_crus,
        'total_test_cases_linked': total_tcs,
        'uncovered_crus_count':    len(gap_report.get('uncovered_crus', [])),
        'missing_req_ids_count':   len(gap_report.get('missing_req_ids', [])),
        'coverage_rate_percent':   round(coverage_rate, 1),
        'verdict_breakdown':       dict(verdict_counts),
    }