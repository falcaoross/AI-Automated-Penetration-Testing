#!/usr/bin/env python3
# runner.py — Autopilot-QA CAU Layer
# CLI entry point. Orchestrates all pipeline steps.
#
# Usage (filesystem paths):
#   python runner.py \
#     --uat      path/to/uat.pdf \
#     --crus     path/to/cru_units.json \
#     --tests    path/to/optimized_test_cases.json \
#     --out      cau_output/
#
# All three inputs also accept raw bytes if called programmatically (UI mode).

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Union

import config
import ingest_uat
import linker
import reporter
import html_report

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(name)s — %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('runner')


# ---------------------------------------------------------------------------
# Public programmatic API (for UI / upload mode)
# ---------------------------------------------------------------------------

def run_pipeline(
    uat_source: Union[str, Path, bytes],
    crus_source: Union[str, Path, bytes],
    tests_source: Union[str, Path, bytes],
    out_dir: Union[str, Path] = 'output',
) -> dict:
    """
    Execute the full CAU pipeline and return the final output dict.
    Writes cau_output.json and cau_traceability_report.html into out_dir.

    Accepts file paths OR raw bytes for all three inputs (transparent dual-mode).
    """
    out_dir = Path(out_dir)
    logger.info("=== Autopilot-QA CAU Layer — START ===")

    # ── Step 1: Ingest UAT PDF ────────────────────────────────────────────
    logger.info("[1/5] Ingesting UAT PDF …")
    raw_caus, req_id_pattern = ingest_uat.ingest_uat_pdf(uat_source)

    if not raw_caus:
        logger.error("No CAU objects extracted from UAT PDF. Check UAT_HEADER_PATTERN.")
        sys.exit(1)

    # ── Step 2: Load CRUs and test cases ─────────────────────────────────
    logger.info("[2/5] Loading CRUs and test cases …")
    crus = linker.load_crus(crus_source)
    test_cases = linker.load_test_cases(tests_source)
    logger.info("Loaded %d CRUs, %d test cases", len(crus), len(test_cases))

    # ── Step 3: Build indexes ─────────────────────────────────────────────
    logger.info("[3/5] Building linkage indexes …")
    req_to_crus, cru_to_tests = linker.build_indexes(crus, test_cases)
    cru_meta = {(c.get('cru_id') or '').upper(): c for c in crus}
    req_deps = linker.build_dependency_index(crus)

    # ── Step 4: Link each CAU ─────────────────────────────────────────────
    logger.info("[4/5] Linking CAU → CRU → test cases …")
    linked_cau_units: list[dict] = []
    for raw in raw_caus:
        linked = linker.link_cau(raw, req_to_crus, cru_to_tests, cru_meta)
        linked_cau_units.append(linked)

    synthetic_caus = linker.infer_coverage(
        linked_cau_units, crus, req_to_crus, cru_to_tests, req_deps,
    )
    linked_cau_units.extend(synthetic_caus)
    inferred_req_ids = {c['req_ids'][0] for c in synthetic_caus if c.get('req_ids')}

    gap_report = linker.compute_gap_report(
        linked_cau_units, crus, req_to_crus, inferred_req_ids=inferred_req_ids,
    )

    # ── Step 5: Build outputs ─────────────────────────────────────────────
    logger.info("[5/5] Writing outputs to %s …", out_dir)
    output = reporter.build_cau_output(linked_cau_units, gap_report)

    reporter.write_cau_json(output, out_dir)
    html_report.generate_html_report(output, out_dir)
    reporter.print_summary(output)

    logger.info("=== Autopilot-QA CAU Layer — DONE ===")
    return output


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='runner.py',
        description='Autopilot-QA CAU Layer — UAT → CRU → Test Case Traceability Pipeline',
    )
    parser.add_argument(
        '--uat', required=True, metavar='PATH',
        help='Path to the UAT PDF file',
    )
    parser.add_argument(
        '--crus', required=True, metavar='PATH',
        help='Path to cru_units.json',
    )
    parser.add_argument(
        '--tests', required=True, metavar='PATH',
        help='Path to optimized_test_cases.json',
    )
    parser.add_argument(
        '--out', default=config.DEFAULT_OUTPUT_DIR, metavar='DIR',
        help=f'Output directory (default: {config.DEFAULT_OUTPUT_DIR})',
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable DEBUG-level logging',
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_pipeline(
        uat_source=args.uat,
        crus_source=args.crus,
        tests_source=args.tests,
        out_dir=args.out,
    )