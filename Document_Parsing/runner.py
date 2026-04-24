"""
runner.py — Pipeline orchestrator for the Autopilot-QA Ingestion Engine.

Entry point for Layer 1.  Runs the full ingestion pipeline for a single SRS PDF:

    toc_parser.parse_toc()
        → body_extractor.extract_body()
            → block_classifier.classify_blocks()

Writes two JSON files to the output directory (default: 01_output/):
    {doc_id}_blocks.json    — flat classified blocks list
    {doc_id}_skeleton.json  — augmented document skeleton + run metadata

Prints a concise audit summary to stdout.
Exits with code 0 on success, 1 on any pipeline error.

This is the ONLY file in Layer 1 that performs file I/O beyond reading the PDF.
"""
import textwrap
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from toc_parser import parse_toc
from body_extractor import extract_body
from block_classifier import classify_blocks


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _write_json(path: Path, data: object, label: str) -> None:
    """Write *data* as indented JSON to *path*, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    size_kb = path.stat().st_size / 1024
    print(f"  [WRITE] {label}: {path}  ({size_kb:.1f} KB)")


def _divider(char: str = "─", width: int = 64) -> str:
    return char * width


# ---------------------------------------------------------------------------
# Audit summary
# ---------------------------------------------------------------------------

def _print_audit(
    doc_meta: dict,
    toc_result: dict,
    body_result: dict,
    classified_blocks: list,
    elapsed_s: float,
) -> None:
    """Print a concise audit summary table to stdout."""
    skeleton  = toc_result["document_skeleton"]
    stats     = body_result["stats"]
    warnings  = toc_result.get("toc_warnings", [])

    skipped     = sum(1 for b in classified_blocks if b.get("skip") is True)
    req_ids     = sum(1 for b in classified_blocks if b.get("structural_role") == "req_id_label")
    low_conf    = sum(1 for b in classified_blocks if b.get("low_confidence_confirmation") is True)
    preamble_n  = sum(1 for b in classified_blocks if b.get("section_path") == "PREAMBLE")
    toc_entries = len(skeleton)
    body_conf   = stats["body_confirmed_sections"]

    from collections import Counter
    sst_counts = Counter(b.get("section_semantic_type") for b in classified_blocks)
    bt_counts  = stats["blocks_by_type"]

    print()
    print(_divider("═"))
    print(f"  AUTOPILOT-QA INGESTION REPORT  —  {doc_meta['doc_id']}")
    print(_divider("═"))
    print(f"  Source        : {doc_meta['source_file']}")
    print(f"  Doc type      : {doc_meta['doc_type']}  |  Module: {doc_meta['module']}  |  Version: {doc_meta['version']}")
    print(f"  Completed at  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ({elapsed_s:.2f}s)")
    print(_divider())

    print("  TOC PARSE")
    print(f"    page_offset       : {toc_result['page_offset']}")
    print(f"    toc entries       : {toc_entries}")
    print(f"    body-confirmed    : {body_conf}  (sub-sections found in body, not in TOC)")
    if warnings:
        for w in warnings:
            print(f"    ⚠  {w['code']} — {w['message']}")
    else:
        print("    warnings          : none")
    print(_divider())

    print("  BODY EXTRACTION")
    print(f"    pages scanned     : {stats['total_pages_scanned']}")
    print(f"    total blocks      : {stats['total_blocks']}")
    print(f"      headings        : {bt_counts.get('heading',   0)}")
    print(f"      paragraphs      : {bt_counts.get('paragraph', 0)}")
    print(f"      list_items      : {bt_counts.get('list_item', 0)}")
    print(f"      tables          : {bt_counts.get('table',     0)}")
    print(f"    images detected   : {len(body_result['images'])}")
    print(_divider())

    print("  BLOCK CLASSIFICATION")
    print(f"    req_id blocks     : {req_ids}  (one per requirement)")
    print(f"    skipped blocks    : {skipped}")
    print(f"    preamble blocks   : {preamble_n}")
    print(f"    low-conf headings : {low_conf}")
    print()
    print("  section_semantic_type distribution:")
    for sst, count in sorted(sst_counts.items(), key=lambda x: -x[1]):
        bar = "█" * min(count // 5, 30)
        print(f"    {sst or 'None':<30} {count:>4}  {bar}")
    print(_divider("═"))
    print()


# ---------------------------------------------------------------------------
# run() — main pipeline function (importable)
# ---------------------------------------------------------------------------

def run(
    pdf_path: str,
    doc_id: str,
    doc_type: str,
    module: str,
    version: str,
    output_dir: str = "01_output",
) -> dict:
    """Execute the full Layer 1 ingestion pipeline for one SRS PDF.

    Args:
        pdf_path:   Path to the input PDF file.
        doc_id:     Unique document identifier (e.g. "DOC-ALI-SRS-v1.0").
        doc_type:   Document type label (e.g. "SRS").
        module:     Module/product name (e.g. "ALI").
        version:    Document version string (e.g. "1.0").
        output_dir: Directory for output JSON files (created if absent).

    Returns:
        dict with keys: blocks, images, skeleton, stats, toc_warnings,
        page_offset, doc_meta, output_blocks_path, output_skeleton_path.

    Raises:
        ValueError: Propagated from toc_parser if no TOC found.
        Exception:  Any pdfplumber or I/O error propagates to the caller.
    """
    t0 = datetime.now()

    doc_meta = {
        "doc_id":      doc_id,
        "doc_type":    doc_type,
        "module":      module,
        "version":     version,
        "source_file": str(pdf_path),
    }

    # ── Step 1: TOC parse ────────────────────────────────────────────────────
    print(f"[1/3] Parsing TOC from: {pdf_path}")
    toc_result      = parse_toc(pdf_path)
    skeleton        = toc_result["document_skeleton"]
    page_offset     = toc_result["page_offset"]
    toc_warnings    = toc_result.get("toc_warnings", [])

    print(f"      TOC entries: {len(skeleton)}  |  page_offset: {page_offset}"
          + (f"  |  {len(toc_warnings)} warning(s)" if toc_warnings else ""))

    # ── Step 2: Body extraction ──────────────────────────────────────────────
    print(f"[2/3] Extracting body blocks …")
    body_result = extract_body(pdf_path, skeleton, page_offset, doc_meta)
    blocks      = body_result["blocks"]
    images      = body_result["images"]
    stats       = body_result["stats"]

    print(f"      pages: {stats['total_pages_scanned']}  |  "
          f"blocks: {stats['total_blocks']}  |  "
          f"images: {len(images)}  |  "
          f"body-confirmed sections: {stats['body_confirmed_sections']}")

    # ── Step 3: Block classification ─────────────────────────────────────────
    print(f"[3/3] Classifying blocks …")
    classified = classify_blocks(blocks, skeleton)
    req_ids    = sum(1 for b in classified if b.get("structural_role") == "req_id_label")
    skipped    = sum(1 for b in classified if b.get("skip") is True)
    print(f"      req_id blocks: {req_ids}  |  skipped: {skipped}")

    # ── Assemble output payloads ─────────────────────────────────────────────
    run_ts = datetime.now().isoformat(timespec="seconds")

    blocks_payload = {
        "meta": {
            "doc_id":        doc_id,
            "doc_type":      doc_type,
            "module":        module,
            "version":       version,
            "source_file":   str(pdf_path),
            "generated_at":  run_ts,
            "total_blocks":  len(classified),
        },
        "blocks": classified,
    }

    skeleton_payload = {
        "meta": {
            "doc_id":                    doc_id,
            "doc_type":                  doc_type,
            "module":                    module,
            "version":                   version,
            "source_file":               str(pdf_path),
            "generated_at":              run_ts,
            "page_offset":               page_offset,
            "toc_entries":               len([v for v in skeleton.values() if v.get("toc_confirmed")]),
            "body_confirmed_sections":   stats["body_confirmed_sections"],
            "toc_warnings":              toc_warnings,
        },
        "document_skeleton": skeleton,
    }

    # ── Write JSON files ─────────────────────────────────────────────────────
    out_dir             = Path(output_dir)
    safe_id             = doc_id.replace("/", "_").replace(" ", "_")
    blocks_path         = out_dir / f"{safe_id}_blocks.json"
    skeleton_path       = out_dir / f"{safe_id}_skeleton.json"

    _write_json(blocks_path,   blocks_payload,   "blocks")
    _write_json(skeleton_path, skeleton_payload, "skeleton")

    # ── Audit summary ─────────────────────────────────────────────────────────
    elapsed = (datetime.now() - t0).total_seconds()
    _print_audit(doc_meta, toc_result, body_result, classified, elapsed)

    return {
        "blocks":               classified,
        "images":               images,
        "skeleton":             skeleton,
        "stats":                stats,
        "toc_warnings":         toc_warnings,
        "page_offset":          page_offset,
        "doc_meta":             doc_meta,
        "output_blocks_path":   str(blocks_path),
        "output_skeleton_path": str(skeleton_path),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="runner.py",
        description=(
            "Autopilot-QA Ingestion Engine — Layer 1 pipeline runner.\n"
            "Parses an SRS PDF and writes classified blocks + skeleton JSON."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python runner.py srs_example.pdf
              python runner.py srs_example.pdf --doc-id DOC-ALI-SRS-v1.0 \\
                  --doc-type SRS --module ALI --version 1.0 --output-dir 01_output
        """),
    )
    p.add_argument("pdf_path",    help="Path to the SRS PDF file to ingest.")
    p.add_argument("--doc-id",    default=None,
                   help="Unique document ID (default: derived from PDF filename).")
    p.add_argument("--doc-type",  default="SRS",
                   help="Document type label, e.g. SRS, SAD, BRD (default: SRS).")
    p.add_argument("--module",    default="UNKNOWN",
                   help="Product/module name stamped on every block (default: UNKNOWN).")
    p.add_argument("--version",   default="1.0",
                   help="Document version string (default: 1.0).")
    p.add_argument("--output-dir", dest="output_dir", default="01_output",
                   help="Directory for output JSON files (default: 01_output).")
    return p


def main(argv=None) -> int:
    """CLI entry point. Returns exit code (0 = success, 1 = error)."""
    # Re-expose textwrap for _build_parser epilog — patch reference
    global textwrap


    parser = _build_parser()
    args   = parser.parse_args(argv)

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print(f"ERROR: PDF not found: {pdf_path}", file=sys.stderr)
        return 1

    # Derive doc_id from filename if not supplied
    doc_id = args.doc_id or f"DOC-{pdf_path.stem.upper().replace(' ', '-')}"

    try:
        run(
            pdf_path    = str(pdf_path),
            doc_id      = doc_id,
            doc_type    = args.doc_type,
            module      = args.module,
            version     = args.version,
            output_dir  = args.output_dir,
        )
        return 0

    except ValueError as exc:
        print(f"PIPELINE ERROR: {exc}", file=sys.stderr)
        return 1

    except Exception as exc:
        print(f"UNEXPECTED ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    import textwrap
    sys.exit(main())
