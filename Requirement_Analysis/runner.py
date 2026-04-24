from __future__ import annotations

"""runner.py — CLI entry point and orchestrator for Layer 2 requirement understanding.

Loads Layer 1 output files, runs all Layer 2 modules in order for every
RequirementGroup, collects results and audit data, and writes the output
JSON file.

Usage:
    python runner.py \
        --blocks   path/to/blocks.json \
        --skeleton path/to/skeleton.json \
        --output   path/to/output.json \
        [--model   mistral] \
        [--ollama-url http://localhost:11434] \
        [--timeout 60.0]
"""

import argparse
import datetime
import json
import sys
import traceback
from pathlib import Path

from utils import load_blocks, load_skeleton
from block_grouper import group_blocks, RequirementGroup
from format_detector import detect_format
from labeled_extractor import extract_labeled
from planguage_extractor import extract_planguage
from llm_extractor import extract_llm
from schemas import ExtractedRequirement


_LLM_FORMATS: frozenset[str] = frozenset({"prose", "gherkin"})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="runner.py",
        description="Layer 2 — Requirement Understanding Pipeline",
    )
    parser.add_argument("--blocks",      required=True,  help="Path to Layer 1 blocks JSON file.")
    parser.add_argument("--skeleton",    required=True,  help="Path to Layer 1 skeleton JSON file.")
    parser.add_argument("--output",      required=True,  help="Path for the output JSON file.")
    parser.add_argument("--model",       default="qwen2.5:14b-instruct",                help="Ollama model name.")
    parser.add_argument("--ollama-url",  default="http://localhost:11434", help="Ollama base URL.")
    parser.add_argument("--timeout",     default=60.0, type=float,         help="LLM request timeout (seconds).")
    return parser


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the full Layer 2 requirement understanding pipeline."""
    parser = _build_arg_parser()
    args = parser.parse_args()

    results:       list[ExtractedRequirement] = []
    failed_groups: list[dict]                 = []

    format_counts: dict[str, int] = {
        "labeled":   0,
        "planguage": 0,
        "prose":     0,
        "gherkin":   0,
    }

    # ── Step 1 — Load inputs ──────────────────────────────────────────────
    blocks   = load_blocks(args.blocks)
    skeleton = load_skeleton(args.skeleton)
    print(f"Loaded {len(blocks)} blocks from {args.blocks}")

    # ── Step 2 — Group blocks ─────────────────────────────────────────────
    groups, grouping_stats = group_blocks(blocks, skeleton)
    print(f"Grouped into {len(groups)} requirement groups")

    for warning in grouping_stats.get("warnings", []):
        print(f"  GROUPING WARNING [{warning['code']}] {warning.get('section_path', '')} — {warning['message']}")

    # ── Step 2b — Drop container/preamble groups ──────────────────────────
    # Groups with candidate_req_id=None are structural containers (section
    # intros, class headers). They hold no extractable requirement and will
    # cause the LLM to hallucinate IDs. Drop them here and track the count.
    _before = len(groups)
    groups = [g for g in groups if g.candidate_req_id is not None]
    _dropped = _before - len(groups)
    if _dropped:
        print(f"  [FILTER] Dropped {_dropped} container/preamble groups (no req_id_label)")

    # ── Step 3 — Detect format and extract ────────────────────────────────
    for group in groups:
        # a. Detect format
        group.format = detect_format(group)

        fmt = group.format

        # b. Route to extractor
        try:
            if fmt == "labeled":
                result = extract_labeled(group)
            elif fmt == "planguage":
                result = extract_planguage(group)
            elif fmt in _LLM_FORMATS:
                result = extract_llm(
                    group,
                    model=args.model,
                    ollama_url=args.ollama_url,
                    timeout=args.timeout,
                )
            else:
                error_msg = f"Unknown format: {group.format}"
                print(f"  WARNING — {group.section_path}: {error_msg}")
                failed_groups.append({
                    "section_path":          group.section_path,
                    "section_semantic_type": group.section_semantic_type,
                    "candidate_req_id":      group.candidate_req_id,
                    "format":                group.format,
                    "error":                 error_msg,
                    "traceback":             "",
                })
                print(f"[{fmt}] {group.section_path} → FAILED: {error_msg}")
                continue

            # c. Success — append result
            results.append(result)
            format_counts[fmt] = format_counts.get(fmt, 0) + 1

            # e. Progress line
            print(f"[{fmt}] {group.section_path} → {result.req_id} ({result.confidence})")

        except Exception as exc:
            # d. Failure — record, do not crash
            tb = traceback.format_exc()
            short_msg = str(exc)
            print(f"  WARNING — [{fmt}] {group.section_path} extraction failed: {short_msg}")
            failed_groups.append({
                "section_path":          group.section_path,
                "section_semantic_type": group.section_semantic_type,
                "candidate_req_id":      group.candidate_req_id,
                "format":                group.format,
                "error":                 short_msg,
                "traceback":             tb,
            })
            print(f"[{fmt}] {group.section_path} → FAILED: {short_msg}")

    # ── Step 4 — Assemble output document ─────────────────────────────────
    output_doc: dict = {
        "metadata": {
            "pipeline_stage":  "02_requirement_understanding",
            "generated_at":    datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "blocks_file":     str(args.blocks),
            "skeleton_file":   str(args.skeleton),
            "model":           args.model,
            "total_groups":    len(groups),
            "extracted_count": len(results),
            "failed_count":    len(failed_groups),
        },
        "grouping_stats": grouping_stats,
        "requirements": [r.model_dump() for r in results],
        "failed_groups": failed_groups,
    }

    # ── Step 5 — Write output JSON ────────────────────────────────────────
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(output_doc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Output written to {args.output}")

    # ── Step 6 — Print final summary ──────────────────────────────────────
    print("── Summary ──────────────────────────")
    print(f"  Total groups:     {len(groups)}")
    print(f"  Extracted:        {len(results)}")
    print(f"  Failed:           {len(failed_groups)}")
    print(f"  labeled:          {format_counts.get('labeled', 0)}")
    print(f"  planguage:        {format_counts.get('planguage', 0)}")
    print(f"  prose (llm):      {format_counts.get('prose', 0)}")
    print(f"  gherkin (llm):    {format_counts.get('gherkin', 0)}")

    sys.exit(0 if len(failed_groups) == 0 else 1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
