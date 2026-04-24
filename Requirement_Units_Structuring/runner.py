#!/usr/bin/env python3
"""runner.py — Layer 3 entry point.

Orchestrates the full CRU normalization pipeline:
  1. Load Layer 2 requirements.json and Layer 1 skeleton.json
  2. Build CRUs via cru_builder.build_crus()
  3. Validate CRUs via cru_validator.validate_crus()
  4. Write output/cru_units.json

Exit codes:
  0 — success (all CRUs valid)
  1 — validation failures present (cru_units.json still written)
  2 — unrecoverable I/O or schema error
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from cru_builder import build_crus
from cru_validator import validate_crus


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Layer 3 — Build and validate Canonical Requirement Units.",
    )
    parser.add_argument(
        "--requirements",
        default="../Requirement_Analysis/output/requirements.json",
        help="Path to Layer 2 requirements.json",
    )
    parser.add_argument(
        "--skeleton",
        default="../Document_Parsing/01_output/DOC-SRS_EXAMPLE_skeleton.json",
        help="Path to Layer 1 skeleton.json",
    )
    parser.add_argument(
        "--output",
        default="output/cru_units.json",
        help="Destination path for cru_units.json",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path, label: str) -> dict:
    if not path.exists():
        print(f"[ERROR] {label} not found: {path}", file=sys.stderr)
        sys.exit(2)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Invalid JSON in {label}: {exc}", file=sys.stderr)
        sys.exit(2)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    req_path      = Path(args.requirements)
    skeleton_path = Path(args.skeleton)
    output_path   = Path(args.output)

    # ── Load inputs ──────────────────────────────────────────────────────
    req_data = _load_json(req_path, "requirements.json")
    skeleton = _load_json(skeleton_path, "skeleton.json")

    requirements   = req_data.get("requirements", [])
    known_req_ids  = {r["req_id"] for r in requirements}
    # skeleton paths live under document_skeleton, not at root level
    skeleton_paths = set(skeleton.get("document_skeleton", {}).keys())

    print(f"[INFO] Loaded {len(requirements)} requirements from {req_path}")
    print(f"[INFO] Loaded {len(skeleton_paths)} skeleton paths from {skeleton_path}")

    # ── Build ────────────────────────────────────────────────────────────
    crus, builder_flags = build_crus(requirements)
    print(f"[INFO] Built {len(crus)} CRUs ({len(builder_flags)} builder flags)")

    # ── Validate ─────────────────────────────────────────────────────────
    result = validate_crus(crus, builder_flags, known_req_ids, skeleton_paths)

    if result.clean:
        print(f"[INFO] Validation passed — all {result.total} CRUs are valid")
    else:
        print(
            f"[WARN] Validation found {result.invalid_count} invalid CRU(s) "
            f"across {len(result.flags)} flag(s)",
            file=sys.stderr,
        )
        for flag in result.flags:
            print(
                f"  [{flag['rule']}] {flag['code']} — {flag['cru_id']}: {flag['message']}",
                file=sys.stderr,
            )

    # ── Assemble output ──────────────────────────────────────────────────
    # Merge builder flags + validation flags into one audit log
    all_flags = builder_flags + result.flags

    output = {
        "metadata": {
            "pipeline_stage":   "03_cru_normalization",
            "generated_at":     datetime.now(timezone.utc).isoformat(),
            "requirements_file": str(req_path),
            "skeleton_file":    str(skeleton_path),
            "total_crus":       result.total,
            "invalid_crus":     result.invalid_count,
            "clean":            result.clean,
            "builder_flags":    len(builder_flags),
            "validation_flags": len(result.flags),
        },
        "cru_units": [c.to_dict() for c in crus],
        "audit_flags": all_flags,
    }

    _write_json(output_path, output)
    print(f"[INFO] Written → {output_path}")

    sys.exit(0 if result.clean else 1)


if __name__ == "__main__":
    main()