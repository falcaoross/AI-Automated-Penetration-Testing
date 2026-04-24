"""
Layer 4: Semantic Chunking & Domain Tagging

Reads Layer 3 cru_units.json, groups CRUs into LLM-friendly semantic chunks,
attaches one fixed application domain for the whole run, generates lightweight
capability tags, and preserves traceability for downstream test generation.

Design rules:
- Deterministic only. No LLM calls here.
- One project/run has one application domain.
- Chunk primarily by parent_requirement_id.
- Never mix unrelated requirements in one chunk.
- Preserve title + acceptance_criteria in chunk CRU payload for Layer 5.
- Preserve traceability strongly enough for GraphRAG and audit.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from domains import APPLICATION_DOMAINS


DEFAULT_APPLICATION_DOMAIN = "Restaurant/Food Service"

CONFIG = {
    "max_crus_per_chunk": 4,
    "min_crus_per_chunk": 1,
}


CAPABILITY_TAG_PATTERNS = {
    "Authentication": [
        "login", "log in", "register", "signup", "sign up",
        "password", "credential", "authenticate", "account"
    ],
    "Search & Filtering": [
        "search", "filter", "query", "sort", "find"
    ],
    "Location & Maps": [
        "map", "location", "gps", "distance", "position", "pin"
    ],
    "Restaurant Discovery": [
        "restaurant", "dish", "menu", "food", "price"
    ],
    "Reservation & Booking": [
        "reserve", "reservation", "book", "booking", "table"
    ],
    "Profile & Account Management": [
        "profile", "account", "update profile", "edit profile"
    ],
    "CRUD Operations": [
        "create", "update", "delete", "edit", "remove", "insert"
    ],
    "Notifications": [
        "notify", "notification", "email", "mail", "alert"
    ],
    "Reporting & Admin": [
        "admin", "administrator", "report", "manage", "dashboard"
    ],
    "Performance": [
        "response time", "latency", "concurrent", "throughput", "seconds"
    ],
    "Security": [
        "encrypt", "hash", "secure", "credential", "auth", "password"
    ],
    "Reliability": [
        "uptime", "backup", "recovery", "availability", "failure"
    ],
    "Portability": [
        "browser", "platform", "compatibility", "os", "device"
    ],
    "User Interface": [
        "display", "show", "render", "screen", "button", "view", "page"
    ],
}


def load_cru_units(path: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Load Layer 3 output.
    Supports the current contract key 'cru_units' and backward-compatible fallback 'crus'.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    crus = data.get("cru_units")
    if crus is None:
        crus = data.get("crus", [])

    if not isinstance(crus, list):
        raise ValueError("Input JSON must contain a list under 'cru_units' or 'crus'.")

    return crus, data.get("metadata", {})


def group_crus_by_requirement(crus: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for cru in crus:
        parent_id = cru.get("parent_requirement_id") or "UNKNOWN"
        grouped[parent_id].append(cru)
    return dict(grouped)


def stable_sort_crus(crus: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Keep ordering deterministic for reproducible chunks.
    """
    return sorted(
        crus,
        key=lambda c: (
            c.get("parent_requirement_id") or "",
            c.get("traceability", {}).get("section_path") or "",
            c.get("cru_id") or "",
        ),
    )


def slice_requirement_group(req_crus: list[dict[str, Any]], max_size: int) -> list[list[dict[str, Any]]]:
    """
    Split one requirement's CRUs into smaller LLM-friendly slices.
    Never mixes different parent requirements.
    """
    ordered = stable_sort_crus(req_crus)
    return [ordered[i:i + max_size] for i in range(0, len(ordered), max_size)]


def infer_chunk_type(crus: list[dict[str, Any]]) -> str:
    types = [c.get("type", "other") for c in crus]
    distinct = sorted(set(types))
    return distinct[0] if len(distinct) == 1 else "mixed"


def create_cru_payload(cru: dict[str, Any]) -> dict[str, Any]:
    """
    Minimal but sufficient payload for Layer 5.
    """
    return {
        "cru_id": cru.get("cru_id"),
        "actor": cru.get("actor"),
        "action": cru.get("action"),
        "constraint": cru.get("constraint"),
        "confidence": cru.get("confidence"),
        "title": cru.get("title"),
        "acceptance_criteria": cru.get("acceptance_criteria"),
    }


def generate_capability_tags(crus: list[dict[str, Any]]) -> list[str]:
    """
    Deterministic label assignment from action/title/type text.
    Returns top matching labels only.
    """
    texts: list[str] = []

    for cru in crus:
        action = (cru.get("action") or "").lower()
        title = (cru.get("title") or "").lower()
        cru_type = (cru.get("type") or "").lower()
        text = " ".join(x for x in [action, title, cru_type] if x)
        if text:
            texts.append(text)

    if not texts:
        return ["Generic"]

    scores: Counter[str] = Counter()

    for text in texts:
        for label, patterns in CAPABILITY_TAG_PATTERNS.items():
            if any(pattern in text for pattern in patterns):
                scores[label] += 1

    cru_types = {c.get("type") for c in crus}
    if "performance" in cru_types:
        scores["Performance"] += 2
    if "security" in cru_types:
        scores["Security"] += 2
    if "reliability" in cru_types:
        scores["Reliability"] += 2
    if "portability" in cru_types:
        scores["Portability"] += 2
    if "usability" in cru_types:
        scores["User Interface"] += 2

    if not scores:
        return ["Generic"]

    top_tags = [label for label, _ in scores.most_common(3)]
    return top_tags


def build_chunk_traceability(crus: list[dict[str, Any]]) -> dict[str, Any]:
    source_requirements = sorted({
        c.get("parent_requirement_id")
        for c in crus
        if c.get("parent_requirement_id")
    })

    sections = sorted({
        c.get("traceability", {}).get("section_path") or c.get("traceability", {}).get("section")
        for c in crus
        if c.get("traceability")
    })

    doc_ids = sorted({
        c.get("traceability", {}).get("doc_id")
        for c in crus
        if c.get("traceability", {}).get("doc_id")
    })

    source_locators = []
    seen = set()
    for c in crus:
        trace = c.get("traceability", {})
        locator = trace.get("source_locator")
        if locator:
            key = json.dumps(locator, sort_keys=True)
            if key not in seen:
                seen.add(key)
                source_locators.append(locator)

    return {
        "source_requirements": source_requirements,
        "sections": sections,
        "doc_ids": doc_ids,
        "source_locators": source_locators,
    }


def create_chunk_from_crus(
    crus: list[dict[str, Any]],
    chunk_id: str,
    application_domain: str,
) -> dict[str, Any]:
    chunk = {
        "chunk_id": chunk_id,
        "chunk_type": infer_chunk_type(crus),
        "application_domain": [application_domain],
        "capability_tags": generate_capability_tags(crus),
        "cru_ids": [c.get("cru_id") for c in crus],
        "crus": [create_cru_payload(c) for c in crus],
        "traceability": build_chunk_traceability(crus),
    }
    return chunk


def create_chunks(crus: list[dict[str, Any]], application_domain: str) -> list[dict[str, Any]]:
    """
    Chunk strategy:
    - group by parent_requirement_id
    - split only if a single requirement has too many CRUs
    """
    req_groups = group_crus_by_requirement(crus)

    chunks: list[dict[str, Any]] = []
    chunk_counter = 1

    for _, req_crus in sorted(req_groups.items(), key=lambda item: item[0]):
        subgroups = slice_requirement_group(req_crus, CONFIG["max_crus_per_chunk"])
        for subgroup in subgroups:
            chunks.append(
                create_chunk_from_crus(
                    crus=subgroup,
                    chunk_id=f"CHUNK_{chunk_counter:03d}",
                    application_domain=application_domain,
                )
            )
            chunk_counter += 1

    return chunks


def validate_chunks(
    chunks: list[dict[str, Any]],
    total_crus: int,
    application_domain: str,
) -> dict[str, Any]:
    all_cru_ids: list[str] = []
    chunk_sizes: list[int] = []

    for chunk in chunks:
        cru_ids = chunk.get("cru_ids", [])
        all_cru_ids.extend(cru_ids)
        chunk_sizes.append(len(cru_ids))

    unique_cru_ids = set(all_cru_ids)

    domain_consistency = all(
        chunk.get("application_domain", [None])[0] == application_domain
        for chunk in chunks
    )

    return {
        "total_chunks": len(chunks),
        "expected_crus": total_crus,
        "total_cru_ids_in_chunks": len(all_cru_ids),
        "unique_cru_ids_in_chunks": len(unique_cru_ids),
        "all_crus_present": len(unique_cru_ids) == total_crus,
        "duplicate_cru_ids_found": len(all_cru_ids) != len(unique_cru_ids),
        "max_chunk_size": max(chunk_sizes) if chunk_sizes else 0,
        "min_chunk_size": min(chunk_sizes) if chunk_sizes else 0,
        "avg_chunk_size": round(sum(chunk_sizes) / len(chunk_sizes), 2) if chunk_sizes else 0.0,
        "size_limit_violated": any(size > CONFIG["max_crus_per_chunk"] for size in chunk_sizes),
        "domain_consistency": domain_consistency,
        "application_domain": application_domain,
    }


def chunk_and_tag_crus(
    cru_json_path: str,
    output_path: str,
    application_domain: str = DEFAULT_APPLICATION_DOMAIN,
) -> dict[str, Any]:
    if application_domain not in APPLICATION_DOMAINS:
        raise ValueError(
            f"Invalid domain '{application_domain}'. Must be one of: {APPLICATION_DOMAINS}"
        )

    crus, input_metadata = load_cru_units(cru_json_path)
    chunks = create_chunks(crus, application_domain)
    validation = validate_chunks(chunks, len(crus), application_domain)

    capability_distribution: Counter[str] = Counter()
    chunk_type_distribution: Counter[str] = Counter()

    for chunk in chunks:
        chunk_type_distribution[chunk["chunk_type"]] += 1
        for tag in chunk.get("capability_tags", []):
            capability_distribution[tag] += 1

    output = {
        "metadata": {
            "stage": "Stage 4: Semantic Chunking & Capability Tagging",
            "version": "2.1",
            "input_total_crus": len(crus),
            "source_stage_metadata": input_metadata,
            "total_chunks": validation["total_chunks"],
            "avg_chunk_size": validation["avg_chunk_size"],
            "max_chunk_size": validation["max_chunk_size"],
            "min_chunk_size": validation["min_chunk_size"],
            "application_domain": application_domain,
            "chunk_type_distribution": dict(chunk_type_distribution),
            "capability_tag_distribution": dict(capability_distribution),
            "all_crus_present": validation["all_crus_present"],
            "duplicate_cru_ids_found": validation["duplicate_cru_ids_found"],
            "size_limit_violated": validation["size_limit_violated"],
            "domain_consistency": validation["domain_consistency"],
        },
        "chunks": chunks,
    }

    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path_obj, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    return output


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Layer 4: Semantic Chunking & Domain Tagging"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to Layer 3 cru_units.json",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output chunked_crus_with_domain.json",
    )
    parser.add_argument(
        "--domain",
        default=DEFAULT_APPLICATION_DOMAIN,
        help=f"Application domain. Default: {DEFAULT_APPLICATION_DOMAIN}",
    )
    args = parser.parse_args()

    result = chunk_and_tag_crus(
        cru_json_path=args.input,
        output_path=args.output,
        application_domain=args.domain,
    )

    print("=" * 70)
    print("STAGE 4 COMPLETE")
    print("=" * 70)
    print(f"Application Domain: {result['metadata']['application_domain']}")
    print(f"Total Chunks:       {result['metadata']['total_chunks']}")
    print(f"Avg Chunk Size:     {result['metadata']['avg_chunk_size']}")
    print(f"Max Chunk Size:     {result['metadata']['max_chunk_size']}")
    print(f"All CRUs Present:   {result['metadata']['all_crus_present']}")
    print(f"Duplicate CRUs:     {result['metadata']['duplicate_cru_ids_found']}")


if __name__ == "__main__":
    main()