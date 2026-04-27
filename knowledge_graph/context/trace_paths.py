def build_trace_paths(anchor_ids: list[str], raw_paths: list[dict]) -> list[dict]:
    results = []
    for item in raw_paths:
        path = item.get("path", [])
        if not path:
            continue
        results.append({
            "why": item.get("why") or f"Evidence for anchor(s): {', '.join(anchor_ids)}",
            "path": path,
            "path_confidence": item.get("path_confidence", 1.0),
        })
    return results
