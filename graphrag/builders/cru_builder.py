import json
from datetime import datetime, timezone


def build_cru_nodes(graph_store, cru_units_path: str):
    with open(cru_units_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    metadata = data.get("metadata") or {}
    version = metadata.get("finalization_version") or metadata.get("version") or "1.0"
    crus = data.get("cru_units") or data.get("crus")
    if crus is None:
        raise ValueError("build_cru_nodes: top-level 'cru_units' or 'crus' array is missing")

    build_ts = datetime.now(timezone.utc)

    for entry in crus:
        cru_id = entry.get("cru_id")
        if not cru_id:
            raise ValueError("build_cru_nodes: cru entry missing 'cru_id'")

        parent_requirement_id = entry.get("parent_requirement_id") or entry.get("parentrequirementid")
        actor = entry.get("actor")
        action = entry.get("action")
        constraint = entry.get("constraint")
        description = entry.get("description")
        title = entry.get("title")
        confidence = entry.get("confidence")
        acceptance_criteria = entry.get("acceptance_criteria") or entry.get("acceptancecriteria")

        if not actor:
            raise ValueError(f"build_cru_nodes(cru_id={cru_id!r}): required field 'actor' is missing or empty")
        if not parent_requirement_id:
            raise ValueError(f"build_cru_nodes(cru_id={cru_id!r}): required field 'parent_requirement_id' is missing or empty")

        if action is None or str(action).strip() == "":
            if constraint is not None and str(constraint).strip() != "":
                action = f"must satisfy constraint: {constraint}"
            elif description is not None and str(description).strip() != "":
                action = description
            else:
                action = "unspecified behavior"

        text = f"{actor} {action}"
        if constraint and str(constraint).strip() and str(constraint) not in text:
            text = f"{text} {constraint}"

        traceability = entry.get("traceability") or {}
        source_locator = traceability.get("source_locator") or {}
        section_path = traceability.get("section_path") or traceability.get("section") or "UNKNOWN_SECTION"
        doc_id = traceability.get("doc_id") or metadata.get("doc_id") or "UNKNOWN_DOC"
        doc_type = traceability.get("doc_type") or "SRS"
        module = traceability.get("module") or "unknown"

        locator = {"section": section_path}
        if source_locator.get("page") is not None:
            locator["page"] = source_locator["page"]
        if source_locator.get("para") is not None:
            locator["para"] = source_locator["para"]

        extra = {
            "original_req_id": parent_requirement_id,
            "cru_type": entry.get("type"),
            "cru_confidence": confidence,
            "title": title,
            "description": description,
            "acceptance_criteria": acceptance_criteria,
            "outputs": entry.get("outputs"),
            "dependencies": entry.get("dependencies", []),
            "scenarios": entry.get("scenarios"),
            "constraint": constraint,
            "extraction_method": entry.get("extraction_method"),
            "input_format": entry.get("input_format"),
            "invalid": entry.get("invalid", False),
        }

        graph_store.insert_node({
            "node_id": cru_id,
            "node_type": "CRU",
            "title": title,
            "text": text,
            "module": module,
            "version": version,
            "doc_id": doc_id,
            "doc_type": doc_type,
            "section_path": section_path,
            "source_locator_json": json.dumps(locator),
            "extra_json": json.dumps(extra),
            "created_at": build_ts,
        })