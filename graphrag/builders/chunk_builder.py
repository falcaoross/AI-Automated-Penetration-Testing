import hashlib
import json
from datetime import datetime, timezone


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_chunk_nodes(graph_store, chunked_crus_path: str):
    with open(chunked_crus_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    metadata = data.get("metadata") or {}
    version = metadata.get("version") or "1.0"
    chunks = data.get("chunks")
    if chunks is None:
        raise ValueError("build_chunk_nodes: top-level 'chunks' array is missing")

    build_ts = datetime.now(timezone.utc)
    written_parents = set()

    for chunk_entry in chunks:
        chunk_id = chunk_entry.get("chunk_id", "")
        ctx = f"build_chunk_nodes(chunk_id={chunk_id!r})"

        cru_ids = chunk_entry.get("cru_ids", [])
        if not cru_ids:
            raise ValueError(f"{ctx}: cru_ids[] is missing or empty")

        first_cru_id = cru_ids[0]
        first_cru = graph_store.get_node(first_cru_id)
        if first_cru is None:
            raise ValueError(f"{ctx}: referenced CRU node {first_cru_id!r} does not exist in graph store")

        traceability = chunk_entry.get("traceability") or {}
        sections = traceability.get("sections") or []
        doc_ids = traceability.get("doc_ids") or []
        source_locators = traceability.get("source_locators") or []

        section_path = sections[0] if sections else first_cru.get("section_path", "UNKNOWN_SECTION")
        doc_id = doc_ids[0] if doc_ids else first_cru.get("doc_id", "UNKNOWN_DOC")
        doc_type = first_cru.get("doc_type", "SRS")
        module = first_cru.get("module", "unknown")

        locator = {"section": section_path}
        if source_locators:
            first_locator = source_locators[0] or {}
            if first_locator.get("page") is not None:
                locator["page"] = first_locator["page"]
            if first_locator.get("para") is not None:
                locator["para"] = first_locator["para"]

        source_locator_json = json.dumps(locator)

        parent_key = (doc_id, section_path)
        parent_node_id = f"P_{_sha256(doc_id + section_path)}"

        if parent_key not in written_parents:
            graph_store.insert_node({
                "node_id": parent_node_id,
                "node_type": "CHUNK",
                "title": f"Section {section_path}",
                "text": f"Section {section_path}",
                "module": module,
                "version": version,
                "doc_id": doc_id,
                "doc_type": doc_type,
                "section_path": section_path,
                "source_locator_json": source_locator_json,
                "extra_json": json.dumps({
                    "chunk_type": "parent",
                    "chunk_id_upstream": chunk_id,
                    "semantic_type": chunk_entry.get("chunk_type"),
                    "capability_tags": chunk_entry.get("capability_tags", []),
                    "application_domain": chunk_entry.get("application_domain", []),
                    "source_cru_ids": cru_ids,
                    "source_requirements": traceability.get("source_requirements", []),
                }),
                "created_at": build_ts,
            })
            written_parents.add(parent_key)

        for cru in chunk_entry.get("crus", []):
            cru_id = cru.get("cru_id", "")
            if not cru_id:
                raise ValueError(f"{ctx}: a cru entry is missing cru_id")

            actor = cru.get("actor")
            action = cru.get("action")
            constraint = cru.get("constraint")

            if not actor:
                raise ValueError(f"{ctx}: cru_id={cru_id!r} missing 'actor'")

            if action is None or str(action).strip() == "":
                if constraint is not None and str(constraint).strip() != "":
                    action = f"must satisfy constraint: {constraint}"
                else:
                    action = "unspecified behavior"

            clause_text = f"{actor} {action}"
            if constraint and str(constraint).strip() and str(constraint) not in clause_text:
                clause_text = f"{clause_text} {constraint}"

            child_node_id = f"CH_{_sha256(doc_id + section_path + cru_id + clause_text.lower().strip())}"

            graph_store.insert_node({
                "node_id": child_node_id,
                "node_type": "CHUNK",
                "title": cru.get("title"),
                "text": clause_text,
                "module": module,
                "version": version,
                "doc_id": doc_id,
                "doc_type": doc_type,
                "section_path": section_path,
                "source_locator_json": source_locator_json,
                "extra_json": json.dumps({
                    "chunk_type": "child",
                    "chunk_id_upstream": chunk_id,
                    "cru_id": cru_id,
                    "semantic_type": chunk_entry.get("chunk_type"),
                    "capability_tags": chunk_entry.get("capability_tags", []),
                    "application_domain": chunk_entry.get("application_domain", []),
                    "source_cru_ids": cru_ids,
                    "source_requirements": traceability.get("source_requirements", []),
                    "title": cru.get("title"),
                    "acceptance_criteria": cru.get("acceptance_criteria"),
                    "confidence": cru.get("confidence"),
                }),
                "created_at": build_ts,
            })