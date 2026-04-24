import hashlib
import json
from datetime import datetime, timezone


# ── Module prefix map (Stage-2.2 Section 2.1, deterministic rule) ─────────────
_MODULE_PREFIX_MAP = {
    "FR":  "functional",
    "QR":  "quality",
    "NFR": "non-functional",
    "SR":  "security",
    "PR":  "performance",
    "UR":  "usability",
}


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require(mapping: dict, key: str, context: str):
    val = mapping.get(key)
    if val is None or val == "":
        raise ValueError(f"{context}: required field '{key}' is missing or empty")
    return val


def _derive_module(parent_requirement_id: str, context: str) -> str:
    """Stage-2.2 Section 2.1: derive module from parent_requirement_id prefix."""
    upper = parent_requirement_id.upper()
    for prefix, module in _MODULE_PREFIX_MAP.items():
        if upper.startswith(prefix):
            return module
    raise ValueError(
        f"{context}: cannot derive module — "
        f"unknown parent_requirement_id prefix in {parent_requirement_id!r}"
    )


# ── REQ nodes ─────────────────────────────────────────────────────────────────

def build_req_nodes(graph_store, cru_units_path: str):
    """Insert one REQ node per CRU entry from cru_units.json (Stage-2.2 §2.1)."""
    with open(cru_units_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    metadata = data.get("metadata") or {}
    # Stage-2.2 §2.1: version = metadata.finalization_version, uniform across build.
    version = metadata.get("finalization_version")
    if not version:
        raise ValueError(
            "build_req_nodes: metadata.finalization_version is missing or empty"
        )

    crus = data.get("crus")
    if crus is None:
        raise ValueError("build_req_nodes: top-level 'crus' array is missing")

    build_ts = datetime.now(timezone.utc)

    for entry in crus:
        # ── Precondition checks (Stage-2.2 §2.1 P1–P5) ──────────────────────
        cru_id = entry.get("cru_id")
        if not cru_id:
            raise ValueError(f"build_req_nodes: cru entry missing 'cru_id': {entry}")
        if not cru_id.startswith("CRU_"):
            raise ValueError(
                f"build_req_nodes: cru_id does not start with 'CRU_': {cru_id!r}"
            )

        ctx = f"build_req_nodes(cru_id={cru_id!r})"

        traceability = entry.get("traceability")
        if not traceability:
            raise ValueError(f"{ctx}: 'traceability' block is missing")

        # Stage-2.2 §2.1 P4, P5
        doc_id       = _require(traceability, "source_file", ctx)
        section_path = _require(traceability, "section",     ctx)

        # ── Field population (Stage-2.2 §2.1 deterministic rules) ────────────

        # text: '[actor] [action]' or '[actor] [action] [constraint]'
        # parent_requirement_id appended after separator when available.
        actor      = entry.get("actor")
        action     = entry.get("action")
        constraint = entry.get("constraint")
        if not actor and not action:
            raise ValueError(f"{ctx}: both 'actor' and 'action' are missing or empty")
        if not actor:
            raise ValueError(f"{ctx}: 'actor' is missing or empty")
        if not action:
            raise ValueError(f"{ctx}: 'action' is missing or empty")

        parent_requirement_id = _require(entry, "parent_requirement_id", ctx)

        if constraint:
            text = f"{actor} {action} {constraint}"
        else:
            text = f"{actor} {action}"

        # module: derived from parent_requirement_id prefix (Stage-2.2 §2.1)
        module = _derive_module(parent_requirement_id, ctx)

        # doc_type: deterministically 'SRS' from source_file extension (Stage-2.2 §2.1)
        doc_type = "SRS"

        # source_locator_json: {"section": ..., "page": ...} omit page if null
        page = traceability.get("page")
        locator: dict = {"section": section_path}
        if page is not None:
            locator["page"] = page
        source_locator_json = json.dumps(locator)

        # extra_json: provenance fields only (Stage-2.2 §2.1)
        extra = {
            "original_req_id":  parent_requirement_id,
            "cru_type":         entry.get("type"),
            "cru_confidence":   entry.get("confidence"),
            "derived_from_cra": entry.get("derived_from_cra"),
            "requirement_type": traceability.get("requirement_type"),
        }

        node = {
            "node_id":             cru_id,
            "node_type":           "REQ",
            "text":                text,
            "module":              module,
            "version":             version,
            "doc_id":              doc_id,
            "doc_type":            doc_type,
            "section_path":        section_path,
            "source_locator_json": source_locator_json,
            "extra_json":          json.dumps(extra),
            "created_at":          build_ts,
        }

        graph_store.insert_node(node)


# ── CHUNK nodes ───────────────────────────────────────────────────────────────

def build_chunk_nodes(graph_store, chunked_crus_path: str):
    """Insert one parent CHUNK per section and one child CHUNK per CRU."""
    with open(chunked_crus_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    metadata = data.get("metadata") or {}
    version = metadata.get("version")
    if not version:
        raise ValueError(
            "build_chunk_nodes: metadata.version is missing or empty"
        )

    chunks = data.get("chunks")
    if chunks is None:
        raise ValueError("build_chunk_nodes: top-level 'chunks' array is missing")

    build_ts = datetime.now(timezone.utc)
    written_parents: set = set()

    for chunk_entry in chunks:
        chunk_id = chunk_entry.get("chunk_id", "<unknown>")
        ctx = f"build_chunk_nodes(chunk_id={chunk_id!r})"

        crus = chunk_entry.get("crus")
        if not crus:
            raise ValueError(f"{ctx}: 'crus' array is missing or empty")

        traceability = chunk_entry.get("traceability") or {}
        sections = traceability.get("sections")
        if not sections:
            raise ValueError(f"{ctx}: traceability.sections[] is missing or empty")

        section_path = sections[0] if len(sections) == 1 else " | ".join(sections)

        source_requirements = traceability.get("source_requirements") or []
        if not source_requirements:
            raise ValueError(
                f"{ctx}: traceability.source_requirements[] is missing or empty"
            )

        first_req_id = source_requirements[0]
        module = _derive_module(first_req_id, ctx)

        first_cru_id = chunk_entry.get("cru_ids", [None])[0]
        if not first_cru_id:
            raise ValueError(f"{ctx}: cru_ids[] is missing or empty")

        req_node = graph_store.get_node(first_cru_id)
        if req_node is None:
            raise ValueError(
                f"{ctx}: precondition P4 violated — "
                f"REQ node {first_cru_id!r} does not exist in the Graph Store"
            )

        doc_id   = req_node["doc_id"]
        doc_type = req_node["doc_type"]

        source_locator_json = json.dumps({"section": section_path})

        # ── Parent CHUNK: one per unique (doc_id, section_path) ──────────────
        parent_key = (doc_id, section_path)
        if parent_key not in written_parents:
            parent_text = f"Section {section_path}"
            parent_extra = {
                "chunk_type":         "parent",
                "chunk_id_upstream":  chunk_id,
                "semantic_type":      chunk_entry.get("chunk_type"),
                "capability_tags":    chunk_entry.get("capability_tags", []),
                "application_domain": chunk_entry.get("application_domain", []),
                "source_cru_ids":     chunk_entry.get("cru_ids", []),
            }
            parent_hash    = _sha256(doc_id + section_path + parent_text)
            parent_node_id = f"P_{parent_hash}"

            graph_store.insert_node({
                "node_id":             parent_node_id,
                "node_type":           "CHUNK",
                "text":                parent_text,
                "module":              module,
                "version":             version,
                "doc_id":              doc_id,
                "doc_type":            doc_type,
                "section_path":        section_path,
                "source_locator_json": source_locator_json,
                "extra_json":          json.dumps(parent_extra),
                "created_at":          build_ts,
            })
            written_parents.add(parent_key)

        # ── Child CHUNKs: one per individual CRU ─────────────────────────────
        for cru in crus:
            cru_id     = cru.get("cru_id", "<unknown>")
            ctx_cru    = f"build_chunk_nodes(chunk_id={chunk_id!r}, cru_id={cru_id!r})"
            actor      = cru.get("actor")
            action     = cru.get("action")
            constraint = cru.get("constraint")

            if not actor:
                raise ValueError(f"{ctx_cru}: 'actor' is missing or empty")
            if not action:
                raise ValueError(f"{ctx_cru}: 'action' is missing or empty")

            if constraint:
                clause_text = f"{actor} {action} {constraint}"
            else:
                clause_text = f"{actor} {action}"

            normalized = clause_text.lower().strip()
            child_hash    = _sha256(doc_id + section_path + normalized)
            child_node_id = f"CH_{child_hash}"

            child_extra = {
                "chunk_type":         "child",
                "chunk_id_upstream":  chunk_id,
                "cru_id":             cru_id,
                "semantic_type":      chunk_entry.get("chunk_type"),
                "capability_tags":    chunk_entry.get("capability_tags", []),
                "application_domain": chunk_entry.get("application_domain", []),
                "source_cru_ids":     chunk_entry.get("cru_ids", []),
            }

            graph_store.insert_node({
                "node_id":             child_node_id,
                "node_type":           "CHUNK",
                "text":                clause_text,
                "module":              module,
                "version":             version,
                "doc_id":              doc_id,
                "doc_type":            doc_type,
                "section_path":        section_path,
                "source_locator_json": source_locator_json,
                "extra_json":          json.dumps(child_extra),
                "created_at":          build_ts,
            })