CREATE TABLE IF NOT EXISTS nodes (
    node_id              TEXT        NOT NULL,
    node_type            TEXT        NOT NULL,
    text                 TEXT,
    module               TEXT        NOT NULL,
    version              TEXT        NOT NULL,
    doc_id               TEXT        NOT NULL,
    doc_type             TEXT        NOT NULL,
    section_path         TEXT        NOT NULL,
    source_locator_json  TEXT,
    extra_json           TEXT,
    created_at           TIMESTAMP   NOT NULL,
    PRIMARY KEY (node_id)
);

CREATE TABLE IF NOT EXISTS edges (
    src_id              TEXT        NOT NULL,
    src_type            TEXT        NOT NULL,
    rel_type            TEXT        NOT NULL,
    dst_id              TEXT        NOT NULL,
    dst_type            TEXT        NOT NULL,
    confidence          DOUBLE      NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    evidence_chunk_id   TEXT,
    extra_json          TEXT,
    created_at          TIMESTAMP   NOT NULL,
    PRIMARY KEY (src_id, rel_type, dst_id)
);