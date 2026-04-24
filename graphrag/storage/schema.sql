-- graphrag/storage/schema.sql
-- Idempotent – safe to re-run on every startup.
--
-- FIX vs old schema:
--   • module/version/doc_id/doc_type/section_path are nullable (nodes may be built
--     incrementally; NOT NULL was too strict and broke partial builds)
--   • node_embeddings: unified to ONE row per node_id (PRIMARY KEY node_id only);
--     old schema had PRIMARY KEY (node_id, embedding_model) which caused duplicate
--     issues when model changes; embedding_model stored as column instead
--   • doc_type column added to node_embeddings (was missing, needed by vector search)
--   • edges PRIMARY KEY on (src_id, rel_type, dst_id) – idempotent inserts work correctly

CREATE TABLE IF NOT EXISTS nodes (
    node_id             TEXT NOT NULL,
    node_type           TEXT NOT NULL,
    title               TEXT,
    text                TEXT,
    module              TEXT,
    version             TEXT,
    doc_id              TEXT,
    doc_type            TEXT,
    section_path        TEXT,
    source_locator_json TEXT,
    extra_json          TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (node_id)
);

CREATE TABLE IF NOT EXISTS edges (
    src_id              TEXT NOT NULL,
    src_type            TEXT NOT NULL,
    rel_type            TEXT NOT NULL,
    dst_id              TEXT NOT NULL,
    dst_type            TEXT NOT NULL,
    confidence          DOUBLE NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    evidence_chunk_id   TEXT,
    extra_json          TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (src_id, rel_type, dst_id)
);

CREATE TABLE IF NOT EXISTS node_embeddings (
    node_id             TEXT NOT NULL,
    node_type           TEXT NOT NULL,
    module              TEXT,
    version             TEXT,
    doc_type            TEXT,
    section_path        TEXT,
    embedding_model     TEXT NOT NULL,
    embedding_bytes     BLOB,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (node_id)
);

CREATE INDEX IF NOT EXISTS idx_nodes_type   ON nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_nodes_modver ON nodes(module, version);
CREATE INDEX IF NOT EXISTS idx_nodes_doc    ON nodes(doc_id, doc_type);
CREATE INDEX IF NOT EXISTS idx_edges_src    ON edges(src_id, rel_type);
CREATE INDEX IF NOT EXISTS idx_edges_dst    ON edges(dst_id, rel_type);
CREATE INDEX IF NOT EXISTS idx_emb_modver   ON node_embeddings(module, version);
