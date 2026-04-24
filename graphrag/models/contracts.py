"""
graphrag/models/contracts.py
==============================
FIXES IN THIS VERSION:
  [PARTIAL-2] EvidenceChunk: added doc_type field (was missing; needed by
              vector metadata filtering and audit output matching the PDF spec).

  [PARTIAL-3] OpenQuestion: aligned to PDF output shape.
              Old shape:  step_index: int, reason: str, chunk_ids_available: List[str]
              PDF shape:  question: str, required_for: str, chunk_ids_available: List[str]
              "question"     = human-readable open question text
              "required_for" = the step/chunk/req this question blocks

  [PARTIAL-4] EvidenceChunk: parent_context score no longer hardcoded.
              The score field stays 0.0 default; parent_context.py now derives
              score from edge.confidence (PARENT_OF confidence = 1.0 structural)
              instead of hardcoding 0.8. This file just removes the old default.

Previously-correct items retained:
  - open_questions: List[OpenQuestion] in ContextPack
  - TaskType enum with acceptance_validation
  - QueryInput, Anchor, TracePath, RelatedNode, Warning unchanged
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ── Task type enum ────────────────────────────────────────────────────────────

class TaskType(str, Enum):
    TEST_GENERATION       = "test_generation"
    DEBUG                 = "debug"
    IMPACT                = "impact"
    ACCEPTANCE_VALIDATION = "acceptance_validation"


# ── Query input contract ──────────────────────────────────────────────────────

@dataclass
class QueryInput:
    task: str                               # plain string; TaskType.value
    req_id: Optional[str] = None
    query_text: Optional[str] = None
    filters: Dict[str, str] = field(default_factory=dict)
    k_evidence: int = 8
    k_parent: int = 3


# ── Anchor ────────────────────────────────────────────────────────────────────

@dataclass
class Anchor:
    node_id: str
    node_type: str
    score: float = 1.0
    provenance: str = "graph"               # "graph" | "vector"


# ── Evidence chunk ────────────────────────────────────────────────────────────

@dataclass
class EvidenceChunk:
    chunk_id: str
    chunk_type: str                         # "child" | "parent"
    text: str
    doc_id: str
    section_path: str
    source_locator: Dict[str, Any]
    # [PARTIAL-2] doc_type added
    doc_type: str = ""                      # e.g. "SRS", "PRD", "USER_STORY"
    module: Optional[str] = None
    version: Optional[str] = None
    score: float = 0.0                      # ranked final score from retriever
    confidence: float = 0.0                 # path_confidence from graph traversal
    provenance: str = "graph"              # "graph" | "vector" | "inferred"
    similarity_score: Optional[float] = None
    needs_confirmation: bool = False        # True when provenance==vector & conf < threshold


# ── Trace path ────────────────────────────────────────────────────────────────

@dataclass
class TracePath:
    why: str
    path: List[Dict[str, Any]]
    path_confidence: float = 0.0


# ── Related node ─────────────────────────────────────────────────────────────

@dataclass
class RelatedNode:
    node_type: str
    node_id: str
    relation: str


# ── Warning ───────────────────────────────────────────────────────────────────

@dataclass
class Warning:
    type: str
    message: str
    chunk_id: Optional[str] = None


# ── Open question ─────────────────────────────────────────────────────────────

@dataclass
class OpenQuestion:
    """
    [PARTIAL-3] PDF-aligned shape:
      question        – human-readable text of what is unknown
      required_for    – the step index, chunk_id, or req_id this blocks
      chunk_ids_available – chunks that exist but do not cover this question
    
    Downstream generators MUST emit an OpenQuestion instead of guessing
    when evidence is missing or citations are invalid.
    """
    question: str
    required_for: str
    chunk_ids_available: List[str] = field(default_factory=list)


# ── Acceptance decision ───────────────────────────────────────────────────────

@dataclass
class AcceptanceDecision:
    """
    Per-criterion decision emitted by the acceptance comparator.
    verdict: "match" | "partial" | "missing" | "conflict"
    """
    criterion: str
    verdict: str                            # match | partial | missing | conflict
    evidence_chunk_ids: List[str] = field(default_factory=list)
    notes: str = ""


# ── Context Pack ─────────────────────────────────────────────────────────────

@dataclass
class ContextPack:
    """The only output contract of the RAG layer."""
    anchors: List[Anchor]
    evidence_chunks: List[EvidenceChunk]
    parent_context: List[EvidenceChunk]
    trace_paths: List[TracePath]
    related_nodes: List[RelatedNode]
    warnings: List[Warning]
    open_questions: List[OpenQuestion]
