from __future__ import annotations

"""Pydantic schemas for Layer 2 requirement understanding outputs."""

from typing import List, Optional
from pydantic import BaseModel


class LabeledRequirement(BaseModel):
    req_id: str
    title: str
    actor: str
    actions: List[str]
    description: str
    constraints: Optional[str] = None
    dependencies: List[str]
    acceptance_criteria: Optional[str] = None
    outputs: Optional[str] = None
    planguage_table: Optional[List[dict]] = None


class GherkinRequirement(BaseModel):
    req_id: str
    title: str
    actor: str
    actions: List[str]
    background: Optional[str] = None
    scenarios: List[dict]
    dependencies: List[str]


class PLangRequirement(BaseModel):
    req_id: str
    title: str
    actor: str
    actions: List[str]
    description: str
    scale: Optional[str] = None
    meter: Optional[str] = None
    must: Optional[str] = None
    wish: Optional[str] = None
    plan: Optional[str] = None
    defined: Optional[str] = None
    dependencies: List[str]


class ExtractedRequirement(BaseModel):
    req_id: str
    section_path: str
    section_semantic_type: str
    input_format: str
    extraction_method: str
    title: str
    actor: str
    actions: List[str]
    description: Optional[str] = None
    constraints: Optional[str] = None
    dependencies: List[str]
    acceptance_criteria: Optional[str] = None
    outputs: Optional[str] = None
    planguage_table: Optional[List[dict]] = None
    scenarios: Optional[List[dict]] = None
    confidence: str
    source_reference: dict