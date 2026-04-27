"""
LLM Comparator Module

Classifies relationship between acceptance statements and candidate CRUs
using local LLM with deterministic generation.
"""

import json
import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ComparisonResult:
    """Structured comparison result."""
    classification: str  # MATCH, PARTIAL_MATCH, MISSING_REQUIREMENT, CONFLICT
    best_matching_cru: Optional[str]
    reasoning: str
    confidence: float


class LLMComparator:
    """LLM-based comparator for acceptance-CRU classification."""
    
    def __init__(self, model_name: str = "qwen-14b-instruct", temperature: float = 0.0):
        """
        Initialize comparator.
        
        Args:
            model_name: LLM model identifier
            temperature: Generation temperature (0.0 for deterministic)
        """
        self.model_name = model_name
        self.temperature = temperature
        self.top_p = 1.0
        
        # Valid classifications
        self.valid_classifications = {
            "MATCH",
            "PARTIAL_MATCH", 
            "MISSING_REQUIREMENT",
            "CONFLICT"
        }
    
    def _construct_prompt(
        self,
        acceptance_text: str,
        candidates: List[Dict[str, Any]]
    ) -> str:
        """
        Construct structured prompt for LLM.
        
        Args:
            acceptance_text: Acceptance statement
            candidates: List of candidate CRUs with metadata
            
        Returns:
            Formatted prompt string
        """
        # Format candidates
        candidate_lines = []
        for i, cand in enumerate(candidates, 1):
            cru_id = cand["cru_id"]
            cru_type = cand["cru_type"]
            parent = cand["parent_requirement"]
            
            # Get CRU text from cru_units (passed separately)
            cru_text = cand.get("cru_text", f"{cand.get('actor', 'System')} {cand.get('action', 'performs action')}")
            
            candidate_lines.append(f"{i}. ({cru_id}) {cru_text}")
        
        candidates_block = "\n".join(candidate_lines)
        
        prompt = f"""ACCEPTANCE STATEMENT:
"{acceptance_text}"

CANDIDATE REQUIREMENTS:
{candidates_block}

TASK:
Classify the relationship between the acceptance statement and the candidate requirements.

Return ONLY valid JSON with this schema:

{{
  "classification": "MATCH | PARTIAL_MATCH | MISSING_REQUIREMENT | CONFLICT",
  "best_matching_cru": "CRU_ID or null",
  "reasoning": "short explanation"
}}

CLASSIFICATION RULES:
- MATCH: Acceptance fully covered by at least one CRU
- PARTIAL_MATCH: Acceptance partially covered, CRU exists but incomplete
- MISSING_REQUIREMENT: No CRU sufficiently covers acceptance
- CONFLICT: CRU contradicts acceptance intent

No additional text. No markdown. No commentary."""

        return prompt
    
    def _call_llm(self, prompt: str) -> str:
        """
        Call local LLM with structured prompt.
        
        Args:
            prompt: Formatted prompt
            
        Returns:
            Raw LLM response
        """
        # Mock implementation - replace with actual LLM call
        # For production: use llama.cpp, vLLM, or ollama
        
        # Simulated deterministic response based on prompt analysis
        # This would be replaced with actual model inference
        
        # Simple heuristic for mock:
        if "register" in prompt.lower() and "establishes account" in prompt.lower():
            return json.dumps({
                "classification": "MATCH",
                "best_matching_cru": "CRU_FR1_01",
                "reasoning": "Acceptance describes user registration which directly maps to system establishing account functionality"
            })
        elif "login" in prompt.lower() and "validates" in prompt.lower():
            return json.dumps({
                "classification": "MATCH",
                "best_matching_cru": "CRU_FR2_01",
                "reasoning": "Acceptance describes authentication which maps to hash validation requirement"
            })
        elif "task" in prompt.lower() and "filter tasks" in prompt.lower():
            return json.dumps({
                "classification": "PARTIAL_MATCH",
                "best_matching_cru": "CRU_FR4_01",
                "reasoning": "Task management acceptance partially covered by task filtering capability"
            })
        elif "handle users" in prompt.lower():
            return json.dumps({
                "classification": "PARTIAL_MATCH",
                "best_matching_cru": "CRU_QR1_01",
                "reasoning": "Performance requirement exists but may not fully address functional acceptance needs"
            })
        else:
            return json.dumps({
                "classification": "MISSING_REQUIREMENT",
                "best_matching_cru": None,
                "reasoning": "No candidate requirement sufficiently covers this acceptance statement"
            })
    
    def _parse_llm_response(self, response: str) -> Dict[str, Any]:
        """
        Parse LLM JSON response with error handling.
        
        Args:
            response: Raw LLM output
            
        Returns:
            Parsed JSON dict
        """
        # Strip markdown code blocks if present
        response = response.strip()
        response = re.sub(r'^```json\s*', '', response)
        response = re.sub(r'^```\s*', '', response)
        response = re.sub(r'\s*```$', '', response)
        
        try:
            parsed = json.loads(response)
            
            # Validate schema
            if "classification" not in parsed:
                raise ValueError("Missing 'classification' field")
            if "best_matching_cru" not in parsed:
                raise ValueError("Missing 'best_matching_cru' field")
            if "reasoning" not in parsed:
                raise ValueError("Missing 'reasoning' field")
            
            # Validate classification value
            if parsed["classification"] not in self.valid_classifications:
                raise ValueError(f"Invalid classification: {parsed['classification']}")
            
            return parsed
            
        except json.JSONDecodeError as e:
            # Fallback response
            return {
                "classification": "MISSING_REQUIREMENT",
                "best_matching_cru": None,
                "reasoning": f"LLM response parsing failed: {str(e)}"
            }
    
    def _calculate_confidence(
        self,
        similarity_score: float,
        classification: str,
        reasoning: str
    ) -> float:
        """
        Calculate confidence score from similarity and LLM output.
        
        Args:
            similarity_score: Top candidate similarity
            classification: Classification result
            reasoning: LLM reasoning text
            
        Returns:
            Confidence score [0, 1]
        """
        # Base confidence from similarity
        sim_component = similarity_score * 0.7
        
        # LLM confidence heuristic
        llm_confidence = 0.5  # Default
        
        # Higher confidence for MATCH
        if classification == "MATCH":
            llm_confidence = 0.9
        elif classification == "PARTIAL_MATCH":
            llm_confidence = 0.7
        elif classification == "MISSING_REQUIREMENT":
            llm_confidence = 0.6
        elif classification == "CONFLICT":
            llm_confidence = 0.8
        
        # Boost confidence if reasoning is detailed
        if len(reasoning) > 50:
            llm_confidence = min(1.0, llm_confidence + 0.1)
        
        llm_component = llm_confidence * 0.3
        
        total_confidence = sim_component + llm_component
        
        return round(min(1.0, max(0.0, total_confidence)), 4)
    
    def compare(
        self,
        acceptance_text: str,
        candidates: List[Dict[str, Any]]
    ) -> ComparisonResult:
        """
        Compare acceptance statement against candidate CRUs.
        
        Args:
            acceptance_text: Acceptance statement text
            candidates: List of candidate CRUs with metadata and similarity scores
            
        Returns:
            ComparisonResult with classification
        """
        # Construct prompt
        prompt = self._construct_prompt(acceptance_text, candidates)
        
        # Call LLM
        response = self._call_llm(prompt)
        
        # Parse response
        parsed = self._parse_llm_response(response)
        
        # Get top similarity score
        top_similarity = candidates[0]["similarity"] if candidates else 0.0
        
        # Calculate confidence
        confidence = self._calculate_confidence(
            similarity_score=top_similarity,
            classification=parsed["classification"],
            reasoning=parsed["reasoning"]
        )
        
        # Build result
        return ComparisonResult(
            classification=parsed["classification"],
            best_matching_cru=parsed["best_matching_cru"],
            reasoning=parsed["reasoning"],
            confidence=confidence
        )


def create_comparator(
    model_name: str = "qwen-14b-instruct",
    temperature: float = 0.0
) -> LLMComparator:
    """
    Factory function to create LLM comparator.
    
    Args:
        model_name: LLM model identifier
        temperature: Generation temperature
        
    Returns:
        LLMComparator instance
    """
    return LLMComparator(model_name=model_name, temperature=temperature)
