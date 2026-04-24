"""
Build Candidate Mappings

Reads canonical acceptance views and retrieves candidate CRUs for:
- Scenario-level text
- Step-level text (preconditions, actions, outcomes, postconditions)
"""

import json
import time
import gc
from pathlib import Path
from typing import List, Dict, Any

from acceptance_validation.module_3_embedding_indexing.embedding_model import load_embedding_model
from acceptance_validation.module_3_embedding_indexing.build_cru_index import build_cru_index
from acceptance_validation.module_4_retrieval.retriever import create_retriever


class CandidateMappingBuilder:
    """Builds candidate mappings for acceptance views."""
    
    def __init__(
        self,
        cru_units_file: str,
        model_name: str = "all-mpnet-base-v2",
        top_k: int = 5
    ):
        """
        Initialize builder.
        
        Args:
            cru_units_file: Path to cru_units.json
            model_name: Embedding model name
            top_k: Number of candidates to retrieve per query
        """
        self.model_name = model_name
        self.top_k = top_k
        
        print(f"\n{'='*70}")
        print("BUILD CANDIDATE MAPPINGS")
        print(f"{'='*70}\n")
        
        # Build CRU index
        print("Building CRU index...")
        cru_builder = build_cru_index(cru_units_file, model_name=model_name)
        
        # Create retriever
        print("\nInitializing retriever...")
        self.retriever = create_retriever(
            embedding_model=cru_builder.model,
            cru_index=cru_builder.index,
            cru_metadata=cru_builder.metadata_store
        )
        print("Retriever ready\n")
        
        # Statistics
        self.total_queries = 0
        self.total_similarity = 0.0
    
    def _prepare_scenario_text(self, view: dict) -> str:
        """Prepare scenario-level text for retrieval."""
        parts = []
        
        if view.get("user_story"):
            parts.append(view["user_story"])
        
        if view.get("scenario_title"):
            parts.append(f"Scenario: {view['scenario_title']}")
        
        for step in view.get("preconditions", []):
            parts.append(step)
        for step in view.get("actions", []):
            parts.append(step)
        for step in view.get("outcomes", []):
            parts.append(step)
        for step in view.get("postconditions", []):
            parts.append(step)
        
        return " ".join(parts)
    
    def _retrieve_for_text(self, text: str) -> List[Dict[str, Any]]:
        """Retrieve candidates for text."""
        candidates = self.retriever.retrieve_candidates(text, top_k=self.top_k)
        
        # Update statistics
        self.total_queries += 1
        if candidates:
            self.total_similarity += candidates[0].similarity
        
        # Convert to dict format
        return [
            {
                "cru_id": c.cru_id,
                "similarity": round(c.similarity, 4),
                "cru_type": c.cru_type,
                "parent_requirement": c.parent_requirement
            }
            for c in candidates
        ]
    
    def build_mappings(self, canonical_views_file: str) -> Dict[str, Any]:
        """
        Build candidate mappings for all acceptance views.
        
        Args:
            canonical_views_file: Path to canonical_acceptance_views.json
            
        Returns:
            Retrieval results dictionary
        """
        print(f"{'='*70}")
        print("RETRIEVING CANDIDATES")
        print(f"{'='*70}\n")
        
        build_start = time.time()
        
        # Load canonical views
        with open(canonical_views_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        views = data["canonical_views"]
        print(f"Loaded {len(views)} canonical views")
        print(f"Top-K: {self.top_k}\n")
        
        retrieval_results = []
        
        # Process each view
        for i, view in enumerate(views, 1):
            scenario_id = view["scenario_id"]
            
            if i % 5 == 0:
                print(f"Processing view {i}/{len(views)}: {scenario_id}")
            
            # 1. Scenario-level retrieval
            scenario_text = self._prepare_scenario_text(view)
            scenario_candidates = self._retrieve_for_text(scenario_text)
            
            retrieval_results.append({
                "acceptance_id": scenario_id,
                "level": "scenario",
                "text": scenario_text,
                "candidates": scenario_candidates
            })
            
            # 2. Step-level retrieval
            step_counter = 0
            
            for step in view.get("preconditions", []):
                step_counter += 1
                step_candidates = self._retrieve_for_text(step)
                retrieval_results.append({
                    "acceptance_id": f"{scenario_id}_step_{step_counter}",
                    "level": "step",
                    "step_type": "precondition",
                    "text": step,
                    "candidates": step_candidates
                })
            
            for step in view.get("actions", []):
                step_counter += 1
                step_candidates = self._retrieve_for_text(step)
                retrieval_results.append({
                    "acceptance_id": f"{scenario_id}_step_{step_counter}",
                    "level": "step",
                    "step_type": "action",
                    "text": step,
                    "candidates": step_candidates
                })
            
            for step in view.get("outcomes", []):
                step_counter += 1
                step_candidates = self._retrieve_for_text(step)
                retrieval_results.append({
                    "acceptance_id": f"{scenario_id}_step_{step_counter}",
                    "level": "step",
                    "step_type": "outcome",
                    "text": step,
                    "candidates": step_candidates
                })
            
            for step in view.get("postconditions", []):
                step_counter += 1
                step_candidates = self._retrieve_for_text(step)
                retrieval_results.append({
                    "acceptance_id": f"{scenario_id}_step_{step_counter}",
                    "level": "step",
                    "step_type": "postcondition",
                    "text": step,
                    "candidates": step_candidates
                })
        
        build_duration = time.time() - build_start
        
        # Garbage collection
        gc.collect()
        
        # Calculate statistics
        avg_similarity = self.total_similarity / self.total_queries if self.total_queries > 0 else 0.0
        
        # Print summary
        print(f"\n{'='*70}")
        print("RETRIEVAL STATISTICS")
        print(f"{'='*70}")
        print(f"Total queries: {self.total_queries}")
        print(f"Total retrieval operations: {self.total_queries}")
        print(f"Average top-1 similarity: {avg_similarity:.4f}")
        print(f"Total time: {build_duration:.2f}s")
        print(f"{'='*70}\n")
        
        # Build output
        output = {
            "metadata": {
                "total_queries": self.total_queries,
                "top_k": self.top_k,
                "model_name": self.model_name,
                "avg_top1_similarity": round(avg_similarity, 4),
                "build_duration_seconds": round(build_duration, 2)
            },
            "retrieval_results": retrieval_results
        }
        
        return output


def build_candidate_mappings(
    canonical_views_file: str,
    cru_units_file: str,
    output_file: str = "retrieval_output.json",
    model_name: str = "all-mpnet-base-v2",
    top_k: int = 5
):
    """
    Build candidate mappings and save to JSON.
    
    Args:
        canonical_views_file: Path to canonical acceptance views
        cru_units_file: Path to CRU units
        output_file: Output JSON file path
        model_name: Embedding model name
        top_k: Number of candidates per query
    """
    builder = CandidateMappingBuilder(
        cru_units_file=cru_units_file,
        model_name=model_name,
        top_k=top_k
    )
    
    output = builder.build_mappings(canonical_views_file)
    
    # Save output
    print(f"Saving results to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Saved {len(output['retrieval_results'])} retrieval results\n")
    
    return output


def test_retrieval(cru_units_file: str, model_name: str = "all-mpnet-base-v2"):
    """
    Test retrieval with sample queries.
    
    Args:
        cru_units_file: Path to CRU units
        model_name: Embedding model name
    """
    print(f"\n{'='*70}")
    print("TEST RETRIEVAL")
    print(f"{'='*70}\n")
    
    # Build CRU index
    print("Building CRU index...")
    cru_builder = build_cru_index(cru_units_file, model_name=model_name)
    
    # Create retriever
    retriever = create_retriever(
        embedding_model=cru_builder.model,
        cru_index=cru_builder.index,
        cru_metadata=cru_builder.metadata_store
    )
    
    # Test queries
    test_queries = [
        ("Scenario-level: User registration", 
         "As an end user, I want to register for the application using a valid email address and password"),
        ("Scenario-level: Admin manages tasks",
         "As an admin, I want full create, read, update, and delete access to my own tasks"),
        ("Step-level: System creates account",
         "Then the system creates a new user account"),
        ("Step-level: API returns forbidden",
         "Then the API returns HTTP 403 Forbidden")
    ]
    
    for query_type, query_text in test_queries:
        print(f"\n{query_type}")
        print(f"Query: {query_text[:80]}...")
        print(f"\nTop 3 candidates:")
        
        candidates = retriever.retrieve_candidates(query_text, top_k=3)
        
        for rank, candidate in enumerate(candidates, 1):
            print(f"  {rank}. {candidate.cru_id} (similarity: {candidate.similarity:.4f})")
            print(f"     Type: {candidate.cru_type}, Parent: {candidate.parent_requirement}")
        
        print()
    
    print(f"{'='*70}\n")


if __name__ == "__main__":
    import sys
    
    # Test mode
    if len(sys.argv) == 2 and sys.argv[1] == "test":
        test_retrieval("cru_units.json")
        sys.exit(0)
    
    # Full build mode
    if len(sys.argv) < 3:
        print("Usage: python build_candidate_mappings.py <canonical_views_file> <cru_units_file> [output_file]")
        print("       python build_candidate_mappings.py test")
        sys.exit(1)
    
    canonical_views_file = sys.argv[1]
    cru_units_file = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else "retrieval_output.json"
    
    build_candidate_mappings(
        canonical_views_file=canonical_views_file,
        cru_units_file=cru_units_file,
        output_file=output_file
    )
