"""
Build Comparison Results

Reads retrieval output and generates LLM-based classifications
for all acceptance-CRU candidate pairs.
"""

import json
import time
from pathlib import Path
from typing import List, Dict, Any

from comparator import create_comparator


class ComparisonResultsBuilder:
    """Builds comparison results for all acceptance units."""
    
    def __init__(
        self,
        model_name: str = "qwen-14b-instruct",
        temperature: float = 0.0,
        batch_size: int = 5
    ):
        """
        Initialize builder.
        
        Args:
            model_name: LLM model name
            temperature: Generation temperature
            batch_size: Number of comparisons per batch
        """
        self.model_name = model_name
        self.temperature = temperature
        self.batch_size = batch_size
        
        # Create comparator
        self.comparator = create_comparator(
            model_name=model_name,
            temperature=temperature
        )
        
        # Statistics
        self.total_comparisons = 0
        self.total_inference_time = 0.0
        self.classification_counts = {
            "MATCH": 0,
            "PARTIAL_MATCH": 0,
            "MISSING_REQUIREMENT": 0,
            "CONFLICT": 0
        }
        self.total_confidence = 0.0
        
        # CRU lookup cache
        self.cru_lookup = {}
    
    def _load_cru_units(self, cru_units_file: str):
        """Load CRU units for text reconstruction."""
        with open(cru_units_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for cru in data["crus"]:
            cru_id = cru["cru_id"]
            
            # Reconstruct CRU text
            parts = []
            if cru.get("actor"):
                parts.append(cru["actor"])
            if cru.get("action"):
                parts.append(cru["action"])
            if cru.get("constraint"):
                parts.append(f"with constraint: {cru['constraint']}")
            if cru.get("outcome"):
                parts.append(f"resulting in: {cru['outcome']}")
            
            self.cru_lookup[cru_id] = {
                "text": " ".join(parts),
                "actor": cru.get("actor"),
                "action": cru.get("action"),
                "constraint": cru.get("constraint"),
                "type": cru["type"]
            }
    
    def _enrich_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich candidates with CRU text from lookup."""
        enriched = []
        for cand in candidates:
            cru_id = cand["cru_id"]
            cru_data = self.cru_lookup.get(cru_id, {})
            
            enriched_cand = {
                **cand,
                "cru_text": cru_data.get("text", ""),
                "actor": cru_data.get("actor"),
                "action": cru_data.get("action")
            }
            enriched.append(enriched_cand)
        
        return enriched
    
    def build_comparisons(
        self,
        retrieval_output_file: str,
        cru_units_file: str
    ) -> Dict[str, Any]:
        """
        Build comparison results for all retrieval outputs.
        
        Args:
            retrieval_output_file: Path to retrieval_output.json
            cru_units_file: Path to cru_units.json
            
        Returns:
            Comparison results dictionary
        """
        print(f"\n{'='*70}")
        print("BUILD COMPARISON RESULTS")
        print(f"{'='*70}\n")
        
        build_start = time.time()
        
        # Load CRU units
        print("Loading CRU units...")
        self._load_cru_units(cru_units_file)
        print(f"Loaded {len(self.cru_lookup)} CRU definitions\n")
        
        # Load retrieval results
        print("Loading retrieval results...")
        with open(retrieval_output_file, 'r', encoding='utf-8') as f:
            retrieval_data = json.load(f)
        
        retrieval_results = retrieval_data["retrieval_results"]
        print(f"Loaded {len(retrieval_results)} retrieval results\n")
        
        # Process comparisons
        print(f"{'='*70}")
        print("PROCESSING COMPARISONS")
        print(f"{'='*70}\n")
        
        comparisons = []
        batch_counter = 0
        
        for i, result in enumerate(retrieval_results, 1):
            acceptance_id = result["acceptance_id"]
            acceptance_text = result["text"]
            candidates = result["candidates"]
            
            # Enrich candidates with CRU text
            enriched_candidates = self._enrich_candidates(candidates)
            
            # Perform comparison
            inference_start = time.time()
            comparison_result = self.comparator.compare(
                acceptance_text=acceptance_text,
                candidates=enriched_candidates
            )
            inference_time = time.time() - inference_start
            
            # Update statistics
            self.total_comparisons += 1
            self.total_inference_time += inference_time
            self.classification_counts[comparison_result.classification] += 1
            self.total_confidence += comparison_result.confidence
            
            # Store comparison
            comparisons.append({
                "acceptance_id": acceptance_id,
                "level": result["level"],
                "step_type": result.get("step_type"),
                "classification": comparison_result.classification,
                "best_matching_cru": comparison_result.best_matching_cru,
                "confidence": comparison_result.confidence,
                "reasoning": comparison_result.reasoning
            })
            
            # Progress reporting
            batch_counter += 1
            if batch_counter >= self.batch_size:
                print(f"Processed {i}/{len(retrieval_results)} comparisons")
                batch_counter = 0
        
        # Final progress
        print(f"Processed {len(retrieval_results)}/{len(retrieval_results)} comparisons\n")
        
        build_duration = time.time() - build_start
        avg_inference_time = self.total_inference_time / self.total_comparisons if self.total_comparisons > 0 else 0.0
        avg_confidence = self.total_confidence / self.total_comparisons if self.total_comparisons > 0 else 0.0
        
        # Print summary
        print(f"{'='*70}")
        print("COMPARISON STATISTICS")
        print(f"{'='*70}")
        print(f"Total comparisons: {self.total_comparisons}")
        print(f"\nClassification breakdown:")
        for classification, count in sorted(self.classification_counts.items()):
            percentage = (count / self.total_comparisons * 100) if self.total_comparisons > 0 else 0.0
            print(f"  {classification}: {count} ({percentage:.1f}%)")
        print(f"\nAverage confidence: {avg_confidence:.4f}")
        print(f"Average inference time: {avg_inference_time:.4f}s")
        print(f"Total time: {build_duration:.2f}s")
        print(f"{'='*70}\n")
        
        # Build output
        output = {
            "metadata": {
                "total_comparisons": self.total_comparisons,
                "model_name": self.model_name,
                "temperature": self.temperature,
                "classification_counts": self.classification_counts,
                "avg_confidence": round(avg_confidence, 4),
                "avg_inference_time_seconds": round(avg_inference_time, 4),
                "total_duration_seconds": round(build_duration, 2)
            },
            "comparisons": comparisons
        }
        
        return output


def build_comparison_results(
    retrieval_output_file: str,
    cru_units_file: str,
    output_file: str = "comparison_results.json",
    model_name: str = "qwen-14b-instruct",
    temperature: float = 0.0,
    batch_size: int = 5
):
    """
    Build comparison results and save to JSON.
    
    Args:
        retrieval_output_file: Path to retrieval output
        cru_units_file: Path to CRU units
        output_file: Output JSON file path
        model_name: LLM model name
        temperature: Generation temperature
        batch_size: Comparisons per batch
    """
    builder = ComparisonResultsBuilder(
        model_name=model_name,
        temperature=temperature,
        batch_size=batch_size
    )
    
    output = builder.build_comparisons(
        retrieval_output_file=retrieval_output_file,
        cru_units_file=cru_units_file
    )
    
    # Save output
    print(f"Saving results to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Saved {len(output['comparisons'])} comparison results\n")
    
    return output


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python build_comparison_results.py <retrieval_output_file> <cru_units_file> [output_file]")
        sys.exit(1)
    
    retrieval_output_file = sys.argv[1]
    cru_units_file = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else "comparison_results.json"
    
    build_comparison_results(
        retrieval_output_file=retrieval_output_file,
        cru_units_file=cru_units_file,
        output_file=output_file
    )
