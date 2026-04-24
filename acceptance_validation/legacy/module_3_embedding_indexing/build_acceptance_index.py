"""
Build Acceptance Index

Reads canonical_acceptance_views.json and builds FAISS index with:
- Scenario-level embeddings
- Step-level embeddings (preconditions, actions, outcomes, postconditions)
"""

import json
import numpy as np
import faiss
import time
import gc
from pathlib import Path
from typing import List, Tuple

# Use package-qualified imports for reliable resolution when modules are executed directly
from acceptance_validation.module_3_embedding_indexing.embedding_model import load_embedding_model
from acceptance_validation.module_3_embedding_indexing.index_metadata import MetadataStore, AcceptanceMetadata

# Force single-threaded FAISS
faiss.omp_set_num_threads(1)


class AcceptanceIndexBuilder:
    """Builds FAISS index for acceptance criteria."""
    
    def __init__(self, model_name: str = "all-mpnet-base-v2", batch_size: int = 16):
        """
        Initialize builder.
        
        Args:
            model_name: Embedding model name
            batch_size: Batch size for encoding
        """
        self.model_name = model_name
        self.batch_size = batch_size
        
        # Load embedding model
        self.model = load_embedding_model(model_name=model_name, batch_size=batch_size)
        self.embedding_dim = self.model.embedding_dim
        
        # Initialize metadata store
        self.metadata_store = MetadataStore(
            index_version="v1",
            model_name=model_name,
            embedding_dim=self.embedding_dim
        )
        
        # Will be created during build
        self.index = None
        self.embedding_counter = 0
    
    def _generate_embedding_id(self) -> str:
        """Generate sequential embedding ID."""
        self.embedding_counter += 1
        return f"ACC_EMB_{self.embedding_counter:05d}"
    
    def _create_faiss_index(self) -> faiss.IndexHNSWFlat:
        """
        Create FAISS HNSW index.
        
        Returns:
            FAISS index configured with HNSW parameters
        """
        # Create HNSW index
        index = faiss.IndexHNSWFlat(self.embedding_dim, 16, faiss.METRIC_INNER_PRODUCT)  # M = 16
        
        # Set HNSW parameters
        index.hnsw.efConstruction = 200
        index.hnsw.efSearch = 64
        
        return index
    
    def _prepare_scenario_text(self, view: dict) -> str:
        """
        Prepare scenario-level text for embedding.
        
        Combines: user_story + scenario_title + all steps
        """
        parts = []
        
        if view.get("user_story"):
            parts.append(view["user_story"])
        
        if view.get("scenario_title"):
            parts.append(f"Scenario: {view['scenario_title']}")
        
        # Add all steps
        for step in view.get("preconditions", []):
            parts.append(step)
        for step in view.get("actions", []):
            parts.append(step)
        for step in view.get("outcomes", []):
            parts.append(step)
        for step in view.get("postconditions", []):
            parts.append(step)
        
        return " ".join(parts)
    
    def _extract_embeddings_from_view(self, view: dict) -> List[Tuple[str, str, str, str]]:
        """
        Extract texts to embed from a canonical view.
        
        Returns:
            List of (text, level, step_type, scenario_id) tuples
        """
        extractions = []
        scenario_id = view["scenario_id"]
        
        # 1. Scenario-level embedding
        scenario_text = self._prepare_scenario_text(view)
        extractions.append((scenario_text, "scenario", None, scenario_id))
        
        # 2. Step-level embeddings
        for step in view.get("preconditions", []):
            extractions.append((step, "step", "precondition", scenario_id))
        
        for step in view.get("actions", []):
            extractions.append((step, "step", "action", scenario_id))
        
        for step in view.get("outcomes", []):
            extractions.append((step, "step", "outcome", scenario_id))
        
        for step in view.get("postconditions", []):
            extractions.append((step, "step", "postcondition", scenario_id))
        
        return extractions
    
    def build_index(self, canonical_views_file: str):
        """
        Build FAISS index from canonical acceptance views.
        
        Args:
            canonical_views_file: Path to canonical_acceptance_views.json
        """
        print(f"\n{'='*70}")
        print("BUILD ACCEPTANCE INDEX")
        print(f"{'='*70}\n")
        
        build_start = time.time()
        
        # Load canonical views
        with open(canonical_views_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        views = data["canonical_views"]
        print(f"Loaded {len(views)} canonical views")
        
        # Extract all texts to embed
        all_extractions = []
        view_mapping = []  # Maps extraction index to view
        
        for view in views:
            extractions = self._extract_embeddings_from_view(view)
            
            for text, level, step_type, scenario_id in extractions:
                all_extractions.append(text)
                view_mapping.append({
                    "scenario_id": scenario_id,
                    "level": level,
                    "step_type": step_type,
                    "source_units": view["traceability"]["source_units"]
                })
        
        print(f"Total embeddings to generate: {len(all_extractions)}")
        print(f"  Scenario-level: {sum(1 for v in view_mapping if v['level'] == 'scenario')}")
        print(f"  Step-level: {sum(1 for v in view_mapping if v['level'] == 'step')}")
        
        # Generate embeddings
        print(f"\nGenerating embeddings with {self.model_name}...")
        embed_start = time.time()
        
        embeddings = self.model.encode_batch(all_extractions, show_progress=True)
        
        embed_duration = time.time() - embed_start
        print(f"Embedding completed in {embed_duration:.2f}s")
        
        # Create FAISS index
        print("\nBuilding FAISS index...")
        index_start = time.time()
        
        self.index = self._create_faiss_index()
        
        # Add embeddings to index
        self.index.add(embeddings)
        
        index_duration = time.time() - index_start
        print(f"Index built in {index_duration:.2f}s")
        
        # Explicit garbage collection after index build
        gc.collect()
        
        # Store metadata
        print("\nStoring metadata...")
        for i, (text, mapping) in enumerate(zip(all_extractions, view_mapping)):
            embedding_id = self._generate_embedding_id()
            
            metadata = AcceptanceMetadata(
                embedding_id=embedding_id,
                scenario_id=mapping["scenario_id"],
                level=mapping["level"],
                step_type=mapping["step_type"],
                source_units=mapping["source_units"],
                text=text
            )
            
            self.metadata_store.add_acceptance(metadata)
        
        build_duration = time.time() - build_start
        
        # Print stats
        print(f"\n{'='*70}")
        print("INDEX STATISTICS")
        print(f"{'='*70}")
        print(f"Total vectors: {self.index.ntotal}")
        print(f"Embedding dimension: {self.embedding_dim}")
        print(f"Device used: {self.model.device}")
        print(f"\nTiming:")
        print(f"  Embedding: {embed_duration:.2f}s")
        print(f"  Index build: {index_duration:.2f}s")
        print(f"  Total: {build_duration:.2f}s")
        
        stats = self.metadata_store.stats()
        print(f"\nMetadata:")
        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    print(f"    {k}: {v}")
            else:
                print(f"  {key}: {value}")
        
        print(f"\n{'='*70}\n")
    
    def test_query(self, query_text: str, top_k: int = 5):
        """
        Test query against index.
        
        Args:
            query_text: Query text
            top_k: Number of results to return
        """
        if self.index is None:
            print("Error: Index not built yet")
            return
        
        print(f"\n{'='*70}")
        print(f"TEST QUERY: {query_text}")
        print(f"{'='*70}\n")
        
        # Embed query
        query_embedding = self.model.encode(query_text, normalize=True)
        query_embedding = query_embedding.reshape(1, -1)
        
        # Search
        distances, indices = self.index.search(query_embedding, top_k)
        
        # Get metadata
        print(f"Top {top_k} results:\n")
        for rank, (idx, distance) in enumerate(zip(indices[0], distances[0]), 1):
            # FAISS returns L2 distance for HNSW, convert to similarity
            # For normalized vectors: similarity = 1 - (distance^2 / 2)
            similarity = 1 - (distance ** 2 / 2)
            
            embedding_id = f"ACC_EMB_{idx + 1:05d}"
            metadata = self.metadata_store.get(embedding_id)
            
            if metadata:
                print(f"{rank}. Similarity: {similarity:.4f}")
                print(f"   Scenario: {metadata.scenario_id}")
                print(f"   Level: {metadata.level}")
                if metadata.step_type:
                    print(f"   Step type: {metadata.step_type}")
                print(f"   Text: {metadata.text[:100]}...")
                print()
        
        print(f"{'='*70}\n")


def build_acceptance_index(
    canonical_views_file: str,
    model_name: str = "all-mpnet-base-v2",
    batch_size: int = 32
) -> AcceptanceIndexBuilder:
    """
    Build acceptance index from canonical views.
    
    Args:
        canonical_views_file: Path to canonical views JSON
        model_name: Embedding model name
        batch_size: Batch size for encoding
        
    Returns:
        AcceptanceIndexBuilder with built index
    """
    builder = AcceptanceIndexBuilder(model_name=model_name, batch_size=batch_size)
    builder.build_index(canonical_views_file)
    return builder


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python build_acceptance_index.py <canonical_views_file>")
        sys.exit(1)
    
    canonical_views_file = sys.argv[1]
    
    # Build index
    builder = build_acceptance_index(canonical_views_file)
    
    # Test queries
    print("\nRunning test queries...\n")
    
    builder.test_query("User wants to register for the application", top_k=3)
    builder.test_query("System validates user credentials", top_k=3)
    builder.test_query("Admin can view all tasks", top_k=3)