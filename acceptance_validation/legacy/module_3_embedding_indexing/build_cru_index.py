"""
Build CRU Index

Reads cru_units.json and builds FAISS index for CRU embeddings.
"""

import json
import numpy as np
import faiss
import time
from pathlib import Path
from typing import List

# Use package-qualified imports so this module works both when run as a package
# and when imported from sibling modules/scripts that adjust sys.path.
from acceptance_validation.module_3_embedding_indexing.embedding_model import load_embedding_model
from acceptance_validation.module_3_embedding_indexing.index_metadata import MetadataStore, CRUMetadata


class CRUIndexBuilder:
    """Builds FAISS index for CRU units."""
    
    def __init__(self, model_name: str = "all-mpnet-base-v2", batch_size: int = 32):
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
        return f"CRU_EMB_{self.embedding_counter:05d}"
    
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
    
    def _reconstruct_cru_text(self, cru: dict) -> str:
        """
        Reconstruct CRU text from components.
        
        Format: "Actor action [constraint] [outcome]"
        """
        parts = []
        
        if cru.get("actor"):
            parts.append(cru["actor"])
        
        if cru.get("action"):
            parts.append(cru["action"])
        
        if cru.get("constraint"):
            parts.append(f"with constraint: {cru['constraint']}")
        
        if cru.get("outcome"):
            parts.append(f"resulting in: {cru['outcome']}")
        
        return " ".join(parts)
    
    def build_index(self, cru_units_file: str):
        """
        Build FAISS index from CRU units.
        
        Args:
            cru_units_file: Path to cru_units.json
        """
        print(f"\n{'='*70}")
        print("BUILD CRU INDEX")
        print(f"{'='*70}\n")
        
        build_start = time.time()
        
        # Load CRU units
        with open(cru_units_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        crus = data["crus"]
        print(f"Loaded {len(crus)} CRU units")
        
        # Prepare texts and metadata
        texts = []
        cru_info = []
        
        for cru in crus:
            text = self._reconstruct_cru_text(cru)
            texts.append(text)
            
            cru_info.append({
                "cru_id": cru["cru_id"],
                "parent_requirement_id": cru["parent_requirement_id"],
                "type": cru["type"],
                "text": text
            })
        
        print(f"Total embeddings to generate: {len(texts)}")
        
        # Type distribution
        type_counts = {}
        for info in cru_info:
            t = info["type"]
            type_counts[t] = type_counts.get(t, 0) + 1
        
        print(f"\nCRU type distribution:")
        for cru_type, count in sorted(type_counts.items()):
            print(f"  {cru_type}: {count}")
        
        # Generate embeddings
        print(f"\nGenerating embeddings with {self.model_name}...")
        embed_start = time.time()
        
        embeddings = self.model.encode_batch(texts, show_progress=True)
        
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
        
        # Store metadata
        print("\nStoring metadata...")
        for i, info in enumerate(cru_info):
            embedding_id = self._generate_embedding_id()
            
            metadata = CRUMetadata(
                embedding_id=embedding_id,
                cru_id=info["cru_id"],
                cru_type=info["type"],
                parent_requirement=info["parent_requirement_id"],
                text=info["text"]
            )
            
            self.metadata_store.add_cru(metadata)
        
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
            
            embedding_id = f"CRU_EMB_{idx + 1:05d}"
            metadata = self.metadata_store.get(embedding_id)
            
            if metadata:
                print(f"{rank}. Similarity: {similarity:.4f}")
                print(f"   CRU ID: {metadata.cru_id}")
                print(f"   Parent: {metadata.parent_requirement}")
                print(f"   Type: {metadata.cru_type}")
                print(f"   Text: {metadata.text}")
                print()
        
        print(f"{'='*70}\n")


def build_cru_index(
    cru_units_file: str,
    model_name: str = "all-mpnet-base-v2",
    batch_size: int = 32
) -> CRUIndexBuilder:
    """
    Build CRU index from CRU units.
    
    Args:
        cru_units_file: Path to CRU units JSON
        model_name: Embedding model name
        batch_size: Batch size for encoding
        
    Returns:
        CRUIndexBuilder with built index
    """
    builder = CRUIndexBuilder(model_name=model_name, batch_size=batch_size)
    builder.build_index(cru_units_file)
    return builder


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python build_cru_index.py <cru_units_file>")
        sys.exit(1)
    
    cru_units_file = sys.argv[1]
    
    # Build index
    builder = build_cru_index(cru_units_file)
    
    # Test queries
    print("\nRunning test queries...\n")
    
    builder.test_query("System handles concurrent users with performance constraints", top_k=3)
    builder.test_query("User authentication and account management", top_k=3)
    builder.test_query("Task filtering functionality", top_k=3)
