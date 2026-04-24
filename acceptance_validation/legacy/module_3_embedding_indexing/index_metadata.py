"""
Index Metadata Module

In-memory storage for embedding metadata with reverse lookup support.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict


@dataclass
class AcceptanceMetadata:
    """Metadata for acceptance embeddings."""
    embedding_id: str
    scenario_id: str
    level: str  # "scenario" or "step"
    step_type: Optional[str]  # "precondition", "action", "outcome", "postcondition", or None
    source_units: List[str]
    text: str  # Original text for reference


@dataclass
class CRUMetadata:
    """Metadata for CRU embeddings."""
    embedding_id: str
    cru_id: str
    cru_type: str
    parent_requirement: str
    text: str  # Reconstructed CRU text


class MetadataStore:
    """In-memory metadata storage with efficient lookups."""
    
    def __init__(self, index_version: str = "v1", model_name: str = None, embedding_dim: int = None):
        """
        Initialize metadata store.
        
        Args:
            index_version: Version identifier
            model_name: Embedding model used
            embedding_dim: Embedding dimension
        """
        self.index_version = index_version
        self.model_name = model_name
        self.embedding_dim = embedding_dim
        
        # Primary storage: id -> metadata
        self._metadata: Dict[str, Any] = {}
        
        # Reverse lookups
        self._by_scenario: Dict[str, List[str]] = {}  # scenario_id -> [embedding_ids]
        self._by_cru_id: Dict[str, str] = {}  # cru_id -> embedding_id
        self._by_parent_req: Dict[str, List[str]] = {}  # parent_requirement -> [embedding_ids]
    
    def add_acceptance(self, metadata: AcceptanceMetadata):
        """Add acceptance metadata."""
        self._metadata[metadata.embedding_id] = metadata
        
        # Update reverse lookup
        if metadata.scenario_id not in self._by_scenario:
            self._by_scenario[metadata.scenario_id] = []
        self._by_scenario[metadata.scenario_id].append(metadata.embedding_id)
    
    def add_cru(self, metadata: CRUMetadata):
        """Add CRU metadata."""
        self._metadata[metadata.embedding_id] = metadata
        
        # Update reverse lookups
        self._by_cru_id[metadata.cru_id] = metadata.embedding_id
        
        if metadata.parent_requirement not in self._by_parent_req:
            self._by_parent_req[metadata.parent_requirement] = []
        self._by_parent_req[metadata.parent_requirement].append(metadata.embedding_id)
    
    def get(self, embedding_id: str) -> Optional[Any]:
        """Get metadata by embedding ID."""
        return self._metadata.get(embedding_id)
    
    def get_by_scenario(self, scenario_id: str) -> List[Any]:
        """Get all embeddings for a scenario."""
        embedding_ids = self._by_scenario.get(scenario_id, [])
        return [self._metadata[eid] for eid in embedding_ids]
    
    def get_by_cru_id(self, cru_id: str) -> Optional[Any]:
        """Get metadata by CRU ID."""
        embedding_id = self._by_cru_id.get(cru_id)
        return self._metadata.get(embedding_id) if embedding_id else None
    
    def get_by_parent_requirement(self, parent_req: str) -> List[Any]:
        """Get all CRU embeddings for a parent requirement."""
        embedding_ids = self._by_parent_req.get(parent_req, [])
        return [self._metadata[eid] for eid in embedding_ids]
    
    def count(self) -> int:
        """Total number of embeddings."""
        return len(self._metadata)
    
    def count_by_level(self, level: str) -> int:
        """Count acceptance embeddings by level."""
        return sum(1 for m in self._metadata.values() 
                  if isinstance(m, AcceptanceMetadata) and m.level == level)
    
    def count_by_step_type(self, step_type: str) -> int:
        """Count acceptance embeddings by step type."""
        return sum(1 for m in self._metadata.values()
                  if isinstance(m, AcceptanceMetadata) and m.step_type == step_type)
    
    def stats(self) -> Dict[str, Any]:
        """Get metadata statistics."""
        acceptance_count = sum(1 for m in self._metadata.values() if isinstance(m, AcceptanceMetadata))
        cru_count = sum(1 for m in self._metadata.values() if isinstance(m, CRUMetadata))
        
        stats = {
            "total_embeddings": len(self._metadata),
            "acceptance_embeddings": acceptance_count,
            "cru_embeddings": cru_count,
            "index_version": self.index_version,
            "model_name": self.model_name,
            "embedding_dim": self.embedding_dim
        }
        
        if acceptance_count > 0:
            stats["acceptance_by_level"] = {
                "scenario": self.count_by_level("scenario"),
                "step": self.count_by_level("step")
            }
            
            stats["acceptance_by_step_type"] = {
                "precondition": self.count_by_step_type("precondition"),
                "action": self.count_by_step_type("action"),
                "outcome": self.count_by_step_type("outcome"),
                "postcondition": self.count_by_step_type("postcondition")
            }
        
        return stats


if __name__ == "__main__":
    # Test metadata store
    print("=" * 70)
    print("METADATA STORE TEST")
    print("=" * 70)
    
    store = MetadataStore(index_version="v1", model_name="all-mpnet-base-v2", embedding_dim=768)
    
    # Add acceptance metadata
    meta1 = AcceptanceMetadata(
        embedding_id="ACC_001",
        scenario_id="SCN_001",
        level="scenario",
        step_type=None,
        source_units=["UAS_001", "UAS_002"],
        text="User registration scenario"
    )
    store.add_acceptance(meta1)
    
    meta2 = AcceptanceMetadata(
        embedding_id="ACC_002",
        scenario_id="SCN_001",
        level="step",
        step_type="action",
        source_units=["UAS_003"],
        text="When user submits form"
    )
    store.add_acceptance(meta2)
    
    # Add CRU metadata
    cru_meta = CRUMetadata(
        embedding_id="CRU_001",
        cru_id="CRU_FR1_01",
        cru_type="functional",
        parent_requirement="FR1",
        text="System establishes account"
    )
    store.add_cru(cru_meta)
    
    # Test lookups
    print(f"\nTotal embeddings: {store.count()}")
    print(f"By scenario SCN_001: {len(store.get_by_scenario('SCN_001'))}")
    print(f"By CRU ID: {store.get_by_cru_id('CRU_FR1_01').embedding_id if store.get_by_cru_id('CRU_FR1_01') else 'None'}")
    
    print("\nStats:")
    for key, value in store.stats().items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 70)
