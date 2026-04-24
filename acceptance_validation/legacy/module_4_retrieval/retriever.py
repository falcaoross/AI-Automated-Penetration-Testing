"""
Retriever Module

Dense retrieval of candidate CRUs using FAISS cosine similarity
(METRIC_INNER_PRODUCT with L2-normalized vectors).
"""

import numpy as np
import faiss
from typing import List
from dataclasses import dataclass

from acceptance_validation.module_3_embedding_indexing.embedding_model import EmbeddingModel
from acceptance_validation.module_3_embedding_indexing.index_metadata import MetadataStore


@dataclass
class CandidateResult:
    """Structured candidate result."""
    cru_id: str
    similarity: float
    cru_type: str
    parent_requirement: str


class CRURetriever:
    """Dense retrieval for CRU candidates using cosine similarity."""
    
    def __init__(
        self,
        embedding_model: EmbeddingModel,
        cru_index: faiss.Index,
        cru_metadata: MetadataStore
    ):
        self.embedding_model = embedding_model
        self.cru_index = cru_index
        self.cru_metadata = cru_metadata

    def retrieve_candidates(self, text: str, top_k: int = 5) -> List[CandidateResult]:
        """
        Retrieve top-K candidate CRUs for query text.
        Uses cosine similarity via INNER_PRODUCT.
        """

        # 1️⃣ Embed query
        query_embedding = self.embedding_model.encode(
            text,
            normalize=True,
            show_progress=False
        )

        query_embedding = query_embedding.astype("float32").reshape(1, -1)

        # 2️⃣ Ensure L2 normalization (safety)
        faiss.normalize_L2(query_embedding)

        # 3️⃣ Search index
        # For METRIC_INNER_PRODUCT + normalized vectors,
        # distances ARE cosine similarity scores.
        similarities, indices = self.cru_index.search(query_embedding, top_k)

        candidates = []

        for idx, score in zip(indices[0], similarities[0]):
            if idx < 0:
                continue

            embedding_id = f"CRU_EMB_{idx + 1:05d}"
            metadata = self.cru_metadata.get(embedding_id)

            if metadata:
                candidates.append(
                    CandidateResult(
                        cru_id=metadata.cru_id,
                        similarity=float(score),  # already cosine similarity
                        cru_type=metadata.cru_type,
                        parent_requirement=metadata.parent_requirement
                    )
                )

        return candidates


def create_retriever(
    embedding_model: EmbeddingModel,
    cru_index: faiss.Index,
    cru_metadata: MetadataStore
) -> CRURetriever:
    return CRURetriever(
        embedding_model=embedding_model,
        cru_index=cru_index,
        cru_metadata=cru_metadata
    )