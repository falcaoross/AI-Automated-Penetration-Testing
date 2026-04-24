"""
Embedding Model Module - Mac M4 Pro Optimized

Loads sentence transformer model with MPS backend support.
Handles batching, normalization, and returns float32 numpy vectors.
Includes workarounds for MPS-specific issues.
"""

import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from typing import List, Union
import time
import os
import warnings

# Critical: Disable tokenizers parallelism to prevent MPS segfaults
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

# Suppress MPS fallback warnings
warnings.filterwarnings('ignore', category=UserWarning, message='.*MPS.*')

# Force single-threaded execution
torch.set_num_threads(1)
torch.set_num_interop_threads(1)


class EmbeddingModel:
    """Manages embedding generation with SentenceTransformers."""
    
    def __init__(self, model_name: str = "all-mpnet-base-v2", batch_size: int = 16):
        """
        Initialize embedding model.
        
        Args:
            model_name: HuggingFace model name
            batch_size: Batch size for encoding (reduced to 16 for MPS stability)
        """
        self.model_name = model_name
        self.batch_size = batch_size
        self.device = self._detect_and_configure_device()
        
        print(f"Loading embedding model: {model_name}")
        print(f"Target device: {self.device}")
        
        # Load model with error handling
        try:
            self.model = SentenceTransformer(model_name)
            # Explicitly move to device after loading
            if self.device != "cpu":
                try:
                    self.model = self.model.to(self.device)
                    print(f"✓ Model loaded on {self.device}")
                except Exception as e:
                    print(f"⚠ Failed to move to {self.device}: {e}")
                    print(f"→ Falling back to CPU")
                    self.device = "cpu"
                    self.model = self.model.to("cpu")
            else:
                print(f"✓ Model loaded on CPU")
                
        except Exception as e:
            print(f"✗ Error loading model: {e}")
            raise
        
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        print(f"Embedding dimension: {self.embedding_dim}\n")
    
    def _detect_and_configure_device(self) -> str:
        """
        Detect available device and configure for MPS stability.
        
        Returns:
            Device string: "mps", "cuda", or "cpu"
        """
        # Check MPS availability (Mac M4 Pro)
        if torch.backends.mps.is_available():
            try:
                # Test MPS with a simple operation
                test_tensor = torch.zeros(1, device="mps")
                _ = test_tensor + 1
                return "mps"
            except Exception as e:
                print(f"⚠ MPS available but not functional: {e}")
                print(f"→ Using CPU instead")
                return "cpu"
        
        # Check CUDA
        elif torch.cuda.is_available():
            return "cuda"
        
        # Fallback to CPU
        else:
            return "cpu"
    
    def encode(
        self,
        texts: Union[str, List[str]],
        normalize: bool = True,
        show_progress: bool = False
    ) -> np.ndarray:
        """
        Encode texts to embeddings.
        
        Args:
            texts: Single text or list of texts
            normalize: Apply L2 normalization
            show_progress: Show progress bar
            
        Returns:
            numpy array of shape (n, embedding_dim) in float32
        """
        if isinstance(texts, str):
            texts = [texts]
        
        start_time = time.time()
        
        # Encode with MPS-safe settings
        try:
            embeddings = self.model.encode(
                texts,
                batch_size=16,
                show_progress_bar=show_progress,
                convert_to_numpy=True,
                normalize_embeddings=False,
                device=self.device if self.device != "mps" else None
            )
        except Exception as e:
            print(f"⚠ Encoding failed on {self.device}: {e}")
            print(f"→ Retrying on CPU...")
            
            # Fallback: move model to CPU and retry
            original_device = self.device
            self.device = "cpu"
            self.model = self.model.to("cpu")
            
            embeddings = self.model.encode(
                texts,
                batch_size=16,
                show_progress_bar=show_progress,
                convert_to_numpy=True,
                normalize_embeddings=False,
                device="cpu"
            )
            
            print(f"✓ Successfully encoded on CPU (device switched from {original_device})")
        
        # Ensure float32 and contiguous
        embeddings = np.ascontiguousarray(embeddings, dtype=np.float32)
        
        # Apply normalization manually if requested
        if normalize:
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            embeddings = embeddings / (norms + 1e-8)
        
        duration = time.time() - start_time
        
        if show_progress or len(texts) > 50:
            print(f"Encoded {len(texts)} texts in {duration:.2f}s")
            print(f"Shape: {embeddings.shape}, Device used: {self.device}")
        
        return embeddings
    
    def encode_batch(
        self,
        texts: List[str],
        batch_size: int = None,
        show_progress: bool = True
    ) -> np.ndarray:
        """
        Encode texts in batches with progress reporting.
        
        Args:
            texts: List of texts to encode
            batch_size: Override default batch size
            show_progress: Print progress
            
        Returns:
            numpy array of embeddings
        """
        if batch_size is None:
            batch_size = self.batch_size
        
        # Temporarily override batch size
        original_batch_size = self.batch_size
        self.batch_size = batch_size
        
        result = self.encode(texts, normalize=True, show_progress=show_progress)
        
        # Restore original batch size
        self.batch_size = original_batch_size
        
        return result


def load_embedding_model(
    model_name: str = "all-mpnet-base-v2",
    batch_size: int = 16
) -> EmbeddingModel:
    """
    Load embedding model with automatic device detection.
    
    Args:
        model_name: Model to load
        batch_size: Batch size for encoding (default 16 for MPS stability)
        
    Returns:
        EmbeddingModel instance
    """
    return EmbeddingModel(model_name=model_name, batch_size=batch_size)


if __name__ == "__main__":
    # Test the embedding model
    print("=" * 70)
    print("EMBEDDING MODEL TEST")
    print("=" * 70 + "\n")
    
    model = load_embedding_model()
    
    # Test single text
    print("Test 1: Single embedding")
    test_text = "The system shall authenticate users with valid credentials."
    embedding = model.encode(test_text)
    
    print(f"  Shape: {embedding.shape}")
    print(f"  Dtype: {embedding.dtype}")
    print(f"  L2 norm: {np.linalg.norm(embedding[0]):.4f}")
    print()
    
    # Test batch
    print("Test 2: Batch embeddings")
    test_texts = [
        "Given the user is on the login page",
        "When the user enters valid credentials",
        "Then the system authenticates the user"
    ]
    
    embeddings = model.encode_batch(test_texts, show_progress=True)
    
    print(f"  Shape: {embeddings.shape}")
    print(f"  Dtype: {embeddings.dtype}")
    print(f"  L2 norms: {[f'{np.linalg.norm(e):.4f}' for e in embeddings]}")
    
    print("\n" + "=" * 70)
    print("✓ All tests passed!")
    print("=" * 70)