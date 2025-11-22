"""Vector store and RAG (Retrieval-Augmented Generation) setup"""
import os
from typing import List, Dict, Any, Optional
from loguru import logger
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from src.config import VECTOR_STORE, EMBEDDING_CONFIG
import json

class VectorStore:
    """Local vector store for RAG"""
    
    def __init__(self):
        self.embedding_model = SentenceTransformer(EMBEDDING_CONFIG["model"])
        self.chroma_settings = Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=VECTOR_STORE["persist_dir"],
            anonymized_telemetry=False
        )
        self.client = chromadb.Client(self.chroma_settings)
        self.collection = self.client.get_or_create_collection(
            name=VECTOR_STORE["collection_name"],
            metadata={"hnsw:space": "cosine"}
        )
        logger.info(f"✓ Vector store initialized: {VECTOR_STORE['persist_dir']}")
    
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for text"""
        try:
            embedding = self.embedding_model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"✗ Failed to embed text: {e}")
            raise
    
    def add_document(self, doc_id: str, text: str, metadata: Dict[str, Any] = None) -> str:
        """Add a document to the vector store"""
        try:
            embedding = self.embed_text(text)
            metadata = metadata or {}
            metadata["text"] = text
            
            self.collection.add(
                ids=[doc_id],
                embeddings=[embedding],
                metadatas=[metadata],
                documents=[text]
            )
            logger.debug(f"✓ Added document: {doc_id}")
            return doc_id
        except Exception as e:
            logger.error(f"✗ Failed to add document: {e}")
            raise
    
    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """Search for relevant documents"""
        try:
            query_embedding = self.embed_text(query)
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=k
            )
            
            documents = []
            if results and len(results["ids"]) > 0:
                for i, doc_id in enumerate(results["ids"][0]):
                    documents.append({
                        "id": doc_id,
                        "distance": float(results["distances"][0][i]),
                        "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                        "text": results["documents"][0][i] if results["documents"] else ""
                    })
            
            return documents
        except Exception as e:
            logger.error(f"✗ Failed to search: {e}")
            return []
    
    def add_profile_report(self, table_name: str, profile_data: Dict[str, Any]) -> None:
        """Add a data profile report to the vector store"""
        try:
            doc_text = self._format_profile_for_rag(table_name, profile_data)
            doc_id = f"profile_{table_name}_{profile_data.get('profile_timestamp', 'latest')}"
            self.add_document(
                doc_id=doc_id,
                text=doc_text,
                metadata={
                    "type": "profile",
                    "table_name": table_name,
                    "timestamp": profile_data.get("profile_timestamp")
                }
            )
            logger.info(f"✓ Added profile report for {table_name} to vector store")
        except Exception as e:
            logger.error(f"✗ Failed to add profile report: {e}")
    
    def _format_profile_for_rag(self, table_name: str, profile_data: Dict[str, Any]) -> str:
        """Format profile data as human-readable text for embedding"""
        text_parts = [f"Data Profile Report for table: {table_name}"]
        text_parts.append(f"Generated at: {profile_data.get('profile_timestamp', 'unknown')}")
        text_parts.append(f"Total rows: {profile_data.get('row_count', 0)}")
        text_parts.append(f"Total columns: {profile_data.get('column_count', 0)}")
        
        for col_profile in profile_data.get("column_profiles", []):
            text_parts.append(f"\nColumn: {col_profile.get('column_name')}")
            text_parts.append(f"Type: {col_profile.get('data_type')}")
            text_parts.append(f"Null percentage: {col_profile.get('null_percentage', 0):.2f}%")
            text_parts.append(f"Unique values: {col_profile.get('unique_count', 0)}")
            if col_profile.get("mean_value"):
                text_parts.append(f"Mean: {col_profile.get('mean_value')}")
        
        return "\n".join(text_parts)
    
    def persist(self):
        """Persist vector store to disk"""
        try:
            self.client.persist()
            logger.info("✓ Vector store persisted to disk")
        except Exception as e:
            logger.error(f"✗ Failed to persist vector store: {e}")
