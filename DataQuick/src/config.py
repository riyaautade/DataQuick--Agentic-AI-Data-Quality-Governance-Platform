"""
DataQuick - Agentic Data Quality & Governance Assistant
Main entry point and configuration
"""
import os
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger.remove()
logger.add(
    "logs/dataquick.log",
    level=LOG_LEVEL,
    format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    rotation="500 MB",
)
logger.add(
    lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<level>{level: <8}</level> | {message}",
)

# Database configuration (SQLite - no config needed, file-based)
DB_CONFIG = {
    "type": "sqlite",
    "path": "./dataquick.db",
}

# Vector store configuration
VECTOR_STORE = {
    "type": "chroma",  # or faiss
    "persist_dir": os.getenv("CHROMA_PERSIST_DIR", "./data/chroma_db"),
    "collection_name": "dataquick_documents",
}

# LLM configuration
LLM_CONFIG = {
    "type": os.getenv("LLM_TYPE", "ollama"),
    "model": os.getenv("OLLAMA_MODEL", "gemma3:4b"),
    "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
}

# Embedding configuration
EMBEDDING_CONFIG = {
    "model": os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2"),
}

# Feature flags
FEATURES = {
    "rag_enabled": os.getenv("ENABLE_RAG", "true").lower() == "true",
    "lineage_enabled": os.getenv("ENABLE_LINEAGE", "true").lower() == "true",
    "drift_detection_enabled": os.getenv("ENABLE_DRIFT_DETECTION", "true").lower() == "true",
}

# Scheduler configuration
SCHEDULER_CONFIG = {
    "enabled": os.getenv("SCHEDULER_ENABLED", "true").lower() == "true",
    "interval_hours": int(os.getenv("SCHEDULE_INTERVAL_HOURS", 24)),
}

logger.info("✓ DataQuick configuration loaded")
