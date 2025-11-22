"""Main entry point"""
import sys
from loguru import logger
from src.config import logger as config_logger
from src.database import test_connection, init_db
from src.scheduler.job_scheduler import start_scheduler

def main():
    logger.info("=" * 60)
    logger.info("🚀 DataQuick - Agentic Data Quality & Governance Assistant")
    logger.info("=" * 60)
    
    # Test database connection
    if not test_connection():
        logger.error("Cannot connect to database. Please ensure Postgres is running.")
        logger.info("Run: docker-compose -f docker/docker-compose.yml up -d")
        sys.exit(1)
    
    # Initialize database
    try:
        init_db()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)
    
    # Start scheduler
    start_scheduler()
    
    logger.info("✓ All systems initialized successfully!")
    logger.info("✓ Starting Streamlit UI...")
    logger.info("Web UI will be available at: http://localhost:8501")
    
    # In production, Streamlit would be started separately
    # For now, just indicate it's ready

if __name__ == "__main__":
    main()
