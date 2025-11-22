"""Database connection and utilities"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from loguru import logger
from src.models import Base

# SQLite database (local file)
DB_PATH = "dataquick.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create engine (SQLite doesn't need pool settings)
engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db_session():
    """Get a database session"""
    return SessionLocal()

def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("✓ Database connection successful (SQLite)")
            return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False

def init_db():
    """Initialize database with schema"""
    try:
        # Create all tables using SQLAlchemy models
        Base.metadata.create_all(bind=engine)
        logger.info(f"✓ Database initialized with schema (SQLite: {DB_PATH})")
    except Exception as e:
        logger.error(f"✗ Database initialization failed: {e}")
        raise
