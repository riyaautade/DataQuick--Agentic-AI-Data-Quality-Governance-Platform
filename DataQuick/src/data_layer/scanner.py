"""Data ingestion and source scanning"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Optional
from loguru import logger
from sqlalchemy.orm import Session
from src.models import Table, ColumnMetadata
from src.database import get_db_session

class DataScanner:
    """Scans and ingests data from various sources"""
    
    def __init__(self):
        self.session = get_db_session()
    
    def scan_csv(self, file_path: str, table_name: str) -> pd.DataFrame:
        """Load and scan CSV file"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"✓ Scanned CSV: {file_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df
        except Exception as e:
            logger.error(f"✗ Failed to scan CSV {file_path}: {e}")
            raise
    
    def scan_excel(self, file_path: str, sheet_name: str = 0) -> pd.DataFrame:
        """Load and scan Excel file"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            logger.info(f"✓ Scanned Excel: {file_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df
        except Exception as e:
            logger.error(f"✗ Failed to scan Excel {file_path}: {e}")
            raise
    
    def infer_data_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer data types for dataframe columns"""
        type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'datetime64': 'TIMESTAMP',
            'bool': 'BOOLEAN',
        }
        
        inferred_types = {}
        for col, dtype in df.dtypes.items():
            inferred_types[col] = type_mapping.get(str(dtype), 'TEXT')
        
        return inferred_types
    
    def register_table(self, table_name: str, df: pd.DataFrame, source_type: str, source_path: str, description: str = "") -> Table:
        """Register a table in the catalog"""
        try:
            # Check if table exists
            existing = self.session.query(Table).filter_by(name=table_name).first()
            if existing:
                logger.warning(f"Table {table_name} already exists, updating...")
                return existing
            
            # Infer data types
            inferred_types = self.infer_data_types(df)
            
            # Create table entry
            table = Table(
                name=table_name,
                source_type=source_type,
                source_path=source_path,
                description=description,
                schema_name="public"
            )
            self.session.add(table)
            self.session.flush()  # Get the ID
            
            # Create column entries
            for position, (col_name, dtype) in enumerate(inferred_types.items()):
                column = ColumnMetadata(
                    table_id=table.id,
                    name=col_name,
                    data_type=dtype,
                    nullable=True,
                    position=position
                )
                self.session.add(column)
            
            self.session.commit()
            logger.info(f"✓ Registered table '{table_name}' in catalog")
            return table
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Failed to register table: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> Optional[Table]:
        """Get table information from catalog"""
        return self.session.query(Table).filter_by(name=table_name).first()
    
    def list_tables(self) -> List[Dict[str, Any]]:
        """List all registered tables"""
        tables = self.session.query(Table).all()
        return [
            {
                "id": t.id,
                "name": t.name,
                "source_type": t.source_type,
                "columns": len(t.columns),
                "created_at": t.created_at.isoformat()
            }
            for t in tables
        ]
