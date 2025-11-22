"""Lineage tracking and dependency management"""
from typing import List, Dict, Tuple, Optional, Set
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import and_
from src.models import Table, ColumnMetadata, LineageEdge, LineageRun
from src.database import get_db_session
import networkx as nx
from datetime import datetime

class LineageTracker:
    """Tracks column and table lineage/dependencies"""
    
    def __init__(self):
        self.session = get_db_session()
    
    def add_lineage_edge(self, source_column_id: int, target_column_id: int, lineage_type: str = "direct", transformation_logic: str = "") -> LineageEdge:
        """Add a lineage edge between two columns"""
        try:
            # Check if edge already exists
            existing = self.session.query(LineageEdge).filter(
                and_(
                    LineageEdge.source_column_id == source_column_id,
                    LineageEdge.target_column_id == target_column_id
                )
            ).first()
            
            if existing:
                logger.debug(f"Lineage edge {source_column_id} → {target_column_id} already exists")
                return existing
            
            edge = LineageEdge(
                source_column_id=source_column_id,
                target_column_id=target_column_id,
                lineage_type=lineage_type,
                transformation_logic=transformation_logic
            )
            self.session.add(edge)
            self.session.commit()
            logger.info(f"✓ Added lineage edge: {source_column_id} → {target_column_id}")
            return edge
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Failed to add lineage edge: {e}")
            raise
    
    def get_upstream_lineage(self, column_id: int, depth: int = 10) -> Dict[str, any]:
        """Get all upstream dependencies for a column"""
        try:
            visited = set()
            dependencies = []
            self._traverse_upstream(column_id, visited, dependencies, depth)
            
            return {
                "column_id": column_id,
                "upstream_count": len(dependencies),
                "upstream_columns": dependencies
            }
        except Exception as e:
            logger.error(f"✗ Failed to get upstream lineage: {e}")
            return {"column_id": column_id, "error": str(e)}
    
    def get_downstream_lineage(self, column_id: int, depth: int = 10) -> Dict[str, any]:
        """Get all downstream dependencies for a column"""
        try:
            visited = set()
            dependents = []
            self._traverse_downstream(column_id, visited, dependents, depth)
            
            return {
                "column_id": column_id,
                "downstream_count": len(dependents),
                "downstream_columns": dependents
            }
        except Exception as e:
            logger.error(f"✗ Failed to get downstream lineage: {e}")
            return {"column_id": column_id, "error": str(e)}
    
    def get_lineage_graph(self) -> nx.DiGraph:
        """Get complete lineage as a directed graph"""
        try:
            G = nx.DiGraph()
            edges = self.session.query(LineageEdge).all()
            
            for edge in edges:
                source_col = self.session.query(ColumnMetadata).filter_by(id=edge.source_column_id).first()
                target_col = self.session.query(ColumnMetadata).filter_by(id=edge.target_column_id).first()
                
                if source_col and target_col:
                    source_label = f"{source_col.table.name}.{source_col.name}"
                    target_label = f"{target_col.table.name}.{target_col.name}"
                    G.add_edge(source_label, target_label, type=edge.lineage_type)
            
            logger.info(f"✓ Lineage graph contains {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
            return G
        except Exception as e:
            logger.error(f"✗ Failed to get lineage graph: {e}")
            return nx.DiGraph()
    
    def record_lineage_run(self, source_table_id: int, target_table_id: int, row_count_source: int, row_count_target: int, status: str = "success"):
        """Record a lineage run (data pipeline execution)"""
        try:
            run = LineageRun(
                source_table_id=source_table_id,
                target_table_id=target_table_id,
                row_count_source=row_count_source,
                row_count_target=row_count_target,
                status=status
            )
            self.session.add(run)
            self.session.commit()
            logger.info(f"✓ Recorded lineage run: {source_table_id} → {target_table_id}")
            return run
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Failed to record lineage run: {e}")
            raise
    
    def _traverse_upstream(self, column_id: int, visited: Set[int], dependencies: List[Dict], depth: int):
        """Recursively traverse upstream dependencies"""
        if depth == 0 or column_id in visited:
            return
        
        visited.add(column_id)
        edges = self.session.query(LineageEdge).filter_by(target_column_id=column_id).all()
        
        for edge in edges:
            source_col = self.session.query(ColumnMetadata).filter_by(id=edge.source_column_id).first()
            if source_col:
                dependencies.append({
                    "column_id": source_col.id,
                    "column_name": source_col.name,
                    "table_name": source_col.table.name,
                    "type": edge.lineage_type
                })
                self._traverse_upstream(edge.source_column_id, visited, dependencies, depth - 1)
    
    def _traverse_downstream(self, column_id: int, visited: Set[int], dependents: List[Dict], depth: int):
        """Recursively traverse downstream dependencies"""
        if depth == 0 or column_id in visited:
            return
        
        visited.add(column_id)
        edges = self.session.query(LineageEdge).filter_by(source_column_id=column_id).all()
        
        for edge in edges:
            target_col = self.session.query(ColumnMetadata).filter_by(id=edge.target_column_id).first()
            if target_col:
                dependents.append({
                    "column_id": target_col.id,
                    "column_name": target_col.name,
                    "table_name": target_col.table.name,
                    "type": edge.lineage_type
                })
                self._traverse_downstream(edge.target_column_id, visited, dependents, depth - 1)
