"""Drift detection for schema and data changes"""
import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import desc
from src.models import Profile, Table, ColumnMetadata, SchemaChange, Issue
from src.database import get_db_session
import json

class DriftDetector:
    """Detects schema and data drift between snapshots"""
    
    def __init__(self):
        self.session = get_db_session()
    
    def detect_schema_drift(self, table_id: int, current_columns: List[str], current_types: Dict[str, str]) -> List[Dict[str, Any]]:
        """Detect schema changes (added, removed, or type-changed columns)"""
        try:
            # Get previous schema from database
            table = self.session.query(Table).filter_by(id=table_id).first()
            if not table:
                logger.error(f"Table {table_id} not found")
                return []
            
            previous_columns = {col.name: col.data_type for col in table.columns}
            schema_changes = []
            
            # Detect removed columns
            for col_name, col_type in previous_columns.items():
                if col_name not in current_columns:
                    change = {
                        "table_id": table_id,
                        "change_type": "column_removed",
                        "change_details": {
                            "column_name": col_name,
                            "previous_type": col_type
                        },
                        "detected_by": "drift_detector"
                    }
                    self._save_schema_change(change)
                    schema_changes.append(change)
                    logger.warning(f"✗ Column '{col_name}' removed from table '{table.name}'")
            
            # Detect added and type-changed columns
            for col_name in current_columns:
                if col_name not in previous_columns:
                    change = {
                        "table_id": table_id,
                        "change_type": "column_added",
                        "change_details": {
                            "column_name": col_name,
                            "new_type": current_types.get(col_name)
                        },
                        "detected_by": "drift_detector"
                    }
                    self._save_schema_change(change)
                    schema_changes.append(change)
                    logger.warning(f"✓ Column '{col_name}' added to table '{table.name}'")
                elif previous_columns[col_name] != current_types.get(col_name):
                    change = {
                        "table_id": table_id,
                        "change_type": "type_changed",
                        "change_details": {
                            "column_name": col_name,
                            "previous_type": previous_columns[col_name],
                            "new_type": current_types.get(col_name)
                        },
                        "detected_by": "drift_detector"
                    }
                    self._save_schema_change(change)
                    schema_changes.append(change)
                    logger.warning(f"✗ Type changed for column '{col_name}': {previous_columns[col_name]} → {current_types.get(col_name)}")
            
            return schema_changes
        except Exception as e:
            logger.error(f"✗ Schema drift detection failed: {e}")
            return []
    
    def detect_data_drift(self, table_id: int, column_id: int, new_profile: Dict[str, Any], threshold: float = 0.2) -> Optional[Dict[str, Any]]:
        """Detect data drift in a column (using statistical comparison)"""
        try:
            # Get previous profile
            previous_profile = self.session.query(Profile).filter(
                Profile.table_id == table_id,
                Profile.column_id == column_id
            ).order_by(desc(Profile.profile_timestamp)).offset(1).first()
            
            if not previous_profile:
                logger.debug(f"No previous profile for column {column_id}, skipping drift detection")
                return None
            
            drifts_detected = []
            
            # Null percentage drift
            if previous_profile.null_percentage is not None:
                null_change = abs(new_profile.get("null_percentage", 0) - previous_profile.null_percentage) / (previous_profile.null_percentage + 1)
                if null_change > threshold:
                    drifts_detected.append({
                        "metric": "null_percentage",
                        "previous": previous_profile.null_percentage,
                        "current": new_profile.get("null_percentage", 0),
                        "change_rate": null_change
                    })
            
            # Mean value drift (for numeric columns)
            if previous_profile.mean_value is not None and new_profile.get("mean_value"):
                try:
                    prev_mean = float(previous_profile.mean_value)
                    curr_mean = float(new_profile.get("mean_value", 0))
                    mean_change = abs(curr_mean - prev_mean) / (abs(prev_mean) + 1)
                    if mean_change > threshold:
                        drifts_detected.append({
                            "metric": "mean_value",
                            "previous": prev_mean,
                            "current": curr_mean,
                            "change_rate": mean_change
                        })
                except:
                    pass
            
            # Unique count drift
            if previous_profile.unique_count is not None:
                unique_change = abs(new_profile.get("unique_count", 0) - previous_profile.unique_count) / (previous_profile.unique_count + 1)
                if unique_change > threshold:
                    drifts_detected.append({
                        "metric": "unique_count",
                        "previous": previous_profile.unique_count,
                        "current": new_profile.get("unique_count", 0),
                        "change_rate": unique_change
                    })
            
            if drifts_detected:
                drift_result = {
                    "table_id": table_id,
                    "column_id": column_id,
                    "drifts": drifts_detected,
                    "severity": "high" if len(drifts_detected) > 2 else "medium"
                }
                
                # Save as quality issue
                column = self.session.query(ColumnMetadata).filter_by(id=column_id).first()
                issue = Issue(
                    table_id=table_id,
                    column_id=column_id,
                    issue_type="data_drift",
                    severity=drift_result["severity"],
                    description=f"Data drift detected in {column.name if column else 'unknown'}: {len(drifts_detected)} metrics changed",
                    detected_at=datetime.utcnow(),
                    suggested_fix="Review the data and investigate the root cause of drift"
                )
                self.session.add(issue)
                self.session.commit()
                
                logger.warning(f"✗ Data drift detected in column {column_id}: {drift_result}")
                return drift_result
            
            return None
        except Exception as e:
            logger.error(f"✗ Data drift detection failed: {e}")
            return None
    
    def _save_schema_change(self, change: Dict[str, Any]):
        """Save schema change to database"""
        try:
            schema_change = SchemaChange(
                table_id=change["table_id"],
                change_type=change["change_type"],
                change_details=change["change_details"],
                detected_by=change["detected_by"]
            )
            self.session.add(schema_change)
            self.session.commit()
        except Exception as e:
            logger.error(f"✗ Failed to save schema change: {e}")
            self.session.rollback()
