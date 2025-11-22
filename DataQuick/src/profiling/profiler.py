"""Data profiling and quality metrics computation"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session
from src.models import Profile, Table, ColumnMetadata
from src.database import get_db_session
import json

class DataProfiler:
    """Computes comprehensive data profiles and quality metrics"""
    
    def __init__(self):
        self.session = get_db_session()
    
    def profile_column(self, df: pd.Series, column_name: str, data_type: str) -> Dict[str, Any]:
        """Profile a single column and compute statistics"""
        try:
            profile = {
                "column_name": column_name,
                "data_type": data_type,
                "null_count": int(df.isna().sum()),
                "null_percentage": float((df.isna().sum() / len(df) * 100) if len(df) > 0 else 0),
                "unique_count": int(df.nunique()),
                "unique_percentage": float((df.nunique() / len(df) * 100) if len(df) > 0 else 0),
            }
            
            # Numeric statistics
            if data_type in ['INTEGER', 'FLOAT']:
                numeric_series = pd.to_numeric(df, errors='coerce')
                profile["min_value"] = str(numeric_series.min()) if not pd.isna(numeric_series.min()) else None
                profile["max_value"] = str(numeric_series.max()) if not pd.isna(numeric_series.max()) else None
                profile["mean_value"] = float(numeric_series.mean()) if not pd.isna(numeric_series.mean()) else None
                profile["median_value"] = float(numeric_series.median()) if not pd.isna(numeric_series.median()) else None
                profile["std_dev"] = float(numeric_series.std()) if not pd.isna(numeric_series.std()) else None
                
                # Histogram bins
                if len(numeric_series.dropna()) > 0:
                    hist, bins = np.histogram(numeric_series.dropna(), bins=10)
                    profile["histogram_bins"] = {
                        "counts": hist.tolist(),
                        "bins": bins.tolist()
                    }
            
            # Sample values (top 10 unique non-null values)
            sample_values = df.dropna().unique()[:10]
            profile["sample_values"] = [str(v) for v in sample_values]
            
            # Detect outliers (IQR method for numeric columns)
            if data_type in ['INTEGER', 'FLOAT']:
                numeric_series = pd.to_numeric(df, errors='coerce')
                Q1 = numeric_series.quantile(0.25)
                Q3 = numeric_series.quantile(0.75)
                IQR = Q3 - Q1
                outlier_count = ((numeric_series < (Q1 - 1.5 * IQR)) | (numeric_series > (Q3 + 1.5 * IQR))).sum()
                profile["outlier_count"] = int(outlier_count) if not pd.isna(outlier_count) else 0
            
            return profile
        except Exception as e:
            logger.error(f"✗ Failed to profile column {column_name}: {e}")
            return {"column_name": column_name, "error": str(e)}
    
    def profile_dataframe(self, df: pd.DataFrame, table_id: int, table_name: str = "") -> Dict[str, Any]:
        """Profile an entire dataframe"""
        try:
            profiles = []
            
            for col in df.columns:
                data_type = self._infer_type(df[col])
                col_profile = self.profile_column(df[col], col, data_type)
                profiles.append(col_profile)
            
            overall_profile = {
                "table_id": table_id,
                "table_name": table_name,
                "profile_timestamp": datetime.utcnow().isoformat(),
                "row_count": len(df),
                "column_count": len(df.columns),
                "column_profiles": profiles,
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
            }
            
            logger.info(f"✓ Profiled table '{table_name}' ({len(df)} rows)")
            return overall_profile
        except Exception as e:
            logger.error(f"✗ Failed to profile dataframe: {e}")
            raise
    
    def save_profile(self, table_id: int, df: pd.DataFrame, table_obj: Optional[Table] = None) -> List[Profile]:
        """Save profile to database"""
        try:
            if not table_obj:
                table_obj = self.session.query(Table).filter_by(id=table_id).first()
            
            saved_profiles = []
            for col in df.columns:
                column_obj = self.session.query(ColumnMetadata).filter_by(
                    table_id=table_id,
                    name=col
                ).first()
                
                if not column_obj:
                    continue
                
                data_type = self._infer_type(df[col])
                col_profile = self.profile_column(df[col], col, data_type)
                
                # Create profile record
                profile = Profile(
                    table_id=table_id,
                    column_id=column_obj.id,
                    row_count=len(df),
                    null_count=col_profile.get("null_count", 0),
                    null_percentage=col_profile.get("null_percentage", 0),
                    unique_count=col_profile.get("unique_count", 0),
                    unique_percentage=col_profile.get("unique_percentage", 0),
                    min_value=col_profile.get("min_value"),
                    max_value=col_profile.get("max_value"),
                    mean_value=col_profile.get("mean_value"),
                    median_value=col_profile.get("median_value"),
                    std_dev=col_profile.get("std_dev"),
                    sample_values=col_profile.get("sample_values"),
                    histogram_bins=col_profile.get("histogram_bins"),
                    data_type=data_type,
                    profile_data=col_profile
                )
                self.session.add(profile)
                saved_profiles.append(profile)
            
            self.session.commit()
            logger.info(f"✓ Saved {len(saved_profiles)} column profiles to database")
            return saved_profiles
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Failed to save profile: {e}")
            raise
    
    def _infer_type(self, series: pd.Series) -> str:
        """Infer data type for a series"""
        dtype = str(series.dtype)
        if 'int' in dtype:
            return 'INTEGER'
        elif 'float' in dtype:
            return 'FLOAT'
        elif 'datetime' in dtype:
            return 'TIMESTAMP'
        elif 'bool' in dtype:
            return 'BOOLEAN'
        else:
            return 'TEXT'
