"""Advanced data quality analyzer for detecting all data issues"""
import pandas as pd
import re
from typing import Dict, List, Any, Tuple
from datetime import datetime
from loguru import logger
import numpy as np
from src.models import Issue
from src.database import get_db_session

class DataQualityAnalyzer:
    """Comprehensive data quality analyzer"""
    
    def __init__(self):
        self.session = get_db_session()
        self.patterns = {
            'date': r'^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$|^\d{1,2}[-/]\d{1,2}[-/]\d{1,4}$',
            'phone': r'^[\d\s\-\+\(\)]+$',
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'currency': r'^[\$£€]?\s*\d+(\.\d{2})?$',
            'percent': r'^\d+(\.\d+)?%?$'
        }
    
    def analyze_dataframe(self, df: pd.DataFrame, table_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Comprehensive quality analysis of entire dataframe"""
        issues = []
        
        for col_idx, col_name in enumerate(df.columns):
            col_issues = self._analyze_column(df[col_name], col_name, col_idx, table_id, table_name)
            issues.extend(col_issues)
        
        return issues
    
    def _analyze_column(self, series: pd.Series, col_name: str, col_idx: int, table_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Analyze a single column for quality issues"""
        issues = []
        
        # Get column ID from database
        from src.models import Table, ColumnMetadata
        table = self.session.query(Table).filter_by(id=table_id).first()
        column = None
        if table:
            column = self.session.query(ColumnMetadata).filter_by(table_id=table_id, position=col_idx).first()
        
        column_id = column.id if column else None
        
        # 1. NULL/MISSING VALUES
        null_count = series.isnull().sum()
        null_pct = (null_count / len(series)) * 100 if len(series) > 0 else 0
        
        if null_count > 0:
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'missing_values',
                'severity': 'critical' if null_pct > 30 else 'high' if null_pct > 10 else 'medium',
                'count': null_count,
                'percentage': null_pct,
                'description': f'{null_count} null/missing values ({null_pct:.1f}%)',
                'examples': list(series[series.isnull()].index[:3].astype(str)),
                'suggested_fix': f'DELETE FROM {table_name} WHERE {col_name} IS NULL; -- or impute with median/mode'
            })
        
        # 2. DUPLICATES
        non_null_series = series.dropna()
        if len(non_null_series) > 0:
            dup_count = non_null_series.duplicated().sum()
            dup_pct = (dup_count / len(non_null_series)) * 100 if len(non_null_series) > 0 else 0
            
            if dup_count > 0:
                issues.append({
                    'table_id': table_id,
                    'column_id': column_id,
                    'column_name': col_name,
                    'issue_type': 'duplicates',
                    'severity': 'medium' if dup_pct < 20 else 'high',
                    'count': dup_count,
                    'percentage': dup_pct,
                    'description': f'{dup_count} duplicate values ({dup_pct:.1f}%)',
                    'examples': list(non_null_series[non_null_series.duplicated()].unique()[:3]),
                    'suggested_fix': f'DELETE FROM {table_name} WHERE ctid NOT IN (SELECT MIN(ctid) FROM {table_name} GROUP BY {col_name});'
                })
        
        # Analyze by data type
        inferred_type = self._infer_type(series)
        
        if inferred_type == 'numeric':
            issues.extend(self._check_numeric_issues(series, col_name, column_id, table_id, table_name))
        elif inferred_type == 'date':
            issues.extend(self._check_date_issues(series, col_name, column_id, table_id, table_name))
        elif inferred_type == 'categorical':
            issues.extend(self._check_categorical_issues(series, col_name, column_id, table_id, table_name))
        else:
            issues.extend(self._check_string_issues(series, col_name, column_id, table_id, table_name))
        
        return issues
    
    def _infer_type(self, series: pd.Series) -> str:
        """Infer the intended data type"""
        non_null = series.dropna()
        if len(non_null) == 0:
            return 'unknown'
        
        # Check if numeric
        try:
            pd.to_numeric(non_null, errors='raise')
            return 'numeric'
        except:
            pass
        
        # Check if date
        if self._is_date_column(non_null):
            return 'date'
        
        # Check if categorical (few unique values)
        if len(non_null.unique()) / len(non_null) < 0.05 or len(non_null.unique()) < 20:
            return 'categorical'
        
        return 'string'
    
    def _is_date_column(self, series: pd.Series) -> bool:
        """Check if series contains date values"""
        non_null = series.dropna().astype(str).head(20)
        date_patterns = [
            r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}',
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
        ]
        
        date_matches = sum(1 for val in non_null if any(re.search(p, str(val)) for p in date_patterns))
        return date_matches / len(non_null) > 0.5 if len(non_null) > 0 else False
    
    def _check_numeric_issues(self, series: pd.Series, col_name: str, column_id: int, table_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Detect numeric column issues"""
        issues = []
        non_null = series.dropna()
        
        # Check for non-numeric strings in numeric column
        invalid_numeric = []
        for val in non_null.unique():
            try:
                float(val)
            except (ValueError, TypeError):
                invalid_numeric.append(str(val))
        
        if invalid_numeric:
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'invalid_numeric',
                'severity': 'critical',
                'count': len([v for v in non_null if str(v) in invalid_numeric]),
                'percentage': (len([v for v in non_null if str(v) in invalid_numeric]) / len(non_null)) * 100,
                'description': f'Non-numeric values in numeric column: {", ".join(invalid_numeric[:3])}',
                'examples': invalid_numeric[:3],
                'suggested_fix': f'DELETE FROM {table_name} WHERE {col_name} ~ \'[^0-9.-]\'; -- Remove non-numeric rows'
            })
        
        # Check for negative values (if they shouldn't be negative)
        numeric_vals = pd.to_numeric(non_null, errors='coerce').dropna()
        if len(numeric_vals) > 0:
            negative_count = (numeric_vals < 0).sum()
            if negative_count > 0:
                issues.append({
                    'table_id': table_id,
                    'column_id': column_id,
                    'column_name': col_name,
                    'issue_type': 'negative_values',
                    'severity': 'high',
                    'count': negative_count,
                    'percentage': (negative_count / len(numeric_vals)) * 100,
                    'description': f'{negative_count} negative values in {col_name}',
                    'examples': list(numeric_vals[numeric_vals < 0].unique()[:3]),
                    'suggested_fix': f'UPDATE {table_name} SET {col_name} = ABS({col_name}) WHERE {col_name} < 0;'
                })
        
        # Check for outliers (IQR method)
        try:
            Q1 = numeric_vals.quantile(0.25)
            Q3 = numeric_vals.quantile(0.75)
            IQR = Q3 - Q1
            outliers = (numeric_vals < Q1 - 1.5 * IQR) | (numeric_vals > Q3 + 1.5 * IQR)
            outlier_count = outliers.sum()
            
            if outlier_count > 0:
                issues.append({
                    'table_id': table_id,
                    'column_id': column_id,
                    'column_name': col_name,
                    'issue_type': 'outliers',
                    'severity': 'low' if (outlier_count / len(numeric_vals)) < 0.05 else 'medium',
                    'count': outlier_count,
                    'percentage': (outlier_count / len(numeric_vals)) * 100,
                    'description': f'{outlier_count} outlier values detected',
                    'examples': list(numeric_vals[outliers].unique()[:3]),
                    'suggested_fix': f'DELETE FROM {table_name} WHERE {col_name} < {Q1 - 1.5 * IQR} OR {col_name} > {Q3 + 1.5 * IQR};'
                })
        except:
            pass
        
        return issues
    
    def _check_date_issues(self, series: pd.Series, col_name: str, column_id: int, table_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Detect date column issues"""
        issues = []
        non_null = series.dropna().astype(str)
        
        # Check for mixed date formats
        date_formats = {}
        for val in non_null.unique()[:100]:
            if re.match(r'^\d{4}-\d{2}-\d{2}', str(val)):
                fmt = 'YYYY-MM-DD'
            elif re.match(r'^\d{2}/\d{2}/\d{4}', str(val)):
                fmt = 'MM/DD/YYYY'
            elif re.match(r'^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', str(val)):
                fmt = 'Mixed format'
            else:
                fmt = 'Invalid'
            
            date_formats[fmt] = date_formats.get(fmt, 0) + 1
        
        if len(date_formats) > 1:
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'mixed_date_formats',
                'severity': 'high',
                'count': len(non_null),
                'percentage': 100,
                'description': f'Mixed date formats: {list(date_formats.keys())}',
                'examples': list(non_null.unique()[:3]),
                'suggested_fix': f'ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE date USING to_date({col_name}, \'YYYY-MM-DD\');'
            })
        
        return issues
    
    def _check_categorical_issues(self, series: pd.Series, col_name: str, column_id: int, table_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Detect categorical column issues"""
        issues = []
        non_null = series.dropna().astype(str)
        
        # Check for casing inconsistencies
        unique_lower = non_null.str.lower().unique()
        if len(unique_lower) < len(non_null.unique()):
            case_issues = non_null.unique()
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'case_sensitivity',
                'severity': 'medium',
                'count': len(case_issues),
                'percentage': (len(case_issues) / len(non_null)) * 100,
                'description': f'Inconsistent casing in categorical column',
                'examples': list(case_issues[:3]),
                'suggested_fix': f'UPDATE {table_name} SET {col_name} = LOWER({col_name});'
            })
        
        # Check for whitespace issues
        whitespace_issues = non_null[non_null.str.strip() != non_null].unique()
        if len(whitespace_issues) > 0:
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'whitespace_issues',
                'severity': 'medium',
                'count': len(whitespace_issues),
                'percentage': (len(whitespace_issues) / len(non_null)) * 100,
                'description': f'Whitespace inconsistencies found',
                'examples': [f"'{v}'" for v in whitespace_issues[:3]],
                'suggested_fix': f'UPDATE {table_name} SET {col_name} = TRIM({col_name});'
            })
        
        return issues
    
    def _check_string_issues(self, series: pd.Series, col_name: str, column_id: int, table_id: int, table_name: str) -> List[Dict[str, Any]]:
        """Detect string column issues"""
        issues = []
        non_null = series.dropna().astype(str)
        
        if len(non_null) == 0:
            return issues
        
        # Check for special characters and noise
        has_special = non_null[non_null.str.contains(r'[^\w\s\-\.]', regex=True)].unique()
        if len(has_special) > 0:
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'special_characters',
                'severity': 'low',
                'count': len(has_special),
                'percentage': (len(has_special) / len(non_null)) * 100,
                'description': f'Special characters or noise in string column',
                'examples': list(has_special[:3]),
                'suggested_fix': f'UPDATE {table_name} SET {col_name} = REGEXP_REPLACE({col_name}, \'[^\\w\\s-]\', \'\', \'g\');'
            })
        
        # Check for very long strings
        max_len = non_null.str.len().max()
        if max_len > 1000:
            issues.append({
                'table_id': table_id,
                'column_id': column_id,
                'column_name': col_name,
                'issue_type': 'unusually_long_values',
                'severity': 'low',
                'count': (non_null.str.len() > 1000).sum(),
                'percentage': ((non_null.str.len() > 1000).sum() / len(non_null)) * 100,
                'description': f'Very long string values (max: {max_len} chars)',
                'examples': [non_null[non_null.str.len() > 1000].iloc[0][:100] + '...'],
                'suggested_fix': f'SELECT * FROM {table_name} WHERE LENGTH({col_name}) > 1000;'
            })
        
        return issues
    
    def save_issues_to_db(self, issues: List[Dict[str, Any]]):
        """Save detected issues to database"""
        try:
            for issue_data in issues:
                issue = Issue(
                    table_id=issue_data['table_id'],
                    column_id=issue_data.get('column_id'),
                    issue_type=issue_data['issue_type'],
                    severity=issue_data['severity'],
                    description=issue_data['description'],
                    detected_at=datetime.utcnow(),
                    suggested_fix=issue_data.get('suggested_fix')
                )
                self.session.add(issue)
            
            self.session.commit()
            logger.info(f"✓ Saved {len(issues)} quality issues to database")
        except Exception as e:
            logger.error(f"✗ Failed to save issues: {e}")
            self.session.rollback()
