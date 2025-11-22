"""Test utilities and example tests"""
import pytest
import pandas as pd
from pathlib import Path

@pytest.fixture
def sample_dataframe():
    """Create sample dataframe for testing"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'salary': [50000, 60000, None, 70000, 65000],
        'joined_date': pd.date_range('2020-01-01', periods=5)
    })

@pytest.fixture
def sample_csv_file(tmp_path, sample_dataframe):
    """Create sample CSV file"""
    csv_path = tmp_path / "test_data.csv"
    sample_dataframe.to_csv(csv_path, index=False)
    return str(csv_path)

def test_import_modules():
    """Test that core modules can be imported"""
    from src.config import DB_CONFIG
    from src.database import get_db_session
    from src.models import Table, Column
    from src.data_layer.scanner import DataScanner
    from src.profiling.profiler import DataProfiler
    assert DB_CONFIG is not None

def test_data_profiler_column_profile(sample_dataframe):
    """Test column profiling"""
    from src.profiling.profiler import DataProfiler
    profiler = DataProfiler()
    
    profile = profiler.profile_column(sample_dataframe['age'], 'age', 'INTEGER')
    
    assert profile['column_name'] == 'age'
    assert profile['unique_count'] == 5
    assert profile['null_count'] == 0
    assert 'mean_value' in profile

def test_data_profiler_dataframe_profile(sample_dataframe):
    """Test dataframe profiling"""
    from src.profiling.profiler import DataProfiler
    profiler = DataProfiler()
    
    profile = profiler.profile_dataframe(sample_dataframe, table_id=1, table_name='test_table')
    
    assert profile['table_id'] == 1
    assert profile['row_count'] == 5
    assert profile['column_count'] == 5
    assert len(profile['column_profiles']) == 5

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
