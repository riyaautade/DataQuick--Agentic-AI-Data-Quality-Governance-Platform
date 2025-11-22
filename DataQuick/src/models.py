"""Database models for catalog and metadata"""
from sqlalchemy import Column as SQLColumn, Integer, String, Float, DateTime, Boolean, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Table(Base):
    __tablename__ = "tables"
    
    id = SQLColumn(Integer, primary_key=True)
    name = SQLColumn(String(255), unique=True, nullable=False)
    schema_name = SQLColumn(String(255))
    source_type = SQLColumn(String(50))  # csv, excel, postgres, duckdb
    source_path = SQLColumn(Text)
    created_at = SQLColumn(DateTime, default=datetime.utcnow)
    updated_at = SQLColumn(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    description = SQLColumn(Text)
    
    columns = relationship("ColumnMetadata", back_populates="table", cascade="all, delete-orphan")
    profiles = relationship("Profile", back_populates="table", cascade="all, delete-orphan")
    issues = relationship("Issue", back_populates="table", cascade="all, delete-orphan")
    schema_changes = relationship("SchemaChange", back_populates="table", cascade="all, delete-orphan")

class ColumnMetadata(Base):
    __tablename__ = "columns"
    
    id = SQLColumn(Integer, primary_key=True)
    table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    name = SQLColumn(String(255), nullable=False)
    data_type = SQLColumn(String(100))
    nullable = SQLColumn(Boolean, default=True)
    position = SQLColumn(Integer)
    description = SQLColumn(Text)
    created_at = SQLColumn(DateTime, default=datetime.utcnow)
    
    table = relationship("Table", back_populates="columns")
    profiles = relationship("Profile", back_populates="column")
    issues = relationship("Issue", back_populates="column")

class Profile(Base):
    __tablename__ = "profiles"
    
    id = SQLColumn(Integer, primary_key=True)
    table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    column_id = SQLColumn(Integer, ForeignKey("columns.id"))
    profile_timestamp = SQLColumn(DateTime, default=datetime.utcnow)
    row_count = SQLColumn(Integer)
    null_count = SQLColumn(Integer)
    null_percentage = SQLColumn(Float)
    unique_count = SQLColumn(Integer)
    unique_percentage = SQLColumn(Float)
    min_value = SQLColumn(Text)
    max_value = SQLColumn(Text)
    mean_value = SQLColumn(Float)
    median_value = SQLColumn(Float)
    std_dev = SQLColumn(Float)
    sample_values = SQLColumn(JSON)  # Store as JSON instead of ARRAY for SQLite
    histogram_bins = SQLColumn(JSON)
    data_type = SQLColumn(String(50))
    profile_data = SQLColumn(JSON)
    
    table = relationship("Table", back_populates="profiles")
    column = relationship("ColumnMetadata", back_populates="profiles")

class Issue(Base):
    __tablename__ = "issues"
    
    id = SQLColumn(Integer, primary_key=True)
    table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    column_id = SQLColumn(Integer, ForeignKey("columns.id"))
    issue_type = SQLColumn(String(100))  # null_violation, outlier, schema_drift, etc.
    severity = SQLColumn(String(20))  # low, medium, high, critical
    description = SQLColumn(Text)
    detected_at = SQLColumn(DateTime, default=datetime.utcnow)
    resolved_at = SQLColumn(DateTime)
    suggested_fix = SQLColumn(Text)
    affected_row_count = SQLColumn(Integer)
    
    table = relationship("Table", back_populates="issues")
    column = relationship("ColumnMetadata", back_populates="issues")

class SchemaChange(Base):
    __tablename__ = "schema_changes"
    
    id = SQLColumn(Integer, primary_key=True)
    table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    change_type = SQLColumn(String(50))  # column_added, column_removed, type_changed
    change_details = SQLColumn(JSON)
    detected_at = SQLColumn(DateTime, default=datetime.utcnow)
    detected_by = SQLColumn(String(100))
    
    table = relationship("Table", back_populates="schema_changes")

class LineageEdge(Base):
    __tablename__ = "lineage_edges"
    
    id = SQLColumn(Integer, primary_key=True)
    source_column_id = SQLColumn(Integer, ForeignKey("columns.id"), nullable=False)
    target_column_id = SQLColumn(Integer, ForeignKey("columns.id"), nullable=False)
    lineage_type = SQLColumn(String(50))  # direct, derived, aggregated
    transformation_logic = SQLColumn(Text)
    created_at = SQLColumn(DateTime, default=datetime.utcnow)

class LineageRun(Base):
    __tablename__ = "lineage_runs"
    
    id = SQLColumn(Integer, primary_key=True)
    source_table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    target_table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    run_timestamp = SQLColumn(DateTime, default=datetime.utcnow)
    row_count_source = SQLColumn(Integer)
    row_count_target = SQLColumn(Integer)
    status = SQLColumn(String(50))  # success, failed, partial
