"""Streamlit UI Dashboard - Minimal Version"""
import streamlit as st
import sys
import os

st.set_page_config(
    page_title="DataQuick",
    page_icon="📊",
    layout="wide"
)

st.title("🚀 DataQuick - Data Quality & Governance")

try:
    st.write("### System Status")
    st.success("✓ DataQuick Started")
    
    # Try to import database
    try:
        from src.database import test_connection
        st.write("✓ Database module imported")
        
        if test_connection():
            st.success("✓ SQLite database connected")
        else:
            st.error("✗ Database connection failed")
    except Exception as e:
        st.error(f"Database error: {e}")
    
    st.write("---")
    st.write("### Ready to Use")
    st.info("Upload CSV files to get started!")
    
except Exception as e:
    st.error(f"Critical error: {str(e)}")
    import traceback
    st.code(traceback.format_exc())
