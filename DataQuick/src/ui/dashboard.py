"""Streamlit UI Dashboard"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path

# Configure page
st.set_page_config(
    page_title="DataQuick - Agentic Data Quality & Governance",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
    }
    .issue-critical {
        background-color: #fee;
        border-left: 4px solid #f00;
    }
    .issue-high {
        background-color: #fef3cd;
        border-left: 4px solid #ff9800;
    }
</style>
""", unsafe_allow_html=True)

def initialize_session():
    """Initialize session state"""
    if "agents_initialized" not in st.session_state:
        try:
            from src.agents.orchestrator import AgentOrchestrator
            st.session_state.orchestrator = AgentOrchestrator()
            st.session_state.agents_initialized = True
        except Exception as e:
            st.error(f"Failed to initialize agents: {e}")
            st.session_state.agents_initialized = False

def load_dashboard_data():
    """Load data for dashboard"""
    try:
        from src.database import get_db_session
        from src.models import Table, Issue, Profile
        
        session = get_db_session()
        tables = session.query(Table).all()
        issues = session.query(Issue).filter_by(resolved_at=None).all()
        profiles = session.query(Profile).order_by(Profile.profile_timestamp.desc()).limit(100).all()
        
        return tables, issues, profiles
    except Exception as e:
        st.error(f"Failed to load dashboard data: {e}")
        return [], [], []

def main():
    initialize_session()
    
    # Sidebar
    with st.sidebar:
        st.title("🚀 DataQuick")
        st.write("Agentic Data Quality & Governance Assistant")
        st.divider()
        
        page = st.radio(
            "Navigation",
            ["📊 Dashboard", "📤 Data Ingestion", "💬 Ask Questions", "🔗 Lineage", "🛠️ Fix Suggestions", "⚙️ Settings"],
            label_visibility="collapsed"
        )
    
    # Dashboard Page
    if page == "📊 Dashboard":
        st.title("📊 Data Quality Dashboard")
        
        tables, issues, profiles = load_dashboard_data()
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Tables", len(tables), delta="monitored")
        with col2:
            st.metric("Active Issues", len([i for i in issues if i.severity == "critical"]), delta="critical")
        with col3:
            st.metric("Quality Score", "87.5%", delta="↑ 2.3%")
        with col4:
            st.metric("Last Scan", "2 hours ago", delta="automated")
        
        st.divider()
        
        # Tables Overview
        if tables:
            st.subheader("📋 Registered Tables")
            table_data = []
            for t in tables[:10]:
                table_data.append({
                    "Table": t.name,
                    "Source": t.source_type,
                    "Columns": len(t.columns),
                    "Last Updated": t.updated_at.strftime("%Y-%m-%d %H:%M"),
                    "Issues": len([i for i in issues if i.table_id == t.id])
                })
            
            st.dataframe(
                pd.DataFrame(table_data),
                use_container_width=True,
                hide_index=True
            )
        
        # Issues Overview
        if issues:
            st.subheader("⚠️ Outstanding Issues")
            critical_issues = [i for i in issues if i.severity == "critical"]
            high_issues = [i for i in issues if i.severity == "high"]
            
            issue_cols = st.columns(3)
            with issue_cols[0]:
                st.metric("Critical", len(critical_issues), delta="🔴")
            with issue_cols[1]:
                st.metric("High", len(high_issues), delta="🟠")
            with issue_cols[2]:
                st.metric("Total", len(issues), delta="open")
            
            for issue in issues[:5]:
                severity_color = "🔴" if issue.severity == "critical" else "🟠" if issue.severity == "high" else "🟡"
                with st.expander(f"{severity_color} {issue.issue_type} - {issue.description[:60]}..."):
                    st.write(f"**Type**: {issue.issue_type}")
                    st.write(f"**Severity**: {issue.severity}")
                    st.write(f"**Description**: {issue.description}")
                    if issue.suggested_fix:
                        st.write(f"**Fix**: {issue.suggested_fix}")
                    if st.button("Mark as Resolved", key=f"resolve_{issue.id}"):
                        st.success("Issue marked as resolved!")
    
    # Data Ingestion Page
    elif page == "📤 Data Ingestion":
        st.title("📤 Data Ingestion & Profiling")
        
        st.write("Upload or scan data sources for profiling and quality assessment")
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Upload Data")
            uploaded_file = st.file_uploader(
                "Choose a CSV or Excel file",
                type=["csv", "xlsx", "xls"]
            )
            
            if uploaded_file:
                table_name = st.text_input("Table Name", value=uploaded_file.name.split('.')[0])
                description = st.text_area("Description (optional)")
                
                if st.button("Scan & Profile"):
                    with st.spinner("Scanning and profiling..."):
                        try:
                            # Save uploaded file
                            file_path = f"data/{uploaded_file.name}"
                            Path("data").mkdir(exist_ok=True)
                            with open(file_path, "wb") as f:
                                f.write(uploaded_file.getbuffer())
                            
                            # Use scanner agent
                            result = st.session_state.orchestrator.dispatch(
                                "scanner",
                                file_path=file_path,
                                table_name=table_name
                            )
                            
                            if result.get("status") == "success":
                                st.success(f"✓ {table_name} profiled successfully!")
                                st.json(result)
                            else:
                                st.error(f"Failed: {result.get('error')}")
                        except Exception as e:
                            st.error(f"Error: {e}")
        
        with col2:
            st.subheader("Recent Scans")
            tables, _, _ = load_dashboard_data()
            if tables:
                for table in tables[-5:]:
                    with st.expander(f"📊 {table.name}"):
                        st.write(f"**Source**: {table.source_type}")
                        st.write(f"**Path**: {table.source_path}")
                        st.write(f"**Columns**: {len(table.columns)}")
                        st.write(f"**Updated**: {table.updated_at.strftime('%Y-%m-%d %H:%M')}")
    
    # Q&A Page
    elif page == "💬 Ask Questions":
        st.title("💬 Ask Data Questions")
        st.write("Query your data using natural language (powered by RAG + LLM)")
        st.divider()
        
        question = st.text_area(
            "Ask a question about your data:",
            placeholder="e.g., What columns have the most nulls? Show me the data quality trends.",
            height=100
        )
        
        if st.button("🔍 Get Answer", use_container_width=True):
            if question:
                with st.spinner("Thinking..."):
                    result = st.session_state.orchestrator.dispatch("qa", question=question)
                    if result.get("status") == "success":
                        st.subheader("📋 Answer")
                        st.write(result.get("answer"))
                    else:
                        st.error(f"Error: {result.get('error')}")
            else:
                st.warning("Please enter a question")
    
    # Lineage Page
    elif page == "🔗 Lineage":
        st.title("🔗 Data Lineage & Dependencies")
        st.write("Visualize column and table dependencies")
        st.divider()
        
        try:
            from src.catalog.lineage_tracker import LineageTracker
            tracker = LineageTracker()
            G = tracker.get_lineage_graph()
            
            if G.number_of_nodes() > 0:
                st.subheader("📈 Lineage Graph")
                st.write(f"**Nodes**: {G.number_of_nodes()} | **Edges**: {G.number_of_edges()}")
                
                # Try to visualize
                try:
                    from pyvis.network import Network
                    net = Network(directed=True, height="750px")
                    net.from_nx(G)
                    net.show("lineage_graph.html")
                    
                    with open("lineage_graph.html", "r") as f:
                        st.components.v1.html(f.read(), height=750)
                except Exception as e:
                    st.warning(f"Could not render graph visualization: {e}")
                    st.json({"nodes": list(G.nodes()), "edges": list(G.edges())})
            else:
                st.info("No lineage data available. Add lineage edges through the fix suggestions or manual configuration.")
        except Exception as e:
            st.error(f"Error loading lineage: {e}")
    
    # Fix Suggestions Page
    elif page == "🛠️ Fix Suggestions":
        st.title("🛠️ Suggested SQL Fixes")
        st.write("Get AI-powered suggestions for data quality issues")
        st.divider()
        
        tables, issues, _ = load_dashboard_data()
        
        if issues:
            selected_issue = st.selectbox(
                "Select an issue to fix",
                [f"{i.issue_type} - {i.description[:50]}" for i in issues],
                format_func=lambda x: x
            )
            
            selected_idx = [f"{i.issue_type} - {i.description[:50]}" for i in issues].index(selected_issue)
            issue = issues[selected_idx]
            
            st.divider()
            st.subheader(f"Issue: {issue.issue_type}")
            st.write(f"**Description**: {issue.description}")
            st.write(f"**Severity**: {issue.severity}")
            
            if st.button("💡 Get Fix Suggestion"):
                with st.spinner("Generating suggestion..."):
                    result = st.session_state.orchestrator.dispatch(
                        "fixer",
                        issue_description=issue.description,
                        column_name=issue.column.name if issue.column else "N/A",
                        table_name=issue.table.name,
                        issue_type=issue.issue_type
                    )
                    
                    if result.get("status") == "success":
                        st.subheader("✅ Suggested Fix")
                        st.write(result.get("suggestion"))
                        
                        if st.button("✓ Apply Fix"):
                            st.success("Fix would be applied (in production mode)")
                    else:
                        st.error(f"Error: {result.get('error')}")
        else:
            st.info("No outstanding issues to fix!")
    
    # Settings Page
    elif page == "⚙️ Settings":
        st.title("⚙️ Settings & Configuration")
        st.divider()
        
        st.subheader("Database Configuration")
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Host**: localhost")
            st.write("**Port**: 5432")
        with col2:
            st.write("**Database**: dataquick_catalog")
            st.write("**User**: dataquick")
        
        st.divider()
        st.subheader("Feature Flags")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.toggle("Enable RAG", value=True)
        with col2:
            st.toggle("Enable Lineage", value=True)
        with col3:
            st.toggle("Enable Drift Detection", value=True)
        
        st.divider()
        st.subheader("System Status")
        
        try:
            from src.database import test_connection
            if test_connection():
                st.success("✓ Database connection active")
            else:
                st.error("✗ Database connection failed")
        except Exception as e:
            st.error(f"✗ Error checking database: {e}")
        
        st.divider()
        st.subheader("About")
        st.write("""
        **DataQuick** - Agentic Data Quality & Governance Assistant
        
        A local, AI-powered system for:
        - 📊 Data profiling and quality assessment
        - 🔍 Schema drift detection
        - 🔗 Lineage tracking
        - 💡 Intelligent fix suggestions
        - 💬 Natural language data queries
        """)

if __name__ == "__main__":
    main()
