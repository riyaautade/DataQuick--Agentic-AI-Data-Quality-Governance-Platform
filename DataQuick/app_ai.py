"""DataQuick - AI-Powered Data Quality & Governance Dashboard"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
from pathlib import Path
from datetime import datetime

# Fix imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Page config
st.set_page_config(
    page_title="DataQuick AI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for beautiful styling
st.markdown("""
    <style>
    /* Sidebar Gradient */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 50%, #6C5B95 100%) !important;
    }
    
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] * {
        color: white !important;
    }
    
    [data-testid="stSidebar"] [role="radiogroup"] {
        background: rgba(255, 255, 255, 0.15) !important;
        padding: 10px !important;
        border-radius: 8px !important;
    }
    
    [data-testid="stSidebar"] [role="radio"] {
        color: white !important;
    }
    
    /* Main background */
    .appViewContainer {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%) !important;
    }
    
    /* Metrics */
    .stMetric {
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        padding: 20px !important;
        border-radius: 12px !important;
        border-left: 5px solid #667eea !important;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.08) !important;
    }
    
    /* Headers */
    h1 {
        color: #667eea !important;
        font-weight: 800 !important;
    }
    
    h2 {
        color: #667eea !important;
        font-weight: 700 !important;
        border-bottom: 3px solid #764ba2 !important;
        padding-bottom: 10px !important;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
    }
    </style>
    """, unsafe_allow_html=True)

# Sidebar Header with Logo
st.sidebar.markdown("""
    <div style="text-align: center; padding: 15px 0; margin-bottom: 10px;">
        <div style="font-size: 32px; margin-bottom: 5px;">🤖</div>
        <div style="font-size: 24px; font-weight: 800; color: white; margin: 0;">DataQuick</div>
        <div style="font-size: 12px; color: rgba(255, 255, 255, 0.85); margin: 5px 0;">Data Quality</div>
    </div>
""", unsafe_allow_html=True)

st.sidebar.divider()

# Navigation label
st.sidebar.markdown("<p style='color: rgba(255,255,255,0.9); font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; margin: 10px 0;'>Navigation</p>", unsafe_allow_html=True)

page = st.sidebar.radio("Select Page", [
    "📊 Dashboard",
    "📁 Data Ingestion", 
    "🤖 Ask Questions (AI)",
    "⚠️ Quality Issues",
    "🔧 Fix Suggestions (AI)",
    "🔗 Lineage Analysis",
    "⚙️ Settings"
], label_visibility="collapsed")

# Footer with credit
st.sidebar.divider()
st.sidebar.markdown("""
    <div style="text-align: center; padding: 10px 0; color: rgba(255, 255, 255, 0.8); font-size: 10px; margin-top: 20px;">
        <p style="margin: 3px 0;">~ Project by Riya Autade</p>
        <p style="margin: 3px 0; opacity: 0.7;">v1.0 | DataQuick</p>
    </div>
""", unsafe_allow_html=True)

try:
    from src.database import test_connection, get_db_session, init_db
    from src.models import Table, Profile, Issue, ColumnMetadata
    from src.data_layer.scanner import DataScanner
    from src.profiling.profiler import DataProfiler
    from src.profiling.drift_detector import DriftDetector
    from src.agents.orchestrator import AgentOrchestrator
    from src.rag.vector_store import VectorStore
    
    # Initialize database
    init_db()
    
    # Initialize agents (cached)
    @st.cache_resource
    def get_agents():
        return AgentOrchestrator()
    
    agents = get_agents()
    
    if page == "📊 Dashboard":
        st.title("📊 Dashboard")
        
        session = get_db_session()
        tables = session.query(Table).all()
        all_issues = session.query(Issue).all()
        profiles = session.query(Profile).all()
        
        # Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "📋 Tables",
                len(tables),
                delta=f"{len(tables)} registered",
                help="Total number of registered tables"
            )
        
        with col2:
            st.metric(
                "⚠️ Issues",
                len(all_issues),
                delta=f"{len(all_issues)} detected",
                help="Total quality issues found"
            )
        
        with col3:
            critical = session.query(Issue).filter(Issue.severity == "critical").count()
            st.metric(
                "🔴 Critical",
                critical,
                delta=f"{critical} issues",
                help="Critical severity issues"
            )
        
        with col4:
            st.metric(
                "📈 Profiles",
                len(profiles),
                delta=f"{len(profiles)} columns",
                help="Statistical profiles generated"
            )
        
        st.divider()
        
        # Charts Row
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Issue Severity Distribution")
            if all_issues:
                severity_counts = {}
                for issue in all_issues:
                    severity_counts[issue.severity] = severity_counts.get(issue.severity, 0) + 1
                
                fig = px.pie(
                    values=list(severity_counts.values()),
                    names=list(severity_counts.keys()),
                    color_discrete_map={
                        "critical": "#FF6B6B",
                        "high": "#FF8787",
                        "medium": "#FFD93D",
                        "low": "#6BCB77"
                    }
                )
                fig.update_layout(height=300, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No issues to display")
        
        with col2:
            st.subheader("Issues by Table")
            if tables:
                table_issues = {}
                for table in tables:
                    count = session.query(Issue).filter(Issue.table_id == table.id).count()
                    if count > 0:
                        table_issues[table.name] = count
                
                if table_issues:
                    fig = px.bar(
                        x=list(table_issues.keys()),
                        y=list(table_issues.values()),
                        labels={"x": "Table", "y": "Issues"},
                        color=list(table_issues.values()),
                        color_continuous_scale="Reds"
                    )
                    fig.update_layout(height=300, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No issues detected")
            else:
                st.info("No tables registered")
        
        st.divider()
        st.subheader("📋 Registered Tables")
        
        if tables:
            for table in tables:
                issues = session.query(Issue).filter(Issue.table_id == table.id).all()
                columns = session.query(ColumnMetadata).filter(ColumnMetadata.table_id == table.id).all()
                
                with st.expander(f"**{table.name}** • {len(columns)} columns • {len(issues)} issues", expanded=False):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.write(f"**Rows:** {table.source_path}")
                    with col2:
                        st.write(f"**Source Type:** {table.source_type}")
                    with col3:
                        st.write(f"**Loaded:** {table.created_at.strftime('%Y-%m-%d %H:%M')}")
                    
                    st.write("**Columns:**")
                    col_df = pd.DataFrame([
                        {
                            "Name": c.name,
                            "Type": c.data_type,
                            "Nullable": "✓" if c.nullable else "✗"
                        }
                        for c in columns
                    ])
                    st.dataframe(col_df, use_container_width=True, hide_index=True)
        else:
            st.info("No tables yet. Upload data in Data Ingestion.")
        
        session.close()
    
    elif page == "📁 Data Ingestion":
        st.title("📁 Upload & Auto-Profile with AI")
        st.write("Upload CSV/Excel → Auto-scan → Profile → Detect issues")
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            table_name = st.text_input(
                "📝 Table Name",
                placeholder="e.g., customers",
                help="Name for your dataset"
            )
        
        with col2:
            description = st.text_area(
                "📄 Description (optional)",
                placeholder="Brief description of this dataset",
                height=40
            )
        
        uploaded_file = st.file_uploader(
            "📤 Upload CSV or Excel",
            type=["csv", "xlsx"],
            help="Max 200MB per file"
        )
        
        if uploaded_file and table_name:
            if st.button("🚀 Scan & Profile with AI", use_container_width=True, type="primary"):
                with st.spinner("🔍 Scanning data..."):
                    try:
                        # Read file
                        if uploaded_file.name.endswith('.csv'):
                            df = pd.read_csv(uploaded_file)
                        else:
                            df = pd.read_excel(uploaded_file)
                        
                        # Use ScannerAgent
                        scanner_result = agents.scanner.scan_and_profile(
                            uploaded_file.name, 
                            table_name
                        )
                        
                        st.success("✅ Data scanned successfully!")
                        
                        # Stats Row
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("📊 Rows", f"{len(df):,}")
                        with col2:
                            st.metric("📋 Columns", len(df.columns))
                        with col3:
                            st.metric("💾 Size", f"{df.memory_usage().sum() / 1024:.1f} KB")
                        
                        # Register table
                        session = get_db_session()
                        table = Table(
                            name=table_name,
                            source_type="csv" if uploaded_file.name.endswith('.csv') else "excel",
                            source_path=uploaded_file.name,
                            description=description or None
                        )
                        session.add(table)
                        session.commit()
                        
                        # Register columns
                        for position, col_name in enumerate(df.columns):
                            col_obj = ColumnMetadata(
                                table_id=table.id,
                                name=col_name,
                                data_type=str(df[col_name].dtype),
                                nullable=df[col_name].isnull().any(),
                                position=position
                            )
                            session.add(col_obj)
                        session.commit()
                        
                        # Profile with DataProfiler
                        with st.spinner("📊 Profiling..."):
                            profiler = DataProfiler()
                            profile = profiler.profile_dataframe(df, table.id, table_name)
                            profiler.save_profile(table.id, df)
                        
                        # Detect comprehensive quality issues
                        with st.spinner("⚠️ Detecting quality issues..."):
                            from src.profiling.data_quality_analyzer import DataQualityAnalyzer
                            
                            analyzer = DataQualityAnalyzer()
                            issues_detected = analyzer.analyze_dataframe(df, table.id, table_name)
                            analyzer.save_issues_to_db(issues_detected)
                        
                        session.commit()
                        session.close()
                        
                        st.success("✅ Profile complete! Issues detected.")
                        
                        # Data Preview
                        st.subheader("📊 Data Preview")
                        st.dataframe(df.head(10), use_container_width=True)
                        
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        import traceback
                        st.write(traceback.format_exc())
        else:
            st.info("👈 Enter table name and upload a file to scan with AI")
    
    elif page == "🤖 Ask Questions (AI)":
        st.title("🤖 Ask Questions (AI-Powered)")
        st.write("Ask natural language questions about your data")
        st.divider()
        
        session = get_db_session()
        tables = session.query(Table).all()
        
        if not tables:
            st.info("📥 No tables yet. Upload data first in Data Ingestion.")
        else:
            # Table selection
            col1, col2 = st.columns(2)
            
            with col1:
                selected_table_name = st.selectbox(
                    "📁 Select Table (optional)",
                    ["All Tables"] + [t.name for t in tables],
                    help="Choose a specific table or ask about all tables"
                )
            
            with col2:
                st.write("")
                st.write("")
                if selected_table_name == "All Tables":
                    st.caption("📊 Will search across all tables")
                else:
                    st.caption(f"🎯 Focused on: {selected_table_name}")
            
            st.divider()
            
            # Full width question box
            question = st.text_area(
                "📝 Ask a question about your data",
                placeholder="e.g., How many quality issues in this table? What columns exist? Show me missing values. What's the data distribution?",
                height=100,
                label_visibility="visible"
            )
            
            # Full width button
            if st.button("🔍 Get AI Answer", use_container_width=True, type="primary"):
                if question:
                    with st.spinner("🤖 AI analyzing your data..."):
                        try:
                            # Pass table context to agent
                            if selected_table_name != "All Tables":
                                result = agents.qa.answer_question(question, selected_table_name)
                            else:
                                result = agents.qa.answer_question(question, None)
                            
                            st.divider()
                            st.subheader("💡 Answer")
                            st.info(result.get("answer", "No answer available"))
                            
                            if result.get("context_count", 0) > 0:
                                st.caption(f"✓ Used {result['context_count']} context sources")
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                else:
                    st.warning("Please type a question first!")
        
        session.close()
    
    elif page == "⚠️ Quality Issues":
        st.title("⚠️ Data Quality Issues")
        
        session = get_db_session()
        tables = session.query(Table).all()
        all_issues = session.query(Issue).all()
        
        if not tables:
            st.info("📥 No tables yet. Upload data first in Data Ingestion.")
        else:
            # Table selection
            selected_table_name = st.selectbox(
                "📁 Select Table",
                [t.name for t in tables],
                help="Filter issues by table"
            )
            
            selected_table = next((t for t in tables if t.name == selected_table_name), None)
            
            if selected_table:
                # Get issues for this table
                table_issues = [i for i in all_issues if i.table_id == selected_table.id]
                
                # Null value analysis for this table
                st.subheader("🔴 Null Value Analysis")
                null_issues = [i for i in table_issues if i.issue_type == "missing_values"]
                
                if null_issues:
                    null_data = []
                    for issue in null_issues:
                        col = session.query(ColumnMetadata).filter_by(id=issue.column_id).first() if issue.column_id else None
                        col_name = col.name if col else "unknown"
                        # Extract percentage from description
                        null_percent = float(''.join(filter(lambda x: x.isdigit() or x == '.', issue.description.split('(')[1].split('%')[0]))) if '(' in issue.description else 0
                        null_data.append({"Column": col_name, "Null %": null_percent})
                    
                    if null_data:
                        null_df = pd.DataFrame(null_data)
                        fig = px.bar(
                            null_df,
                            x="Column",
                            y="Null %",
                            color="Null %",
                            color_continuous_scale="Reds",
                            labels={"Null %": "Percentage (%)"}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("✅ No null values detected!")
                
                st.divider()
                
                if table_issues:
                    # Severity breakdown for this table
                    critical = [i for i in table_issues if i.severity == "critical"]
                    high = [i for i in table_issues if i.severity == "high"]
                    medium = [i for i in table_issues if i.severity == "medium"]
                    low = [i for i in table_issues if i.severity == "low"]
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("🔴 Critical", len(critical), help="Critical issues")
                    with col2:
                        st.metric("🟠 High", len(high), help="High severity issues")
                    with col3:
                        st.metric("🟡 Medium", len(medium), help="Medium severity issues")
                    with col4:
                        st.metric("🟢 Low", len(low), help="Low severity issues")
                    
                    st.divider()
                    
                    # Issues by type chart for this table
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Issues by Type")
                        issue_types = {}
                        for issue in table_issues:
                            issue_types[issue.issue_type] = issue_types.get(issue.issue_type, 0) + 1
                        
                        fig = px.bar(
                            x=list(issue_types.keys()),
                            y=list(issue_types.values()),
                            color=list(issue_types.values()),
                            color_continuous_scale="Reds",
                            labels={"x": "Issue Type", "y": "Count"}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        st.subheader("Issues by Severity")
                        severity_data = {
                            "Critical": len(critical),
                            "High": len(high),
                            "Medium": len(medium),
                            "Low": len(low)
                        }
                        fig = px.pie(
                            values=list(severity_data.values()),
                            names=list(severity_data.keys()),
                            color_discrete_map={
                                "Critical": "#FF6B6B",
                                "High": "#FF8787",
                                "Medium": "#FFD93D",
                                "Low": "#6BCB77"
                            }
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    st.divider()
                    st.write("### Issue Details")
                    
                    for severity_level, severity_issues, color in [
                        ("🔴 CRITICAL", critical, "🔴"),
                        ("🟠 HIGH", high, "🟠"),
                        ("🟡 MEDIUM", medium, "🟡"),
                        ("🟢 LOW", low, "🟢")
                    ]:
                        if severity_issues:
                            st.subheader(severity_level)
                            for issue in severity_issues:
                                col_name = ""
                                if issue.column_id:
                                    col = session.query(ColumnMetadata).filter_by(id=issue.column_id).first()
                                    col_name = f" - {col.name}" if col else ""
                                
                                with st.expander(f"{issue.issue_type}{col_name}"):
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        st.write(f"**Table:** {issue.table.name}")
                                    with col2:
                                        st.write(f"**Severity:** {issue.severity}")
                                    with col3:
                                        st.write(f"**Type:** {issue.issue_type}")
                                    
                                    st.write(f"**Description:** {issue.description}")
                                    
                                    if issue.suggested_fix:
                                        st.code(issue.suggested_fix, language="sql")
                                        if st.button("🔧 Get AI Fix", key=f"fix_{issue.id}"):
                                            with st.spinner("AI generating fix..."):
                                                try:
                                                    fix = agents.fixer.suggest_fix(
                                                        issue.description,
                                                        col_name or "unknown",
                                                        issue.table.name,
                                                        issue.issue_type
                                                    )
                                                    st.success("✅ AI-Suggested Fix:")
                                                    st.code(fix, language="sql")
                                                except Exception as e:
                                                    st.error(f"Error: {str(e)}")
                else:
                    st.success(f"✅ No quality issues detected in {selected_table_name}!")
        
        session.close()
    
    elif page == "🔧 Fix Suggestions (AI)":
        st.title("🔧 AI-Powered Fix Suggestions")
        st.write("Get AI-suggested SQL fixes for data quality issues")
        st.divider()
        
        session = get_db_session()
        tables = session.query(Table).all()
        all_issues = session.query(Issue).all()
        
        # Store issue details in session before closing
        issue_details = {}
        for issue in all_issues:
            col_name = ""
            if issue.column_id:
                col = session.query(ColumnMetadata).filter_by(id=issue.column_id).first()
                col_name = col.name if col else "unknown"
            
            issue_details[issue.id] = {
                "id": issue.id,
                "issue_type": issue.issue_type,
                "description": issue.description,
                "severity": issue.severity,
                "table_id": issue.table_id,
                "table_name": issue.table.name,
                "column_name": col_name
            }
        
        if not tables:
            st.info("📥 No tables yet. Upload data first in Data Ingestion.")
        else:
            # Table selection
            selected_table_name = st.selectbox(
                "📁 Select Table",
                [t.name for t in tables],
                help="Filter issues by table",
                key="fix_table_select"
            )
            
            selected_table = next((t for t in tables if t.name == selected_table_name), None)
            
            if selected_table:
                # Get issues for this table
                table_issues = [i for i in all_issues if i.table_id == selected_table.id]
                
                if table_issues:
                    # Issue selection for this table
                    selected_issue = st.selectbox(
                        "⚠️ Select Issue",
                        table_issues,
                        format_func=lambda x: f"🔴 {x.issue_type} ({x.severity.upper()})",
                        help="Select an issue to get AI fix suggestions"
                    )
                    
                    if selected_issue:
                        issue_detail = issue_details[selected_issue.id]
                        
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            st.write(f"**Issue Type:** {issue_detail['issue_type']}")
                            st.write(f"**Description:** {issue_detail['description']}")
                            st.write(f"**Table:** {issue_detail['table_name']}")
                        
                        with col2:
                            severity_colors = {
                                "critical": "🔴",
                                "high": "🟠",
                                "medium": "🟡",
                                "low": "🟢"
                            }
                            st.write(f"**Severity:** {severity_colors.get(issue_detail['severity'], '❓')} {issue_detail['severity'].upper()}")
                        
                        st.divider()
                        
                        if st.button("🤖 Generate AI Fix", use_container_width=True, type="primary"):
                            with st.spinner("AI generating fix..."):
                                try:
                                    fix = agents.fixer.suggest_fix(
                                        issue_detail['description'],
                                        issue_detail['column_name'],
                                        issue_detail['table_name'],
                                        issue_detail['issue_type']
                                    )
                                    
                                    st.success("✅ Fix suggestion generated:")
                                    st.code(fix, language="sql")
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                else:
                    st.success(f"✅ No quality issues detected in {selected_table_name}!")
        
        session.close()
    
    elif page == "🔗 Lineage Analysis":
        st.title("🔗 Data Lineage & Dependencies")
        st.write("Understand how data flows between tables")
        st.divider()
        
        session = get_db_session()
        tables = session.query(Table).all()
        
        if tables:
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.subheader("📋 Tables")
                for table in tables:
                    columns = session.query(ColumnMetadata).filter(ColumnMetadata.table_id == table.id).all()
                    st.write(f"**{table.name}** • {len(columns)} columns")
            
            with col2:
                st.subheader("📊 Column Details")
                selected_table = st.selectbox(
                    "Select table",
                    [t.name for t in tables],
                    key="lineage_table"
                )
                
                table = next(t for t in tables if t.name == selected_table)
                columns = session.query(ColumnMetadata).filter(ColumnMetadata.table_id == table.id).all()
                
                col_df = pd.DataFrame([
                    {
                        "Position": c.position + 1,
                        "Name": c.name,
                        "Type": c.data_type,
                        "Nullable": "✓" if c.nullable else "✗"
                    }
                    for c in columns
                ])
                st.dataframe(col_df, use_container_width=True, hide_index=True)
        else:
            st.info("📥 No tables registered yet.")
        
        session.close()
    
    elif page == "⚙️ Settings":
        st.title("⚙️ System Configuration")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("🗄️ Database")
            if test_connection():
                st.success("✅ SQLite Connected")
                st.caption("`dataquick.db`")
            else:
                st.error("❌ Connection Error")
        
        with col2:
            st.subheader("📊 Statistics")
            session = get_db_session()
            st.write(f"**Tables:** {session.query(Table).count()}")
            st.write(f"**Issues:** {session.query(Issue).count()}")
            st.write(f"**Profiles:** {session.query(Profile).count()}")
            session.close()
        
        with col3:
            st.subheader("🔧 System Info")
            st.write(f"**Version:** 1.0.0")
            st.write(f"**Environment:** Production")
            st.write(f"**Last Updated:** Nov 22, 2025")

except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    import traceback
    st.write(traceback.format_exc())
