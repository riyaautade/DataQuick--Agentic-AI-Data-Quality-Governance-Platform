"""LangChain Agent orchestration"""
from typing import Optional, Dict, Any, List
from loguru import logger
from langchain_core.language_models import BaseLanguageModel
from src.config import LLM_CONFIG

def get_llm() -> Optional[BaseLanguageModel]:
    """Initialize LLM based on configuration"""
    try:
        if LLM_CONFIG["type"] == "ollama":
            try:
                from langchain_community.llms import Ollama
                llm = Ollama(
                    model=LLM_CONFIG["model"],
                    base_url=LLM_CONFIG["base_url"]
                )
                logger.info(f"✓ Initialized Ollama LLM: {LLM_CONFIG['model']}")
                return llm
            except ImportError:
                logger.warning("Ollama not available, attempting fallback...")
        
        logger.warning("LLM initialization skipped - will use mock for now")
        return None
    except Exception as e:
        logger.error(f"✗ Failed to initialize LLM: {e}")
        return None

class MockLLM:
    """Mock LLM for development without external dependencies"""
    def __init__(self):
        self.model = "mock-llm"
    
    def invoke(self, prompt: str) -> str:
        """Mock invoke method"""
        logger.debug(f"Mock LLM invoked with prompt: {prompt[:100]}...")
        return "This is a mock response. Please set up a real LLM (Ollama, GPT4All, etc.)"
    
    def __call__(self, prompt: str) -> str:
        return self.invoke(prompt)

class ScannerAgent:
    """Agent for scanning and profiling data sources"""
    
    def __init__(self):
        self.llm = get_llm() or MockLLM()
        logger.info("✓ Scanner Agent initialized")
    
    def scan_and_profile(self, file_path: str, table_name: str) -> Dict[str, Any]:
        """Orchestrate scanning and profiling"""
        from src.data_layer.scanner import DataScanner
        from src.profiling.profiler import DataProfiler
        
        try:
            scanner = DataScanner()
            profiler = DataProfiler()
            
            # Scan file
            if file_path.endswith('.csv'):
                df = scanner.scan_csv(file_path, table_name)
            elif file_path.endswith('.xlsx'):
                df = scanner.scan_excel(file_path)
            else:
                return {"error": f"Unsupported file format: {file_path}"}
            
            # Register table
            table = scanner.register_table(table_name, df, "csv", file_path)
            
            # Profile data
            profile = profiler.profile_dataframe(df, table.id, table_name)
            profiler.save_profile(table.id, df, table)
            
            return {
                "status": "success",
                "table_id": table.id,
                "table_name": table_name,
                "rows": len(df),
                "columns": len(df.columns),
                "profile": profile
            }
        except Exception as e:
            logger.error(f"✗ Scanner Agent failed: {e}")
            return {"error": str(e)}

class QAAgent:
    """Agent for answering business and data questions via RAG"""
    
    def __init__(self):
        self.llm = get_llm() or MockLLM()
        try:
            from src.rag.vector_store import VectorStore
            self.vector_store = VectorStore()
        except Exception as e:
            logger.warning(f"RAG not available: {e}")
            self.vector_store = None
        logger.info("✓ QA Agent initialized")
    
    def answer_question(self, question: str, table_name: str = None) -> Dict[str, Any]:
        """Answer a question using database context + LLM for natural language generation
        
        Args:
            question: The user's question
            table_name: Optional table name to filter context to a specific table
        """
        try:
            from src.database import get_db_session
            from src.models import Table, ColumnMetadata, Profile, Issue
            
            session = get_db_session()
            
            # Build context for specific table or all tables
            if table_name:
                # Filter to specific table
                selected_table = session.query(Table).filter_by(name=table_name).first()
                if selected_table:
                    columns = session.query(ColumnMetadata).filter_by(table_id=selected_table.id).all()
                    issues = session.query(Issue).filter_by(table_id=selected_table.id).all()
                    profiles = session.query(Profile).filter_by(table_id=selected_table.id).all()
                    
                    context_data = f"Selected Table: {table_name}\n"
                    context_data += f"Columns ({len(columns)}): {', '.join([c.name for c in columns])}\n"
                    
                    if profiles:
                        rows = profiles[0].row_count if profiles[0].row_count else "Unknown"
                        context_data += f"Rows: {rows}\n"
                    
                    if issues:
                        context_data += f"\nQuality Issues ({len(issues)}):\n"
                        issue_summary = {}
                        for issue in issues:
                            issue_summary[issue.issue_type] = issue_summary.get(issue.issue_type, 0) + 1
                        for issue_type, count in sorted(issue_summary.items()):
                            context_data += f"  • {issue_type}: {count}\n"
                    else:
                        context_data += "Quality Issues: None - data looks good!\n"
                else:
                    session.close()
                    return {"answer": f"Table '{table_name}' not found.", "error": f"Table not found"}
            else:
                # All tables context
                tables = session.query(Table).all()
                all_issues = session.query(Issue).all()
                all_profiles = session.query(Profile).all()
                
                context_data = f"Database contains {len(tables)} table(s):\n"
                
                for table in tables:
                    cols = session.query(ColumnMetadata).filter_by(table_id=table.id).all()
                    table_issues = [i for i in all_issues if i.table_id == table.id]
                    table_profiles = [p for p in all_profiles if p.table_id == table.id]
                    
                    context_data += f"\nTable: {table.name}\n"
                    context_data += f"  Columns ({len(cols)}): {', '.join([c.name for c in cols])}\n"
                    
                    if table_profiles:
                        rows = table_profiles[0].row_count if table_profiles[0].row_count else "Unknown"
                        context_data += f"  Rows: {rows}\n"
                    
                    if table_issues:
                        issue_summary = {}
                        for issue in table_issues:
                            issue_summary[issue.issue_type] = issue_summary.get(issue.issue_type, 0) + 1
                        context_data += f"  Issues: {issue_summary}\n"
            
            session.close()
            
            # Create a prompt that uses the LLM to understand and answer naturally
            prompt = f"""You are a data quality expert. Answer the following question about the database.

Database Context:
{context_data}

User Question: {question}

Provide a clear, specific answer based on the database context above. Be concise and helpful."""
            
            # Use LLM to generate natural language answer
            try:
                if hasattr(self.llm, 'invoke'):
                    llm_answer = self.llm.invoke(prompt)
                else:
                    llm_answer = self.llm(prompt)
                
                # If LLM returns a mock response, fall back to rule-based
                if "mock response" in llm_answer.lower():
                    llm_answer = self._rule_based_answer(question, context_data, table_name)
            except Exception as e:
                logger.debug(f"LLM failed, using rule-based answer: {e}")
                llm_answer = self._rule_based_answer(question, context_data, table_name)
            
            return {
                "status": "success",
                "question": question,
                "answer": llm_answer,
                "context_count": 1 if llm_answer else 0
            }
        except Exception as e:
            logger.error(f"✗ QA Agent failed: {e}")
            return {"answer": f"Error processing question: {str(e)}", "error": str(e)}
    
    def _rule_based_answer(self, question: str, context_data: str, table_name: str = None) -> str:
        """Fallback rule-based answer when LLM is unavailable"""
        from src.database import get_db_session
        from src.models import Table, ColumnMetadata, Issue
        
        session = get_db_session()
        question_lower = question.lower()
        answer = ""
        
        try:
            # Prioritize by keyword - check for quality/issue first
            if "issue" in question_lower or "quality" in question_lower or "problem" in question_lower:
                if table_name:
                    selected_table = session.query(Table).filter_by(name=table_name).first()
                    if selected_table:
                        issues = session.query(Issue).filter_by(table_id=selected_table.id).all()
                        if issues:
                            answer = f"Quality issues in {table_name} ({len(issues)} total):\n"
                            issue_summary = {}
                            for issue in issues:
                                issue_summary[issue.issue_type] = issue_summary.get(issue.issue_type, 0) + 1
                            for issue_type, count in sorted(issue_summary.items()):
                                answer += f"• {issue_type}: {count}\n"
                        else:
                            answer = f"No quality issues found in {table_name}. Data looks good!"
                else:
                    all_issues = session.query(Issue).all()
                    if all_issues:
                        answer = f"Total quality issues across all tables: {len(all_issues)}\n"
                        issue_summary = {}
                        for issue in all_issues:
                            table = session.query(Table).filter_by(id=issue.table_id).first()
                            key = f"{table.name} - {issue.issue_type}"
                            issue_summary[key] = issue_summary.get(key, 0) + 1
                        for key, count in sorted(issue_summary.items())[:15]:
                            answer += f"• {key}: {count}\n"
                    else:
                        answer = "No quality issues detected. Your data looks great!"
            
            elif "column" in question_lower:
                if table_name:
                    selected_table = session.query(Table).filter_by(name=table_name).first()
                    if selected_table:
                        cols = session.query(ColumnMetadata).filter_by(table_id=selected_table.id).all()
                        col_names = [c.name for c in cols]
                        answer = f"Columns in {table_name}: {', '.join(col_names)}"
                else:
                    tables = session.query(Table).all()
                    if tables:
                        answer = "Columns in your tables:\n"
                        for table in tables:
                            cols = session.query(ColumnMetadata).filter_by(table_id=table.id).all()
                            col_names = [c.name for c in cols]
                            answer += f"• {table.name}: {', '.join(col_names)}\n"
                    else:
                        answer = "No tables found yet."
            
            elif "table" in question_lower:
                tables = session.query(Table).all()
                if tables:
                    answer = f"You have {len(tables)} table(s): {', '.join([t.name for t in tables])}"
                else:
                    answer = "No tables uploaded yet. Please upload data first."
            
            elif "data" in question_lower or "statistic" in question_lower:
                if table_name:
                    selected_table = session.query(Table).filter_by(name=table_name).first()
                    if selected_table:
                        cols = session.query(ColumnMetadata).filter_by(table_id=selected_table.id).all()
                        issues = session.query(Issue).filter_by(table_id=selected_table.id).all()
                        answer = f"Data Summary for {table_name}:\n"
                        answer += f"• Columns: {len(cols)}\n"
                        answer += f"• Quality Issues: {len(issues)}\n"
                else:
                    tables = session.query(Table).all()
                    if tables:
                        answer = f"Data Summary ({len(tables)} table(s)):\n"
                        for table in tables:
                            cols = session.query(ColumnMetadata).filter_by(table_id=table.id).all()
                            issues = session.query(Issue).filter_by(table_id=table.id).all()
                            answer += f"• {table.name}: {len(cols)} columns, {len(issues)} issues\n"
                    else:
                        answer = "No data available."
            
            else:
                # Generic answer
                if table_name:
                    selected_table = session.query(Table).filter_by(name=table_name).first()
                    if selected_table:
                        cols = session.query(ColumnMetadata).filter_by(table_id=selected_table.id).all()
                        issues = session.query(Issue).filter_by(table_id=selected_table.id).all()
                        answer = f"Overview of {table_name}:\n"
                        answer += f"• Columns: {len(cols)}\n"
                        answer += f"• Quality Issues: {len(issues)}\n"
                else:
                    tables = session.query(Table).all()
                    all_issues = session.query(Issue).all()
                    answer = f"Database Overview:\n"
                    answer += f"• Tables: {len(tables)}\n"
                    answer += f"• Quality Issues: {len(all_issues)}\n"
            
            return answer if answer else "Could not find relevant information for your question."
        
        finally:
            session.close()

class FixSuggestionAgent:
    """Agent for suggesting SQL fixes and transformations"""
    
    def __init__(self):
        self.llm = get_llm() or MockLLM()
        logger.info("✓ Fix Suggestion Agent initialized")
    
    def suggest_fix(self, issue_description: str, column_name: str, table_name: str, issue_type: str) -> str:
        """Suggest SQL fixes for data quality issues"""
        try:
            # Generate context-specific SQL fixes based on issue type
            fixes = {
                "missing_values": f"""-- Remove rows with null values
DELETE FROM {table_name} WHERE {column_name} IS NULL;

-- OR: Impute with appropriate value
UPDATE {table_name} SET {column_name} = COALESCE({column_name}, 'UNKNOWN') WHERE {column_name} IS NULL;""",
                
                "duplicates": f"""-- Remove duplicate rows, keeping first occurrence
DELETE FROM {table_name} a USING {table_name} b 
WHERE a.ctid > b.ctid AND a.{column_name} = b.{column_name};

-- OR: Use window function to identify duplicates
SELECT *, ROW_NUMBER() OVER (PARTITION BY {column_name} ORDER BY ctid) as rn
FROM {table_name} WHERE rn > 1;""",
                
                "invalid_numeric": f"""-- Remove rows with non-numeric values
DELETE FROM {table_name} WHERE {column_name} ~ '[^0-9.-]';

-- OR: Convert to numeric, setting invalid to NULL
UPDATE {table_name} SET {column_name} = NULL 
WHERE {column_name} !~ '^-?\\d+(\\.\\d+)?$';""",
                
                "negative_values": f"""-- Convert negative values to absolute
UPDATE {table_name} SET {column_name} = ABS(CAST({column_name} AS NUMERIC)) 
WHERE {column_name} < 0;

-- OR: Remove negative values
DELETE FROM {table_name} WHERE {column_name} < 0;""",
                
                "outliers": f"""-- Identify and remove outliers (IQR method)
WITH stats AS (
  SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST({column_name} AS NUMERIC)) as Q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST({column_name} AS NUMERIC)) as Q3
  FROM {table_name}
)
DELETE FROM {table_name} 
WHERE CAST({column_name} AS NUMERIC) < (SELECT Q1 - 1.5*(Q3-Q1) FROM stats)
  OR CAST({column_name} AS NUMERIC) > (SELECT Q3 + 1.5*(Q3-Q1) FROM stats);""",
                
                "mixed_date_formats": f"""-- Standardize to ISO format (YYYY-MM-DD)
UPDATE {table_name} 
SET {column_name} = to_char(to_timestamp({column_name}, 'MM/DD/YYYY'), 'YYYY-MM-DD')
WHERE {column_name} ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}$';

-- Then convert column type
ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE date USING to_date({column_name}, 'YYYY-MM-DD');""",
                
                "case_sensitivity": f"""-- Standardize to lowercase
UPDATE {table_name} SET {column_name} = LOWER({column_name});

-- OR: Standardize to title case
UPDATE {table_name} SET {column_name} = INITCAP(LOWER({column_name}));""",
                
                "whitespace_issues": f"""-- Remove leading/trailing whitespace
UPDATE {table_name} SET {column_name} = TRIM({column_name});

-- Also remove extra internal spaces
UPDATE {table_name} SET {column_name} = REGEXP_REPLACE(TRIM({column_name}), '\\s+', ' ', 'g');""",
                
                "special_characters": f"""-- Remove special characters
UPDATE {table_name} 
SET {column_name} = REGEXP_REPLACE({column_name}, '[^\\w\\s-]', '', 'g');

-- Keep only alphanumeric and spaces
UPDATE {table_name} 
SET {column_name} = REGEXP_REPLACE({column_name}, '[^a-zA-Z0-9\\s]', '', 'g');""",
                
                "unusually_long_values": f"""-- Find rows with very long strings
SELECT * FROM {table_name} WHERE LENGTH({column_name}) > 1000 ORDER BY LENGTH({column_name}) DESC;

-- Truncate to reasonable length
UPDATE {table_name} SET {column_name} = SUBSTRING({column_name}, 1, 500) WHERE LENGTH({column_name}) > 500;""",
            }
            
            # Return context-specific fix or fallback
            suggestion = fixes.get(issue_type, f"""-- Generic fix for {issue_type}
SELECT * FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT 10;
-- Review the data and adjust the following query as needed:
-- UPDATE {table_name} SET {column_name} = ... WHERE ...;""")
            
            return suggestion
            
        except Exception as e:
            logger.error(f"✗ Fix Suggestion Agent failed: {e}")
            return f"-- Error generating fix: {str(e)}"

class LineageAgent:
    """Agent for visualizing and explaining lineage"""
    
    def __init__(self):
        self.llm = get_llm() or MockLLM()
        from src.catalog.lineage_tracker import LineageTracker
        self.lineage_tracker = LineageTracker()
        logger.info("✓ Lineage Agent initialized")
    
    def explain_lineage(self, column_id: int) -> Dict[str, Any]:
        """Explain column lineage"""
        try:
            upstream = self.lineage_tracker.get_upstream_lineage(column_id)
            downstream = self.lineage_tracker.get_downstream_lineage(column_id)
            
            return {
                "column_id": column_id,
                "upstream": upstream,
                "downstream": downstream
            }
        except Exception as e:
            logger.error(f"✗ Lineage Agent failed: {e}")
            return {"error": str(e)}
    
    def get_graph(self):
        """Get lineage graph"""
        try:
            return self.lineage_tracker.get_lineage_graph()
        except Exception as e:
            logger.error(f"✗ Failed to get lineage graph: {e}")
            return None

class AgentOrchestrator:
    """Main orchestrator for all agents"""
    
    def __init__(self):
        self.scanner = ScannerAgent()
        self.qa = QAAgent()
        self.fixer = FixSuggestionAgent()
        self.lineage = LineageAgent()
        logger.info("✓ Agent Orchestrator initialized with all agents")
    
    def dispatch(self, agent_type: str, **kwargs) -> Dict[str, Any]:
        """Dispatch requests to appropriate agent"""
        try:
            if agent_type == "scanner":
                return self.scanner.scan_and_profile(**kwargs)
            elif agent_type == "qa":
                return self.qa.answer_question(**kwargs)
            elif agent_type == "fixer":
                return self.fixer.suggest_fix(**kwargs)
            elif agent_type == "lineage":
                return self.lineage.explain_lineage(**kwargs)
            else:
                return {"error": f"Unknown agent type: {agent_type}"}
        except Exception as e:
            logger.error(f"✗ Agent dispatch failed: {e}")
            return {"error": str(e)}
