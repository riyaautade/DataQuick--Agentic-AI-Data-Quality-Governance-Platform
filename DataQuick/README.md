# DataQuick 📊 - AI-Powered Data Quality & Governance

> **Agentic Data Quality Assistant**: Automatically scan datasets, detect quality issues, generate AI-powered SQL fixes, and ask questions about your data—all locally with Streamlit + LangChain agents.

![Python 3.13+](https://img.shields.io/badge/Python-3.13%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)

---

## 🎯 Key Features

✅ **Data Profiling** - Automated statistical analysis of all columns  
✅ **Quality Detection** - 11+ issue types (nulls, duplicates, outliers, formats, etc.)  
✅ **AI-Powered Fixes** - LangChain agents suggest context-specific SQL remedies  
✅ **Table Lineage** - Track column-to-column dependencies and data flows  
✅ **Intelligent Q&A** - Ask questions about your data via natural language  
✅ **Real-time Dashboard** - Beautiful Streamlit UI with interactive charts  
✅ **SQLite Backend** - Local, file-based database (no setup required)  
✅ **Issue Categorization** - Table-wise organization and severity tracking  

---

## 🚀 Quick Start (5 minutes)

### 1. **Clone & Setup**

```powershell
# Clone repository
git clone https://github.com/yourusername/dataquick.git
cd DataQuick

# Create virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. **Run the Dashboard**

```powershell
streamlit run app_ai.py
```

Opens automatically at: **http://localhost:8501**

### 3. **Upload Your Data**

- Go to **📁 Data Ingestion** page
- Upload CSV/Excel file
- System auto-profiles and detects issues

### 4. **Explore Issues & Fix Suggestions**

- **⚠️ Quality Issues**: View all detected problems table-wise
- **🔧 Fix Suggestions (AI)**: Get AI-generated SQL to fix issues
- **🤖 Ask Questions**: Query your data naturally

---

## 📁 Project Structure

```
DataQuick/
├── app_ai.py                      # Main Streamlit dashboard (7 pages)
├── src/
│   ├── database.py                # SQLite connection & session management
│   ├── models.py                  # SQLAlchemy ORM (7 models)
│   ├── config.py                  # Configuration
│   ├── data_layer/
│   │   └── scanner.py             # Data ingestion & table registration
│   ├── profiling/
│   │   ├── profiler.py            # Statistical profiling
│   │   ├── drift_detector.py      # Schema change detection
│   │   └── data_quality_analyzer.py # 11-type quality analyzer
│   ├── catalog/
│   │   └── lineage_tracker.py     # Column lineage tracking
│   └── agents/
│       └── orchestrator.py        # 4 LangChain agents (Scanner, QA, Fix, Lineage)
├── data/                          # Local data storage (CSV uploads)
├── dataquick.db                   # SQLite database file
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment template
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```

---

## 🛠️ Dashboard Pages

| Page | Purpose |
|------|---------|
| **📊 Dashboard** | KPI metrics (total tables, columns, issues), quality distribution charts |
| **📁 Data Ingestion** | Upload CSV/Excel, auto-scan, profile all columns |
| **⚠️ Quality Issues** | Browse detected issues table-wise, view null analysis |
| **🔧 Fix Suggestions** | Generate issue-specific SQL fixes using AI |
| **🤖 Ask Questions** | Natural language Q&A about your data |
| **🔗 Lineage Analysis** | View table and column relationships |
| **⚙️ Settings** | Database status and statistics |

---

## 🧠 11 Data Quality Issue Types Detected

1. **Missing Values** - Null/empty cells in columns
2. **Duplicates** - Exact duplicate rows
3. **Invalid Numeric** - Non-numeric values in numeric columns
4. **Negative Values** - Unexpected negative numbers
5. **Outliers** - Statistical outliers (IQR method)
6. **Mixed Date Formats** - Inconsistent date representations
7. **Case Sensitivity** - Inconsistent capitalization
8. **Whitespace Issues** - Leading/trailing/extra spaces
9. **Special Characters** - Unexpected special characters
10. **Unusually Long Values** - Strings exceeding typical length
11. **Generic Issues** - Fallback for unmapped issue types

---

## 🤖 AI Agents (LangChain)

Four specialized agents handle different tasks:

### **Scanner Agent**
- Scans CSV/Excel files
- Registers tables in database
- Triggers profiling

### **QA Agent**
- Answers natural language questions
- Searches database for context
- Supports table-specific queries

### **Fix Suggestion Agent**
- Generates SQL fixes for each issue type
- Context-specific recommendations
- Deterministic 11-issue-type mapping

### **Lineage Agent**
- Tracks column dependencies
- Visualizes relationships
- Detects schema changes

---

## 📊 Database Schema (SQLite)

7 core models with relationships:

| Model | Purpose |
|-------|---------|
| **Table** | Data source metadata (name, path, created_at) |
| **ColumnMetadata** | Column details (name, inferred_type, sample_value) |
| **Profile** | Statistics (nulls, cardinality, mean, stddev, etc.) |
| **Issue** | Quality problems (issue_type, severity, suggested_fix) |
| **SchemaChange** | Track schema drift |
| **LineageEdge** | Column-to-column relationships |
| **LineageRun** | Lineage analysis runs |

---

## 🌐 Environment Setup

### **.env Configuration**

```bash
# .env file (copy from .env.example)
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_HEADLESS=true

# Optional: LLM Configuration
OLLAMA_BASE_URL=http://localhost:11434  # If using Ollama
```

### **Requirements**

- Python 3.13+
- Windows/Mac/Linux
- ~500MB disk space
- 2GB+ RAM recommended

---

## 🔧 Running the App

### **Windows (PowerShell)**

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run Streamlit
streamlit run app_ai.py

# Or with logging disabled
streamlit run app_ai.py --logger.level=warning
```

### **Mac/Linux (Bash)**

```bash
# Activate virtual environment
source venv/bin/activate

# Run Streamlit
streamlit run app_ai.py
```

### **Access Dashboard**

Open your browser to: **http://localhost:8501**

---

## 📈 Workflow Example

```
1. Upload CSV → Data Ingestion page
   ↓
2. Auto-scan & profile tables
   ↓
3. View detected issues → Quality Issues page
   ↓
4. Generate SQL fixes → Fix Suggestions page
   ↓
5. Ask questions → Ask Questions page
   ↓
6. Monitor progress → Dashboard
```

---

## 🧪 Testing

```powershell
# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=src tests/
```

---

## 📝 Features & Technical Details

### **Profiling Engine**
- Column type inference (numeric, date, categorical, string)
- Null/cardinality analysis
- Distribution statistics
- Outlier detection (IQR method)
- Sample value collection

### **Quality Analyzer**
- 11-type issue detection
- Per-column analysis
- Severity scoring (High/Medium/Low)
- Database persistence
- Issue-specific suggested fixes

### **QA Agent**
- Database-backed context (no RAG required)
- Table-specific filtering
- Keyword-based fallback
- Natural language questions

### **Fix Generator**
- Issue-type-specific SQL
- Deterministic mapping (11 types)
- Example queries for each issue
- Copy-paste ready solutions

---

## 🎨 UI Highlights

✨ **Beautiful Streamlit Dashboard** with:
- Purple gradient sidebar (#667eea → #764ba2)
- Smooth CSS transitions (0.3s ease)
- Interactive Plotly charts (pie, bar, histograms)
- Responsive full-width layout
- Emoji-enhanced navigation
- Professional card-based design

---

## 🔐 Security Notes

- **Local-only operation**: No cloud connectivity required
- **SQLite database**: File-based, no network exposure
- **Environment variables**: Use `.env` for sensitive config
- **No authentication**: Designed for local development

---

## 🚨 Troubleshooting

### **Port 8501 Already in Use**

```powershell
# Kill existing process
taskkill /IM streamlit.exe /F

# Or specify different port
streamlit run app_ai.py --server.port 8502
```

### **Module Import Errors**

```powershell
# Verify virtual environment
.\venv\Scripts\Activate.ps1

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### **Database Errors**

```powershell
# Reset database
del dataquick.db
python init_db.py
```

---

## 📦 Dependencies

**Core Stack:**
- SQLAlchemy 2.0+ (ORM)
- Streamlit 1.28+ (UI)
- LangChain 0.1+ (Agents)
- Pandas 2.0+ (Data)
- Plotly 5.0+ (Charts)
- APScheduler 3.10+ (Jobs)

**See `requirements.txt` for full list**

---

## 🤝 Contributing

Contributions welcome! Areas:
- Additional quality issue detectors
- Enhanced visualizations
- More LLM backends (GPT-4, Claude, etc.)
- Lineage graph rendering
- Background job scheduling

---

## 📄 License

MIT License - See LICENSE file

---

## 👤 Project by

**Riya Autade**

v1.0 | November 2025

---

## 📞 Support

- Check troubleshooting section above
- Review app logs in terminal
- Ensure Python 3.13+ installed
- Verify all dependencies: `pip list`

---

**🎉 Happy Data Quality Checking!**
