# NL-BI Dashboard (Natural Language Business Intelligence Dashboard)

An AI-powered analytics tool that allows non-technical users to query business data using natural language.

## 🎯 Overview

The NL-BI Dashboard translates natural language questions into secure SQL queries, executes them against a read-only database, and automatically visualizes the results. Built following the PRD specifications for the MVP phase.

## 🏗️ Architecture

```
User Question → LangChain SQL Chain → Security Validation → Database Query → Results
```

### Security Layers (Defense in Depth)

1. **SQL Parser Validation** - Validates SQL structure using `sqlparse`
2. **Keyword Blocklist** - Blocks DROP, DELETE, UPDATE, INSERT, etc.
3. **Statement Type Check** - Only SELECT statements allowed
4. **Schema Allow-List** - Only permitted tables/columns accessible
5. **Read-Only Connection** - SQLite URI mode enforces read-only access

## 📁 Project Structure

```
nlbi-dashboard/
├── database_setup.py    # Database initialization and connection management
├── sql_chain.py         # LangChain SQL chain and security validation
├── data/
│   └── ecommerce.db     # SQLite database with sample e-commerce data
├── requirements.txt     # Python dependencies
├── .env.example         # Environment configuration template
└── README.md            # This file
```

## 🗃️ Database Schema

| Table | Rows | Description |
|-------|------|-------------|
| `customers` | 60 | Customer profiles with regions and segments |
| `products` | 60 | Products across 7 categories |
| `orders` | 200 | Orders spanning ~15 months |
| `order_items` | 547 | Line items with quantities and prices |

## 🚀 Quick Start

### 1. Set Up Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Initialize Database

```bash
python database_setup.py
```

### 3. Configure API Key

```bash
cp .env.example .env
# Edit .env and add your OpenAI API key
```

### 4. Test SQL Chain

```bash
# Run validation tests
python sql_chain.py

# Test in Python
python -c "
from sql_chain import run_query, format_result_summary
result = run_query('Show me the top 5 customers by total order amount')
print(format_result_summary(result))
"
```

## 🔒 Security Features

### SQL Injection Prevention

| Attack Vector | Mitigation |
|---------------|------------|
| Multiple statements | Blocked at parser level |
| Comment injection | Pattern detection for `--` and `/*` |
| UNION injection | System table access blocked |
| Keyword injection | Blocklist enforcement |
| Schema violations | Allow-list validation |

### Read-Only Enforcement

```python
# SQLite read-only connection
conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
```

## 📋 API Reference

### `run_query(user_question, llm=None, api_key=None, model="gpt-4o")`

Execute a natural language query against the database.

**Parameters:**
- `user_question` (str): Natural language question
- `llm` (BaseChatModel, optional): Pre-configured LLM instance
- `api_key` (str, optional): OpenAI API key
- `model` (str): Model name (default: "gpt-4o")

**Returns:** `QueryResult` dataclass with:
- `success` (bool): Whether query succeeded
- `sql_query` (str): Generated SQL query
- `dataframe` (pd.DataFrame): Query results
- `error_message` (str): Error if failed
- `retry_count` (int): Number of retries used

### `validate_sql(query)`

Validate SQL query for security compliance.

**Parameters:**
- `query` (str): SQL query to validate

**Returns:** `SQLValidationResult` with validation status and details.

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run validation tests only
python sql_chain.py
```

## 📈 Roadmap

### Phase 1: MVP (Current)
- [x] Database setup with sample data
- [x] Security validation layer
- [x] LangChain SQL chain
- [ ] Streamlit UI
- [ ] Plotly visualization

### Phase 2: Enhancement
- [ ] PostgreSQL migration
- [ ] Few-shot prompting
- [ ] Query history
- [ ] "Explain this chart" feature

### Phase 3: Production
- [ ] React frontend
- [ ] Redis caching
- [ ] Model fine-tuning
- [ ] RBAC implementation

## 📄 License

MIT License

## 👤 Author

Sydr Dev (rsd.iz.rosyid@gmail.com)
