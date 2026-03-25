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

### 3. Configure LLM Provider

```bash
cp .env.example .env
# Edit .env and configure your LLM provider
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

## 🤖 LLM Configuration

The NL-BI Dashboard supports **any OpenAI-compatible endpoint**, giving you complete flexibility in choosing your LLM provider.

### Supported Providers

| Provider | Type | Endpoint | Notes |
|----------|------|----------|-------|
| **OpenAI** | Cloud | Default | GPT-4o recommended |
| **Ollama** | Local | `http://localhost:11434/v1` | Free, runs locally |
| **LM Studio** | Local | `http://localhost:1234/v1` | GUI-based local LLM |
| **vLLM** | Local | `http://localhost:8000/v1` | High-performance serving |
| **Groq** | Cloud | `https://api.groq.com/openai/v1` | Ultra-fast inference |
| **Together AI** | Cloud | `https://api.together.xyz/v1` | Open-source models |
| **Azure OpenAI** | Cloud | Your Azure endpoint | Enterprise integration |

### Configuration Methods

#### Method 1: Environment Variables

```bash
# For OpenAI
export OPENAI_API_KEY="sk-your-key-here"

# For custom endpoints (e.g., Ollama)
export LLM_API_KEY="ollama"
export LLM_BASE_URL="http://localhost:11434/v1"
export LLM_MODEL="llama3"
```

#### Method 2: Using Predefined Providers

```python
from sql_chain import run_query

# Use predefined provider
result = run_query("Show top customers", provider="ollama")
```

#### Method 3: Custom Configuration

```python
from sql_chain import run_query, LLMConfig, get_llm

# Create custom config
config = LLMConfig(
    base_url="http://localhost:8000/v1",
    model="llama3",
    api_key="dummy"
)

# Use with run_query
result = run_query("Total revenue by category", config=config)

# Or create LLM directly
llm = get_llm(
    base_url="https://api.groq.com/openai/v1",
    model="llama-3.1-70b-versatile",
    api_key="gsk_xxxx"
)
```

### Example: Using Ollama Locally

```bash
# 1. Install and run Ollama
# See: https://ollama.ai

# 2. Pull a model
ollama pull llama3

# 3. Configure environment
export LLM_API_KEY="ollama"
export LLM_BASE_URL="http://localhost:11434/v1"
export LLM_MODEL="llama3"

# 4. Run queries
python -c "
from sql_chain import run_query
result = run_query('What is the total revenue?')
print(result.sql_query)
print(result.dataframe)
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

### `run_query(user_question, **options)`

Execute a natural language query against the database.

**Parameters:**
- `user_question` (str): Natural language question
- `llm` (BaseChatModel, optional): Pre-configured LLM instance
- `api_key` (str, optional): API key for the LLM provider
- `base_url` (str, optional): Custom API endpoint URL
- `model` (str): Model name (default: "gpt-4o")
- `config` (LLMConfig, optional): Complete configuration object
- `provider` (str, optional): Predefined provider name

**Returns:** `QueryResult` dataclass with:
- `success` (bool): Whether query succeeded
- `sql_query` (str): Generated SQL query
- `dataframe` (pd.DataFrame): Query results
- `error_message` (str): Error if failed
- `retry_count` (int): Number of retries used

### `get_llm(**options)`

Create an LLM instance with flexible configuration.

### `validate_sql(query)`

Validate SQL query for security compliance.

### `LLMConfig`

Configuration dataclass for LLM settings.

```python
from sql_chain import LLMConfig

config = LLMConfig(
    api_key="your-key",
    base_url="http://localhost:11434/v1",
    model="llama3",
    temperature=0.0,
    max_tokens=2000
)
```

## 🧪 Testing

```bash
# Run all tests
python sql_chain.py

# Run with specific provider
LLM_API_KEY=ollama LLM_BASE_URL=http://localhost:11434/v1 LLM_MODEL=llama3 python sql_chain.py
```

## 📈 Roadmap

### Phase 1: MVP (Current)
- [x] Database setup with sample data
- [x] Security validation layer
- [x] LangChain SQL chain
- [x] OpenAI-compatible endpoint support
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
