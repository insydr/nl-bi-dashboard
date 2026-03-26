# NL-BI Dashboard (Natural Language Business Intelligence Dashboard)

An AI-powered analytics tool that allows non-technical users to query business data using natural language.

## 🎯 Overview

The NL-BI Dashboard translates natural language questions into secure SQL queries, executes them against a read-only database, and automatically visualizes the results. Built following the PRD specifications for the MVP phase.

## 🏗️ Architecture

```
User Question → LangChain SQL Chain → Security Validation → Database Query → Auto Visualization
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
├── app.py               # Streamlit frontend UI
├── sql_chain.py         # LangChain SQL chain and security validation
├── visualization.py     # Automatic chart generation engine
├── database_setup.py    # Database initialization and connection management
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

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/insydr/nl-bi-dashboard.git
cd nl-bi-dashboard

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

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
# Edit .env with your preferred LLM configuration
```

### 4. Run the Dashboard

```bash
streamlit run app.py
```

The dashboard will open at **http://localhost:8501**

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

# 4. Run the dashboard
streamlit run app.py
```

## 📊 Features

### Natural Language Query Interface
- Type questions in plain English
- Context retention for follow-up questions
- Suggested questions for quick exploration

### Automatic Visualization
- **KPI Cards** - Single aggregate values
- **Line Charts** - Time series data
- **Bar Charts** - Category comparisons
- **Pie Charts** - Distribution breakdowns
- **Scatter Plots** - Correlation analysis
- **Tables** - Fallback for complex data

### Transparency & Control
- View generated SQL query
- Download results as CSV
- Query history in sidebar

### Feedback System
- Thumbs up/down feedback
- Helps improve model accuracy

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

```python
from sql_chain import run_query

result = run_query("What were total sales by region last month?")

if result.success:
    print(result.sql_query)
    print(result.dataframe.head())
```

### `generate_chart(df, query)`

Generate an appropriate chart from a DataFrame.

```python
from visualization import generate_chart

fig = generate_chart(df, "Sales by region")
fig.show()  # Or use st.plotly_chart(fig) in Streamlit
```

### `LLMConfig`

Configuration for LLM settings.

```python
from sql_chain import LLMConfig

config = LLMConfig(
    api_key="your-key",
    base_url="http://localhost:11434/v1",
    model="llama3"
)
```

## 🧪 Testing

```bash
# Run validation tests
python sql_chain.py

# Run visualization tests
python visualization.py --save

# Run with specific provider
LLM_API_KEY=ollama LLM_BASE_URL=http://localhost:11434/v1 LLM_MODEL=llama3 python sql_chain.py
```

## 📸 Screenshots

### Main Dashboard
![Dashboard](https://via.placeholder.com/800x400/3b82f6/white?text=NL-BI+Dashboard)

### Query Example
![Query](https://via.placeholder.com/800x400/10b981/white?text=Query+Results)

## 📈 Roadmap

### Phase 1: MVP (Current) ✅
- [x] Database setup with sample data
- [x] Security validation layer
- [x] LangChain SQL chain
- [x] OpenAI-compatible endpoint support
- [x] Streamlit UI
- [x] Plotly visualization

### Phase 2: Enhancement
- [ ] PostgreSQL migration
- [ ] Few-shot prompting
- [ ] Query history persistence
- [ ] "Explain this chart" feature
- [ ] Query caching

### Phase 3: Production
- [ ] React frontend
- [ ] Redis caching
- [ ] Model fine-tuning
- [ ] RBAC implementation
- [ ] Multi-tenant support

## 🐛 Troubleshooting

### "LLM Not Configured" Error
Make sure you have set either `OPENAI_API_KEY` or `LLM_API_KEY` environment variable.

### "Database not found" Error
Run `python database_setup.py` to create the database.

### "Query Failed" with SQL Error
- Check the generated SQL in the "View Generated SQL" section
- Try rephrasing your question
- Use suggested questions as examples

### Slow Response Time
- For local LLMs, ensure your model is loaded
- Consider using a smaller model (e.g., `llama3:8b` instead of `llama3:70b`)
- For cloud providers, check your API rate limits

## 📄 License

MIT License

## 👤 Author

Sydr Dev (rsd.iz.rosyid@gmail.com)

## 🙏 Acknowledgments

- [LangChain](https://langchain.com) - SQL chain framework
- [Streamlit](https://streamlit.io) - UI framework
- [Plotly](https://plotly.com) - Visualization library
- [sqlparse](https://github.com/andialbrecht/sqlparse) - SQL parsing and validation
