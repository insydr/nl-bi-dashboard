"""
Natural Language Business Intelligence Dashboard - SQL Chain Module
====================================================================

This module implements the core LangChain SQL logic that converts natural
language queries to SQL and executes them safely against the database.

Features:
- LangChain SQL query chain integration
- Multi-layer security validation (sqlparse + regex + schema allow-list)
- Dynamic schema loading for LLM context
- Self-correction retry mechanism (max 2 retries)
- Read-only enforcement at multiple levels
- Multi-database support (SQLite, PostgreSQL)

Security Architecture (Defense in Depth):
1. SQL Parser Validation (sqlparse) - Validates SQL structure
2. Keyword Blocklist (regex) - Blocks destructive commands
3. Statement Type Check - Only SELECT statements allowed
4. Schema Allow-List - Only permitted tables/columns
5. Read-Only DB Connection - SQLite URI mode or PostgreSQL read-only user
"""

import re
import sqlparse
import pandas as pd
import os
from typing import Optional, Tuple, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum

# LangChain imports
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate, MessagesPlaceholder, FewShotPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableLambda, RunnableSequence
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, BaseMessage
from langchain_core.language_models.chat_models import BaseChatModel

# LangChain OpenAI for LLM integration
from langchain_openai import ChatOpenAI

# LangChain database utilities
from langchain_community.utilities import SQLDatabase

# Pydantic for model validation
from pydantic import BaseModel, Field

# Local imports
from database_setup import (
    get_db_engine,
    get_schema_for_prompt,
    ALLOWED_TABLES,
    BLOCKED_KEYWORDS,
    DB_TYPE,
    DatabaseType,
    get_connection_string,
    log_query_to_db,
    get_recent_queries,
    update_query_feedback,
    ensure_query_logs_table
)

# Few-shot examples for improved SQL generation
from query_examples import (
    get_few_shot_examples,
    get_examples_for_langchain,
    get_relevant_examples,
    SQLExample
)

# Security module imports
from security import (
    sanitize_user_input,
    enforce_row_limit,
    check_rate_limit,
    record_query,
    check_additional_sql_patterns,
    generate_safe_error_message,
    perform_security_check,
    log_security_event,
    MAX_ROWS_LIMIT
)


# =============================================================================
# Configuration & Constants
# =============================================================================

# Maximum retries for SQL self-correction (per PRD FR-10)
MAX_RETRIES = 2

# SQL statement types that are allowed
ALLOWED_STATEMENT_TYPES = {"SELECT"}


# =============================================================================
# LLM Configuration (OpenAI-Compatible Endpoints)
# =============================================================================

@dataclass
class LLMConfig:
    """
    Configuration for LLM connections supporting any OpenAI-compatible endpoint.
    
    This allows using:
    - OpenAI (default)
    - Azure OpenAI
    - Local LLMs (Ollama, LM Studio, vLLM)
    - Other providers (Together AI, Groq, Anyscale, etc.)
    
    Environment Variables (in order of priority):
        LLM_API_KEY / OPENAI_API_KEY - API key
        LLM_BASE_URL - Base URL for API endpoint
        LLM_MODEL - Model name to use
    """
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 2000
    
    def __post_init__(self):
        """Load from environment variables if not explicitly set."""
        # API Key: Check LLM_API_KEY first, then OPENAI_API_KEY for backward compatibility
        if self.api_key is None:
            self.api_key = os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY")
        
        # Base URL: For custom endpoints (Ollama, vLLM, etc.)
        if self.base_url is None:
            self.base_url = os.environ.get("LLM_BASE_URL")
        
        # Model: Allow override via environment
        env_model = os.environ.get("LLM_MODEL")
        if env_model:
            self.model = env_model
    
    @classmethod
    def from_env(cls) -> "LLMConfig":
        """Create configuration from environment variables."""
        return cls()
    
    def validate(self) -> Tuple[bool, str]:
        """Validate configuration."""
        if not self.api_key:
            return False, "No API key configured. Set LLM_API_KEY or OPENAI_API_KEY environment variable."
        return True, ""
    
    def get_provider_info(self) -> str:
        """Return human-readable provider information."""
        if self.base_url:
            if "localhost" in self.base_url or "127.0.0.1" in self.base_url:
                return f"Local LLM at {self.base_url}"
            return f"Custom endpoint: {self.base_url}"
        return "OpenAI (default)"


# Predefined configurations for common providers
LLM_PROVIDERS = {
    "openai": LLMConfig(
        base_url=None,  # Uses default OpenAI endpoint
        model="gpt-4o"
    ),
    "ollama": LLMConfig(
        base_url="http://localhost:11434/v1",
        model="llama3",
        api_key="ollama"  # Ollama doesn't need a real API key
    ),
    "lm-studio": LLMConfig(
        base_url="http://localhost:1234/v1",
        model="local-model",
        api_key="lm-studio"
    ),
    "groq": LLMConfig(
        base_url="https://api.groq.com/openai/v1",
        model="llama-3.1-70b-versatile"
    ),
    "together": LLMConfig(
        base_url="https://api.together.xyz/v1",
        model="meta-llama/Llama-3-70b-chat-hf"
    ),
    "azure": LLMConfig(
        # Azure requires additional setup, base_url should be your Azure endpoint
        model="gpt-4o"
    ),
    "vllm": LLMConfig(
        base_url="http://localhost:8000/v1",
        model="auto"
    ),
}


# =============================================================================
# Few-Shot Example Selection Configuration
# =============================================================================

# Enable/disable dynamic example selection based on semantic similarity
ENABLE_DYNAMIC_EXAMPLES = os.environ.get("ENABLE_DYNAMIC_EXAMPLES", "true").lower() == "true"

# Number of examples to include in prompt (3-5 recommended to avoid token limits)
NUM_FEW_SHOT_EXAMPLES = int(os.environ.get("NUM_FEW_SHOT_EXAMPLES", "3"))

# Minimum similarity threshold for dynamic example selection
MIN_EXAMPLE_SIMILARITY = float(os.environ.get("MIN_EXAMPLE_SIMILARITY", "0.3"))


# =============================================================================
# Database-Specific SQL Prompts
# =============================================================================

SQL_SYSTEM_PROMPT_SQLITE = """You are an expert SQL assistant for an e-commerce database. Your task is to translate natural language questions into valid SQLite queries.

{schema_info}

{few_shot_examples}

CRITICAL SECURITY RULES:
1. ONLY generate SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, or any data modification commands.
2. Use ONLY the tables and columns shown in the schema above.
3. For date comparisons, use SQLite date format (YYYY-MM-DD).
4. Return ONLY the SQL query without any explanation, markdown formatting, or code blocks.
5. Do not use subqueries that attempt to access system tables.
6. Use proper JOIN syntax when combining data from multiple tables.
7. Use meaningful column aliases for calculated fields.
8. For aggregations, always include appropriate GROUP BY clauses.

DATABASE-SPECIFIC NOTES (SQLite):
- This is SQLite, not PostgreSQL or MySQL
- Date functions: Use date(), strftime(), datetime() for date operations
- String functions: Use || for concatenation, LIKE for pattern matching
- Boolean values: SQLite uses 0 and 1 for false/true
- Use LIMIT for row restrictions
- For month-over-month comparisons, use window functions like LAG() with CTEs

When the user asks a question, generate a single, valid SQLite SELECT query that answers it.
Return ONLY the SQL query - no explanations, no markdown, no code blocks."""

SQL_SYSTEM_PROMPT_POSTGRESQL = """You are an expert SQL assistant for an e-commerce database. Your task is to translate natural language questions into valid PostgreSQL queries.

{schema_info}

{few_shot_examples}

CRITICAL SECURITY RULES:
1. ONLY generate SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, or any data modification commands.
2. Use ONLY the tables and columns shown in the schema above.
3. For date comparisons, use PostgreSQL date format (YYYY-MM-DD).
4. Return ONLY the SQL query without any explanation, markdown formatting, or code blocks.
5. Do not use subqueries that attempt to access system tables.
6. Use proper JOIN syntax when combining data from multiple tables.
7. Use meaningful column aliases for calculated fields.
8. For aggregations, always include appropriate GROUP BY clauses.

DATABASE-SPECIFIC NOTES (PostgreSQL):
- This is PostgreSQL, not SQLite or MySQL
- Date functions: Use DATE_TRUNC(), TO_CHAR(), EXTRACT(), NOW() for date operations
- String functions: Use || for concatenation, ILIKE for case-insensitive pattern matching
- Boolean values: Use TRUE/FALSE keywords
- Use LIMIT for row restrictions
- Use DOUBLE PRECISION for floating point calculations
- Quote identifiers with double quotes if needed (e.g., "order")
- For month-over-month comparisons, use window functions like LAG() with CTEs

When the user asks a question, generate a single, valid PostgreSQL SELECT query that answers it.
Return ONLY the SQL query - no explanations, no markdown, no code blocks."""


def get_sql_system_prompt() -> str:
    """Get the appropriate SQL system prompt based on database type."""
    if DB_TYPE == DatabaseType.POSTGRESQL:
        return SQL_SYSTEM_PROMPT_POSTGRESQL
    return SQL_SYSTEM_PROMPT_SQLITE


def format_few_shot_examples(examples: List[SQLExample]) -> str:
    """
    Format few-shot examples for inclusion in the system prompt.

    Args:
        examples: List of SQLExample objects to format

    Returns:
        Formatted string with examples for the prompt
    """
    if not examples:
        return ""

    formatted = "HERE ARE SOME EXAMPLE QUERIES TO GUIDE YOU:\n\n"

    for i, ex in enumerate(examples, 1):
        formatted += f"Example {i}:\n"
        formatted += f"Question: {ex.question}\n"
        formatted += f"SQL: {ex.sql}\n\n"

    return formatted


def select_examples_for_question(question: str) -> List[SQLExample]:
    """
    Select the most relevant examples for a given question.

    Uses dynamic selection if enabled, otherwise returns a curated set.

    Args:
        question: User's natural language question

    Returns:
        List of relevant SQLExample objects
    """
    if ENABLE_DYNAMIC_EXAMPLES:
        # Use semantic similarity or keyword matching
        return get_relevant_examples(
            question,
            top_k=NUM_FEW_SHOT_EXAMPLES,
            min_similarity=MIN_EXAMPLE_SIMILARITY
        )
    else:
        # Return a curated mix of examples
        all_examples = get_few_shot_examples()
        # Prioritize examples of varying complexity
        simple = [ex for ex in all_examples if ex.complexity == "simple"]
        medium = [ex for ex in all_examples if ex.complexity == "medium"]
        complex_ex = [ex for ex in all_examples if ex.complexity == "complex"]

        selected = []
        if simple:
            selected.append(simple[0])
        if medium:
            selected.append(medium[0])
        if complex_ex:
            selected.append(complex_ex[0])

        return selected[:NUM_FEW_SHOT_EXAMPLES]


# For backward compatibility
SQL_SYSTEM_PROMPT = get_sql_system_prompt()


# =============================================================================
# Security Validation Layer
# =============================================================================

class ValidationError(Exception):
    """Custom exception for SQL validation errors."""
    pass


class SQLValidationResult(BaseModel):
    """Result of SQL validation."""
    is_valid: bool
    error_message: Optional[str] = None
    cleaned_sql: Optional[str] = None
    detected_tables: List[str] = Field(default_factory=list)
    detected_statement_type: Optional[str] = None


def validate_sql(query: str) -> SQLValidationResult:
    """
    Comprehensive SQL validation with multiple security layers.
    
    This function implements defense-in-depth security by checking:
    1. Statement type (only SELECT allowed)
    2. Keyword blocklist (DROP, DELETE, UPDATE, etc.)
    3. Schema allow-list (only known tables/columns)
    4. SQL injection patterns (multi-statement, comments, etc.)
    
    Args:
        query: The SQL query string to validate
        
    Returns:
        SQLValidationResult with validation status and details
    """
    if not query or not query.strip():
        return SQLValidationResult(
            is_valid=False,
            error_message="Empty query provided"
        )
    
    # Clean and normalize the query
    cleaned_query = query.strip()
    
    # Remove trailing semicolon for consistent parsing
    if cleaned_query.endswith(';'):
        cleaned_query = cleaned_query[:-1].strip()
    
    # =========================================================================
    # Layer 1: Parse SQL and check statement type
    # =========================================================================
    try:
        parsed = sqlparse.parse(cleaned_query)
        
        if not parsed:
            return SQLValidationResult(
                is_valid=False,
                error_message="Could not parse SQL query"
            )
        
        # Check for multiple statements (potential injection)
        if len(parsed) > 1:
            return SQLValidationResult(
                is_valid=False,
                error_message="Multiple SQL statements are not allowed (potential injection)"
            )
        
        statement = parsed[0]
        statement_type = statement.get_type().upper()
        
        # Only SELECT statements allowed
        if statement_type not in ALLOWED_STATEMENT_TYPES:
            return SQLValidationResult(
                is_valid=False,
                error_message=f"Statement type '{statement_type}' is not allowed. Only SELECT statements are permitted.",
                detected_statement_type=statement_type
            )
        
    except Exception as e:
        return SQLValidationResult(
            is_valid=False,
            error_message=f"SQL parsing error: {str(e)}"
        )
    
    # =========================================================================
    # Layer 2: Keyword blocklist check
    # =========================================================================
    query_upper = cleaned_query.upper()
    
    # Check for blocked keywords
    for keyword in BLOCKED_KEYWORDS:
        # Use word boundary to avoid false positives
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, query_upper):
            return SQLValidationResult(
                is_valid=False,
                error_message=f"Blocked keyword '{keyword}' detected in query. This operation is not permitted.",
                detected_statement_type=statement_type
            )
    
    # =========================================================================
    # Layer 3: SQL Injection Pattern Detection
    # =========================================================================
    
    # Check for common injection patterns
    injection_patterns = [
        (r';.*(?:SELECT|INSERT|UPDATE|DELETE|DROP)', "Multiple statements"),
        (r'--', "SQL comment"),
        (r'/\*', "Block comment"),
        (r'xp_cmdshell', "Command execution"),
        (r'INTO\s+OUTFILE', "File output operation"),
        (r'LOAD_FILE', "File reading"),
        (r'EXEC\s+\w+', "Stored procedure execution"),
        (r'WAITFOR\s+DELAY', "Time-based injection"),
        (r'BENCHMARK\s*\(', "MySQL benchmark injection"),
        (r'SLEEP\s*\(', "MySQL sleep injection"),
    ]
    
    for pattern, desc in injection_patterns:
        if re.search(pattern, query_upper, re.IGNORECASE):
            return SQLValidationResult(
                is_valid=False,
                error_message=f"Potential SQL injection pattern detected ({desc}). Query blocked for security.",
                detected_statement_type=statement_type
            )
    
    # Check for UNION-based injection (but allow legitimate UNION queries)
    # A UNION injection typically tries to extract data from system tables
    union_system_pattern = r'UNION\s+(?:ALL\s+)?SELECT.*(?:information_schema|sqlite_master|pg_|sys\.|mysql\.)'
    if re.search(union_system_pattern, query_upper, re.IGNORECASE):
        return SQLValidationResult(
            is_valid=False,
            error_message="Potential SQL injection: UNION with system table access attempt.",
            detected_statement_type=statement_type
        )
    
    # =========================================================================
    # Layer 4: Schema Allow-List Validation
    # =========================================================================

    # Extract CTE names from WITH clause (these are valid virtual tables)
    cte_names = set()
    cte_pattern = r'\bWITH\s+(\w+)\s+AS\s*\('
    cte_matches = re.findall(cte_pattern, query_upper, re.IGNORECASE)
    for match in cte_matches:
        cte_names.add(match.lower())

    # Also handle multiple CTEs: WITH cte1 AS (...), cte2 AS (...)
    multi_cte_pattern = r',\s*(\w+)\s+AS\s*\('
    multi_cte_matches = re.findall(multi_cte_pattern, query_upper, re.IGNORECASE)
    for match in multi_cte_matches:
        cte_names.add(match.lower())

    # Extract table names from query using regex
    detected_tables = []

    # Pattern to find tables in FROM and JOIN clauses
    from_pattern = r'\bFROM\s+(\w+)'
    join_pattern = r'\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+(\w+)'

    for pattern in [from_pattern, join_pattern]:
        matches = re.findall(pattern, query_upper)
        for match in matches:
            table_lower = match.lower()
            if table_lower not in detected_tables:
                detected_tables.append(table_lower)

    # Validate tables against allow-list (excluding CTE names)
    allowed_tables_lower = [t.lower() for t in ALLOWED_TABLES.keys()]

    for table in detected_tables:
        # Skip CTE names - they are valid virtual tables defined in the query
        if table in cte_names:
            continue
        if table not in allowed_tables_lower:
            return SQLValidationResult(
                is_valid=False,
                error_message=f"Table '{table}' is not in the allowed list. Access denied.",
                detected_tables=detected_tables,
                detected_statement_type=statement_type
            )
    
    # =========================================================================
    # Layer 5: Check for suspicious hex encoding (potential obfuscation)
    # =========================================================================
    
    # Hex encoded strings that are excessively long (potential obfuscation)
    hex_pattern = r'0x[0-9a-fA-F]{20,}'
    if re.search(hex_pattern, cleaned_query):
        return SQLValidationResult(
            is_valid=False,
            error_message="Suspicious hex-encoded string detected. Query blocked for security.",
            detected_statement_type=statement_type
        )
    
    # All validations passed
    return SQLValidationResult(
        is_valid=True,
        cleaned_sql=cleaned_query,
        detected_tables=detected_tables,
        detected_statement_type=statement_type
    )


# =============================================================================
# SQL Query Chain
# =============================================================================

def get_llm(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: str = "gpt-4o",
    temperature: float = 0.0,
    config: Optional[LLMConfig] = None,
    provider: Optional[str] = None
) -> BaseChatModel:
    """
    Get the LLM instance for SQL generation.
    
    Supports any OpenAI-compatible endpoint including:
    - OpenAI (default)
    - Azure OpenAI
    - Local LLMs: Ollama, LM Studio, vLLM
    - Other providers: Together AI, Groq, Anyscale, etc.
    
    Args:
        api_key: API key (uses LLM_API_KEY or OPENAI_API_KEY env var if not provided)
        base_url: Custom API endpoint URL (uses LLM_BASE_URL env var if not provided)
        model: Model name (default: gpt-4o, uses LLM_MODEL env var if set)
        temperature: Temperature for generation (default: 0.0 for consistency)
        config: LLMConfig instance (overrides individual parameters)
        provider: Predefined provider name ("openai", "ollama", "groq", etc.)
        
    Returns:
        Configured ChatModel instance
        
    Examples:
        # Using OpenAI (default)
        llm = get_llm(api_key="sk-...")
        
        # Using Ollama locally
        llm = get_llm(provider="ollama")
        
        # Using custom endpoint
        llm = get_llm(
            base_url="http://localhost:8000/v1",
            model="llama3",
            api_key="dummy"
        )
        
        # Using environment variables
        # export LLM_BASE_URL="http://localhost:11434/v1"
        # export LLM_MODEL="llama3"
        # export LLM_API_KEY="ollama"
        llm = get_llm()
    """
    # Use predefined provider if specified
    if provider and provider in LLM_PROVIDERS:
        config = LLM_PROVIDERS[provider]
    
    # Create or merge configuration
    if config is None:
        config = LLMConfig(
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=temperature
        )
    else:
        # Override config with explicit parameters
        if api_key is not None:
            config.api_key = api_key
        if base_url is not None:
            config.base_url = base_url
        if model != "gpt-4o":  # Only override if not default
            config.model = model
        if temperature != 0.0:
            config.temperature = temperature
    
    # Validate configuration
    is_valid, error_msg = config.validate()
    if not is_valid:
        raise ValueError(error_msg)
    
    # Build ChatOpenAI with optional base_url
    llm_kwargs = {
        "model": config.model,
        "temperature": config.temperature,
        "api_key": config.api_key,
        "max_tokens": config.max_tokens,
    }
    
    # Only add base_url if specified (for custom endpoints)
    if config.base_url:
        llm_kwargs["base_url"] = config.base_url
    
    return ChatOpenAI(**llm_kwargs)


def create_sql_generation_prompt(
    examples: Optional[List[SQLExample]] = None,
    use_few_shot: bool = True
) -> ChatPromptTemplate:
    """
    Create the prompt template for SQL generation with few-shot examples.

    Args:
        examples: Optional list of SQLExample objects. If not provided and use_few_shot=True,
                  a default set will be used.
        use_few_shot: Whether to include few-shot examples in the prompt

    Returns:
        ChatPromptTemplate configured for SQL generation
    """
    schema_info = get_schema_for_prompt()
    system_prompt = get_sql_system_prompt()

    # Format few-shot examples
    if use_few_shot:
        if examples is None:
            # Get a default set of examples
            examples = get_few_shot_examples()[:NUM_FEW_SHOT_EXAMPLES]
        few_shot_str = format_few_shot_examples(examples)
    else:
        few_shot_str = ""

    system_content = system_prompt.format(
        schema_info=schema_info,
        few_shot_examples=few_shot_str
    )

    return ChatPromptTemplate.from_messages([
        ("system", system_content),
        ("human", "{question}")
    ])


def create_dynamic_prompt_for_question(question: str) -> ChatPromptTemplate:
    """
    Create a prompt template dynamically selected examples for a specific question.

    This function selects the most relevant examples based on the user's
    question using semantic similarity or keyword matching.

    Args:
        question: User's natural language question

    Returns:
        ChatPromptTemplate with relevant few-shot examples
    """
    # Select relevant examples
    relevant_examples = select_examples_for_question(question)

    # Create prompt with selected examples
    return create_sql_generation_prompt(examples=relevant_examples)


def extract_sql_from_response(response: str) -> str:
    """
    Extract SQL query from LLM response.
    
    Handles cases where LLM might wrap SQL in markdown code blocks
    or include extra text.
    """
    if not response:
        return ""
    
    # Remove markdown code blocks if present
    sql_pattern = r'```(?:sql)?\s*([\s\S]*?)\s*```'
    matches = re.findall(sql_pattern, response, re.IGNORECASE)
    
    if matches:
        return matches[0].strip()
    
    # If no code blocks, try to extract SQL-like content
    # Look for SELECT ... FROM ... pattern
    select_pattern = r'(SELECT\s+[\s\S]+?)(?:;|$)'
    matches = re.findall(select_pattern, response, re.IGNORECASE)
    
    if matches:
        return matches[0].strip()
    
    # Return as-is if no patterns match
    return response.strip()


def execute_sql_safely(sql: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Execute a validated SQL query safely with read-only connection.
    
    Security measures:
    1. Uses read-only database connection (SQLAlchemy engine)
    2. Enforces row limit to prevent memory overload
    3. Sanitizes error messages to prevent information leakage
    
    Args:
        sql: The validated SQL query string
        
    Returns:
        Tuple of (success, dataframe, error_message)
    """
    try:
        # Enforce row limit before execution
        safe_sql = enforce_row_limit(sql, MAX_ROWS_LIMIT)
        
        # Get read-only SQLAlchemy engine
        engine = get_db_engine(read_only=True)
        
        # Execute query with row limit
        df = pd.read_sql_query(safe_sql, engine)
        
        # Double-check row count (belt and suspenders)
        if len(df) > MAX_ROWS_LIMIT:
            df = df.head(MAX_ROWS_LIMIT)
            log_security_event(
                "ROW_LIMIT_ENFORCED",
                {"rows": len(df), "limit": MAX_ROWS_LIMIT}
            )
        
        return True, df, ""
        
    except Exception as e:
        # Generate safe error message
        safe_error = generate_safe_error_message(e, include_details=False)
        return False, pd.DataFrame(), safe_error


# =============================================================================
# Main Query Result Class
# =============================================================================

@dataclass
class QueryResult:
    """Result of a natural language query execution."""
    success: bool
    sql_query: Optional[str] = None
    dataframe: Optional[pd.DataFrame] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    validation_details: Optional[SQLValidationResult] = None
    log_id: Optional[int] = None  # ID of the query log entry
    execution_time_ms: Optional[int] = None  # Query execution time in milliseconds


# =============================================================================
# Main Query Function
# =============================================================================

def run_query(
    user_question: str,
    llm: Optional[BaseChatModel] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: str = "gpt-4o",
    config: Optional[LLMConfig] = None,
    provider: Optional[str] = None,
    user_id: str = "default"
) -> QueryResult:
    """
    Main entry point for natural language to SQL query execution.
    
    This function orchestrates the full query pipeline with security checks:
    1. Sanitize and validate user input
    2. Check rate limits
    3. Generate SQL from natural language using LLM
    4. Validate the generated SQL for security
    5. Execute the query safely with row limits
    6. Retry with error feedback if needed (max 2 retries per PRD FR-10)
    
    Security Features (per PRD Section 7):
    - Input sanitization
    - Rate limiting (30 queries per 5 minutes by default)
    - SQL validation (multiple layers)
    - Row limits (max 1000 rows)
    - Read-only connection enforcement
    - Safe error messages
    
    Args:
        user_question: The natural language question from the user
        llm: Optional pre-configured LLM instance (bypasses other config)
        api_key: API key (uses LLM_API_KEY or OPENAI_API_KEY env var if not provided)
        base_url: Custom API endpoint URL (uses LLM_BASE_URL env var if not provided)
        model: Model name (default: gpt-4o)
        config: LLMConfig instance with all settings
        provider: Predefined provider name ("openai", "ollama", "groq", "together", etc.)
        user_id: User/session identifier for rate limiting
        
    Returns:
        QueryResult containing the SQL, dataframe, and status
    """
    import time
    start_time = time.time()
    
    # =========================================================================
    # Security Check 1: Input Sanitization
    # =========================================================================
    sanitization = sanitize_user_input(user_question)
    if not sanitization.is_safe:
        log_security_event(
            "INPUT_REJECTED",
            {"reason": sanitization.blocked_reason},
            user_id
        )
        return QueryResult(
            success=False,
            error_message=sanitization.blocked_reason
        )
    
    # Use sanitized input
    clean_question = sanitization.sanitized_input
    
    # =========================================================================
    # Security Check 2: Rate Limiting
    # =========================================================================
    rate_check = check_rate_limit(user_id)
    if not rate_check.is_allowed:
        log_security_event(
            "RATE_LIMITED",
            {"remaining": rate_check.remaining_queries},
            user_id
        )
        return QueryResult(
            success=False,
            error_message=rate_check.blocked_reason
        )
    
    # =========================================================================
    # Initialize LLM
    # =========================================================================
    if llm is None:
        try:
            llm = get_llm(
                api_key=api_key,
                base_url=base_url,
                model=model,
                config=config,
                provider=provider
            )
        except ValueError as e:
            return QueryResult(
                success=False,
                error_message=str(e)
            )
    
    # Create the prompt template with dynamic few-shot examples
    prompt = create_dynamic_prompt_for_question(clean_question)
    
    retry_count = 0
    last_error = ""
    last_sql = ""
    
    # Get database-specific syntax hint for retries
    db_syntax = "PostgreSQL" if DB_TYPE == DatabaseType.POSTGRESQL else "SQLite"
    
    while retry_count <= MAX_RETRIES:
        try:
            # =================================================================
            # Step 1: Generate SQL from natural language
            # =================================================================
            
            if retry_count == 0:
                # First attempt - use original question
                chain = prompt | llm | StrOutputParser()
                raw_response = chain.invoke({"question": user_question})
            else:
                # Retry attempt - include error feedback for self-correction
                # Use the same few-shot examples as the original prompt
                relevant_examples = select_examples_for_question(clean_question)
                few_shot_str = format_few_shot_examples(relevant_examples)
                
                retry_prompt = ChatPromptTemplate.from_messages([
                    ("system", get_sql_system_prompt().format(
                        schema_info=get_schema_for_prompt(),
                        few_shot_examples=few_shot_str
                    )),
                    ("human", """Previous attempt generated this SQL:
```sql
{last_sql}
```

This failed with error: {error}

Please generate a CORRECTED SQL query for: {question}

Remember:
1. Only SELECT statements allowed
2. Use exact table/column names from schema
3. Valid {db_syntax} syntax only
4. Refer to the examples above for correct syntax patterns"""),
                ])
                
                chain = retry_prompt | llm | StrOutputParser()
                raw_response = chain.invoke({
                    "question": user_question,
                    "last_sql": last_sql,
                    "error": last_error,
                    "db_syntax": db_syntax
                })
            
            # Extract SQL from response
            sql_query = extract_sql_from_response(raw_response)
            last_sql = sql_query
            
            if not sql_query:
                return QueryResult(
                    success=False,
                    error_message="LLM did not generate a valid SQL query",
                    retry_count=retry_count
                )
            
            # =================================================================
            # Step 2: Validate the generated SQL
            # =================================================================
            
            validation_result = validate_sql(sql_query)
            
            if not validation_result.is_valid:
                # If validation fails, try to fix with LLM
                if retry_count < MAX_RETRIES:
                    last_error = validation_result.error_message
                    retry_count += 1
                    continue
                
                return QueryResult(
                    success=False,
                    sql_query=sql_query,
                    error_message=f"SQL Validation Failed: {validation_result.error_message}",
                    retry_count=retry_count,
                    validation_details=validation_result
                )
            
            # =================================================================
            # Step 3: Execute the validated query
            # =================================================================
            
            success, df, error = execute_sql_safely(validation_result.cleaned_sql)
            
            if success:
                # Calculate execution time
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                # Log successful query to database
                log_id = log_query_to_db(
                    user_question=user_question,
                    generated_sql=validation_result.cleaned_sql,
                    success=True,
                    row_count=len(df),
                    execution_time_ms=execution_time_ms
                )
                
                return QueryResult(
                    success=True,
                    sql_query=validation_result.cleaned_sql,
                    dataframe=df,
                    retry_count=retry_count,
                    validation_details=validation_result,
                    log_id=log_id,
                    execution_time_ms=execution_time_ms
                )
            else:
                # Execution failed - prepare for retry
                last_error = error
                retry_count += 1
                
                if retry_count > MAX_RETRIES:
                    # Calculate execution time
                    execution_time_ms = int((time.time() - start_time) * 1000)
                    
                    # Log failed query
                    log_id = log_query_to_db(
                        user_question=user_question,
                        generated_sql=sql_query,
                        success=False,
                        error_message=error,
                        execution_time_ms=execution_time_ms
                    )
                    
                    return QueryResult(
                        success=False,
                        sql_query=sql_query,
                        error_message=f"Query execution failed after {MAX_RETRIES} retries. Last error: {error}",
                        retry_count=retry_count,
                        validation_details=validation_result,
                        log_id=log_id,
                        execution_time_ms=execution_time_ms
                    )
                
        except Exception as e:
            retry_count += 1
            last_error = str(e)
            
            if retry_count > MAX_RETRIES:
                # Calculate execution time
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                # Log failed query
                log_id = log_query_to_db(
                    user_question=user_question,
                    generated_sql=last_sql if last_sql else None,
                    success=False,
                    error_message=str(e),
                    execution_time_ms=execution_time_ms
                )
                
                return QueryResult(
                    success=False,
                    error_message=f"Unexpected error after {MAX_RETRIES} retries: {str(e)}",
                    retry_count=retry_count,
                    log_id=log_id,
                    execution_time_ms=execution_time_ms
                )
    
    # Should not reach here, but return failure if we do
    execution_time_ms = int((time.time() - start_time) * 1000)
    log_id = log_query_to_db(
        user_question=user_question,
        generated_sql=last_sql if last_sql else None,
        success=False,
        error_message="Maximum retries exceeded",
        execution_time_ms=execution_time_ms
    )
    
    return QueryResult(
        success=False,
        error_message="Maximum retries exceeded",
        retry_count=retry_count,
        log_id=log_id,
        execution_time_ms=execution_time_ms
    )


# =============================================================================
# Utility Functions
# =============================================================================

def get_table_info() -> str:
    """
    Get formatted table information for debugging/display.
    """
    conn_str = get_connection_string(read_only=True)
    db = SQLDatabase.from_uri(conn_str)
    return db.get_table_info()


def format_result_summary(result: QueryResult) -> str:
    """
    Format a QueryResult into a human-readable summary.
    """
    if result.success:
        summary = f"""Query executed successfully!
SQL: {result.sql_query}
Rows returned: {len(result.dataframe)}
Retries: {result.retry_count}
"""
        if not result.dataframe.empty:
            summary += f"\nPreview:\n{result.dataframe.head(5).to_string()}"
        return summary
    else:
        return f"""Query failed.
Error: {result.error_message}
SQL attempted: {result.sql_query}
Retries: {result.retry_count}
"""


# =============================================================================
# Data Insights Function
# =============================================================================

def generate_data_insights(
    df: pd.DataFrame,
    question: str,
    llm: Optional[BaseChatModel] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: str = "gpt-4o",
    config: Optional[LLMConfig] = None
) -> str:
    """
    Generate natural language insights from a DataFrame using the LLM.
    
    This function analyzes the data and provides key trends, patterns,
    and anomalies in an easy-to-understand format.
    
    Args:
        df: The DataFrame to analyze
        question: The original user question (for context)
        llm: Optional pre-configured LLM instance
        api_key: API key override
        base_url: Custom endpoint URL
        model: Model name (default: gpt-4o)
        config: LLMConfig instance
        
    Returns:
        String containing the insights in bullet point format
    """
    if df is None or df.empty:
        return "No data available to analyze."
    
    # Initialize LLM if not provided
    if llm is None:
        try:
            llm = get_llm(
                api_key=api_key,
                base_url=base_url,
                model=model,
                config=config
            )
        except ValueError as e:
            return f"Unable to generate insights: {str(e)}"
    
    # Prepare data summary for the LLM
    try:
        # Get head of data
        head_str = df.head(10).to_string()
        
        # Get describe for numeric columns
        describe_str = ""
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            describe_str = df[numeric_cols].describe().to_string()
        
        # Get info about columns
        col_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            unique = df[col].nunique()
            nulls = df[col].isnull().sum()
            col_info.append(f"  - {col}: {dtype}, {unique} unique values, {nulls} nulls")
        
        col_info_str = "\n".join(col_info)
        
        # Create the prompt
        insights_prompt = f"""Act as a data analyst. Analyze the following data and provide key insights.

**Original Question:** {question}

**Data Summary:**
- Total rows: {len(df)}
- Total columns: {len(df.columns)}
- Columns:
{col_info_str}

**First 10 rows:**
```
{head_str}
```

**Statistical Summary:**
```
{describe_str}
```

Based on this data, provide exactly 3 key insights in bullet point format. Focus on:
1. The most significant trend or pattern
2. Any notable outliers or anomalies
3. Actionable business recommendations

Format your response as:
• [Insight 1]
• [Insight 2]
• [Insight 3]
"""
        
        # Create prompt template and invoke LLM
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert data analyst who provides clear, actionable insights from data. Be concise and specific."),
            ("human", "{prompt_text}")
        ])
        
        chain = prompt | llm | StrOutputParser()
        insights = chain.invoke({"prompt_text": insights_prompt})
        
        return insights
        
    except Exception as e:
        return f"Error generating insights: {str(e)}"


# =============================================================================
# Test Functions
# =============================================================================

def test_validation():
    """Test the SQL validation layer with various inputs."""
    
    test_cases = [
        # Valid queries
        ("SELECT * FROM customers LIMIT 5", True, "Basic SELECT"),
        ("SELECT name, email FROM customers WHERE region = 'North America'", True, "SELECT with WHERE"),
        ("SELECT COUNT(*) FROM orders WHERE status = 'completed'", True, "Aggregate function"),
        ("SELECT c.name, o.total_amount FROM customers c JOIN orders o ON c.id = o.customer_id", True, "JOIN query"),
        ("SELECT category, SUM(price) as total FROM products GROUP BY category", True, "GROUP BY"),
        
        # Invalid queries - blocked keywords
        ("DROP TABLE customers", False, "DROP statement"),
        ("DELETE FROM orders WHERE id = 1", False, "DELETE statement"),
        ("UPDATE customers SET email = 'hacked@evil.com'", False, "UPDATE statement"),
        ("INSERT INTO orders (customer_id, total_amount) VALUES (1, 0)", False, "INSERT statement"),
        ("TRUNCATE TABLE orders", False, "TRUNCATE statement"),
        
        # Invalid queries - injection attempts
        ("SELECT * FROM customers; DROP TABLE customers;--", False, "Multiple statements"),
        ("SELECT * FROM customers WHERE id = 1 /* comment */", False, "Block comment"),
        ("SELECT * FROM customers --", False, "Line comment"),
        
        # Invalid queries - unknown tables
        ("SELECT * FROM secret_table", False, "Unknown table"),
        ("SELECT * FROM users", False, "Non-existent table"),
        
        # Edge cases
        ("", False, "Empty query"),
        ("   ", False, "Whitespace only"),
    ]
    
    print("\n" + "=" * 80)
    print("SQL VALIDATION TESTS")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    for query, expected_valid, description in test_cases:
        result = validate_sql(query)
        
        if result.is_valid == expected_valid:
            status = "✓ PASS"
            passed += 1
        else:
            status = "✗ FAIL"
            failed += 1
        
        query_preview = query[:40] + "..." if len(query) > 40 else query
        print(f"\n{status}: {description}")
        print(f"   Query: {query_preview}")
        print(f"   Expected: {expected_valid}, Got: {result.is_valid}")
        if not result.is_valid and result.error_message:
            print(f"   Reason: {result.error_message}")
    
    print("\n" + "=" * 80)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 80)
    
    return failed == 0


def test_sql_generation():
    """Test SQL generation with configured LLM."""
    print("\n" + "=" * 80)
    print("SQL GENERATION TEST")
    print("=" * 80)
    
    # Check for any API key configuration
    config = LLMConfig.from_env()
    
    if not config.api_key:
        print("\n⚠️  No LLM API key configured.")
        print("\n   To use OpenAI:")
        print("      export OPENAI_API_KEY='your-key-here'")
        print("\n   To use a custom endpoint (Ollama, vLLM, etc.):")
        print("      export LLM_API_KEY='your-key'")
        print("      export LLM_BASE_URL='http://localhost:11434/v1'")
        print("      export LLM_MODEL='llama3'")
        print("\n   Or use a predefined provider:")
        print("      result = run_query('question', provider='ollama')")
        return False
    
    print(f"\n✓ Using: {config.get_provider_info()}")
    print(f"  Model: {config.model}")
    
    test_questions = [
        "Show me the top 5 customers by total order amount",
        "What is the total revenue by product category?",
        "How many orders were placed in each region?",
        "List all products with low stock (less than 50 units)",
    ]
    
    for question in test_questions:
        print(f"\n--- Question: {question} ---")
        result = run_query(question, config=config)
        print(format_result_summary(result))
    
    return True


def show_llm_config():
    """Display current LLM and database configuration."""
    print("\n" + "=" * 80)
    print("CONFIGURATION")
    print("=" * 80)
    
    # Database config
    print(f"\n  Database Type: {DB_TYPE.value}")
    if DB_TYPE == DatabaseType.POSTGRESQL:
        from database_setup import PG_HOST, PG_PORT, PG_NAME, PG_USER
        print(f"  Database Host: {PG_HOST}:{PG_PORT}")
        print(f"  Database Name: {PG_NAME}")
        print(f"  Database User: {PG_USER}")
    
    # LLM config
    config = LLMConfig.from_env()
    
    print(f"\n  LLM Provider: {config.get_provider_info()}")
    print(f"  LLM Model: {config.model}")
    print(f"  Temperature: {config.temperature}")
    print(f"  Max Tokens: {config.max_tokens}")
    print(f"  API Key: {'✓ Set' if config.api_key else '✗ Not set'}")
    print(f"  Base URL: {config.base_url or 'Default (OpenAI)'}")
    
    # Few-shot config
    print(f"\n  Few-Shot Prompting:")
    print(f"    Dynamic Examples: {'Enabled' if ENABLE_DYNAMIC_EXAMPLES else 'Disabled'}")
    print(f"    Number of Examples: {NUM_FEW_SHOT_EXAMPLES}")
    print(f"    Min Similarity: {MIN_EXAMPLE_SIMILARITY}")
    examples = get_few_shot_examples()
    print(f"    Total Examples Available: {len(examples)}")
    
    print("\n  Predefined LLM Providers:")
    for name, cfg in LLM_PROVIDERS.items():
        print(f"    - {name}: {cfg.base_url or 'OpenAI default'}")
    
    print("\n  Environment Variables:")
    print("    DB_TYPE - 'sqlite' or 'postgresql'")
    print("    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD - PostgreSQL connection")
    print("    LLM_API_KEY / OPENAI_API_KEY - API key")
    print("    LLM_BASE_URL - Custom endpoint URL")
    print("    LLM_MODEL - Model name")
    print("    ENABLE_DYNAMIC_EXAMPLES - Enable semantic similarity (true/false)")
    print("    NUM_FEW_SHOT_EXAMPLES - Number of examples in prompt (default: 3)")
    print("    MIN_EXAMPLE_SIMILARITY - Minimum similarity threshold (default: 0.3)")


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import sys
    
    print("=" * 80)
    print("NL-BI Dashboard - SQL Chain Module Tests")
    print("=" * 80)
    
    # Show configuration
    show_llm_config()
    
    # Run validation tests
    all_passed = test_validation()
    
    # Show schema info
    print("\n" + "=" * 80)
    print("SCHEMA INFORMATION FOR LLM")
    print("=" * 80)
    print(get_schema_for_prompt())
    
    # Run SQL generation tests if API key available
    test_sql_generation()
    
    sys.exit(0 if all_passed else 1)
