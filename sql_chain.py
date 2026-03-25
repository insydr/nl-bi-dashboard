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

Security Architecture (Defense in Depth):
1. SQL Parser Validation (sqlparse) - Validates SQL structure
2. Keyword Blocklist (regex) - Blocks destructive commands
3. Statement Type Check - Only SELECT statements allowed
4. Schema Allow-List - Only permitted tables/columns
5. Read-Only DB Connection - SQLite URI mode enforces read-only
"""

import re
import sqlparse
import pandas as pd
import os
from typing import Optional, Tuple, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

# LangChain imports
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate, MessagesPlaceholder
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
    get_db_connection,
    get_schema_for_prompt,
    ALLOWED_TABLES,
    BLOCKED_KEYWORDS,
    DB_PATH
)


# =============================================================================
# Configuration & Constants
# =============================================================================

# Maximum retries for SQL self-correction (per PRD FR-10)
MAX_RETRIES = 2

# SQL statement types that are allowed
ALLOWED_STATEMENT_TYPES = {"SELECT"}


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
    
    # Validate tables against allow-list
    allowed_tables_lower = [t.lower() for t in ALLOWED_TABLES.keys()]
    
    for table in detected_tables:
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

# System prompt for SQL generation
SQL_SYSTEM_PROMPT = """You are an expert SQL assistant for an e-commerce database. Your task is to translate natural language questions into valid SQLite queries.

{schema_info}

CRITICAL SECURITY RULES:
1. ONLY generate SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, or any data modification commands.
2. Use ONLY the tables and columns shown in the schema above.
3. For date comparisons, use SQLite date format (YYYY-MM-DD).
4. Return ONLY the SQL query without any explanation, markdown formatting, or code blocks.
5. Do not use subqueries that attempt to access system tables.
6. Use proper JOIN syntax when combining data from multiple tables.
7. Use meaningful column aliases for calculated fields.
8. For aggregations, always include appropriate GROUP BY clauses.

DATABASE-SPECIFIC NOTES:
- This is SQLite, not PostgreSQL or MySQL
- Date functions: Use date(), strftime(), datetime() for date operations
- String functions: Use || for concatenation, LIKE for pattern matching
- Boolean values: SQLite uses 0 and 1 for false/true

When the user asks a question, generate a single, valid SQLite SELECT query that answers it.
Return ONLY the SQL query - no explanations, no markdown, no code blocks."""


def get_llm(api_key: Optional[str] = None, model: str = "gpt-4o", temperature: float = 0.0) -> BaseChatModel:
    """
    Get the LLM instance for SQL generation.
    
    This function creates a LangChain ChatOpenAI instance configured for
    SQL generation tasks. You can configure the model and API key.
    
    Args:
        api_key: OpenAI API key (will use OPENAI_API_KEY env var if not provided)
        model: Model name (default: gpt-4o)
        temperature: Temperature for generation (default: 0.0 for consistency)
        
    Returns:
        Configured ChatModel instance
    """
    # Use environment variable if no key provided
    if api_key is None:
        api_key = os.environ.get("OPENAI_API_KEY")
    
    if api_key is None:
        raise ValueError(
            "No API key provided. Set OPENAI_API_KEY environment variable "
            "or pass api_key parameter."
        )
    
    return ChatOpenAI(
        model=model,
        temperature=temperature,
        api_key=api_key,
        max_tokens=2000,
    )


def create_sql_generation_prompt() -> ChatPromptTemplate:
    """Create the prompt template for SQL generation."""
    
    schema_info = get_schema_for_prompt()
    system_content = SQL_SYSTEM_PROMPT.format(schema_info=schema_info)
    
    return ChatPromptTemplate.from_messages([
        ("system", system_content),
        ("human", "{question}")
    ])


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
    
    Args:
        sql: The validated SQL query string
        
    Returns:
        Tuple of (success, dataframe, error_message)
    """
    try:
        conn = get_db_connection(read_only=True)
        
        # Execute query
        df = pd.read_sql_query(sql, conn)
        conn.close()
        
        return True, df, ""
        
    except Exception as e:
        return False, pd.DataFrame(), str(e)


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


# =============================================================================
# Main Query Function
# =============================================================================

def run_query(
    user_question: str,
    llm: Optional[BaseChatModel] = None,
    api_key: Optional[str] = None,
    model: str = "gpt-4o"
) -> QueryResult:
    """
    Main entry point for natural language to SQL query execution.
    
    This function orchestrates the full query pipeline:
    1. Generate SQL from natural language using LLM
    2. Validate the generated SQL for security
    3. Execute the query safely
    4. Retry with error feedback if needed (max 2 retries per PRD FR-10)
    
    Args:
        user_question: The natural language question from the user
        llm: Optional pre-configured LLM instance
        api_key: OpenAI API key (optional, uses env var if not provided)
        model: Model name to use if creating new LLM instance
        
    Returns:
        QueryResult containing the SQL, dataframe, and status
        
    Example:
        >>> result = run_query("What were total sales by region last month?")
        >>> if result.success:
        ...     print(result.sql_query)
        ...     print(result.dataframe.head())
    """
    # Initialize LLM if not provided
    if llm is None:
        try:
            llm = get_llm(api_key=api_key, model=model)
        except ValueError as e:
            return QueryResult(
                success=False,
                error_message=str(e)
            )
    
    # Create the prompt template
    prompt = create_sql_generation_prompt()
    
    retry_count = 0
    last_error = ""
    last_sql = ""
    
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
                retry_prompt = ChatPromptTemplate.from_messages([
                    ("system", SQL_SYSTEM_PROMPT.format(schema_info=get_schema_for_prompt())),
                    ("human", """Previous attempt generated this SQL:
```sql
{last_sql}
```

This failed with error: {error}

Please generate a CORRECTED SQL query for: {question}

Remember:
1. Only SELECT statements allowed
2. Use exact table/column names from schema
3. Valid SQLite syntax only"""),
                ])
                
                chain = retry_prompt | llm | StrOutputParser()
                raw_response = chain.invoke({
                    "question": user_question,
                    "last_sql": last_sql,
                    "error": last_error
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
                return QueryResult(
                    success=True,
                    sql_query=validation_result.cleaned_sql,
                    dataframe=df,
                    retry_count=retry_count,
                    validation_details=validation_result
                )
            else:
                # Execution failed - prepare for retry
                last_error = error
                retry_count += 1
                
                if retry_count > MAX_RETRIES:
                    return QueryResult(
                        success=False,
                        sql_query=sql_query,
                        error_message=f"Query execution failed after {MAX_RETRIES} retries. Last error: {error}",
                        retry_count=retry_count,
                        validation_details=validation_result
                    )
                
        except Exception as e:
            retry_count += 1
            last_error = str(e)
            
            if retry_count > MAX_RETRIES:
                return QueryResult(
                    success=False,
                    error_message=f"Unexpected error after {MAX_RETRIES} retries: {str(e)}",
                    retry_count=retry_count
                )
    
    # Should not reach here, but return failure if we do
    return QueryResult(
        success=False,
        error_message="Maximum retries exceeded",
        retry_count=retry_count
    )


# =============================================================================
# Utility Functions
# =============================================================================

def get_table_info() -> str:
    """
    Get formatted table information for debugging/display.
    """
    db = SQLDatabase.from_uri(f"sqlite:///{DB_PATH}")
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
    """Test SQL generation (requires API key)."""
    print("\n" + "=" * 80)
    print("SQL GENERATION TEST (requires OPENAI_API_KEY)")
    print("=" * 80)
    
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("\n⚠️  OPENAI_API_KEY not set. Skipping SQL generation tests.")
        print("   Set the environment variable to run these tests:")
        print("   export OPENAI_API_KEY='your-key-here'")
        return False
    
    test_questions = [
        "Show me the top 5 customers by total order amount",
        "What is the total revenue by product category?",
        "How many orders were placed in each region?",
        "List all products with low stock (less than 50 units)",
    ]
    
    for question in test_questions:
        print(f"\n--- Question: {question} ---")
        result = run_query(question)
        print(format_result_summary(result))
    
    return True


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import sys
    
    print("=" * 80)
    print("NL-BI Dashboard - SQL Chain Module Tests")
    print("=" * 80)
    
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
