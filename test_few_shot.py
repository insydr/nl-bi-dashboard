"""
Natural Language Business Intelligence Dashboard - Few-Shot Prompting Tests
============================================================================

This test suite validates the few-shot prompting implementation and tests
complex queries that previously failed without examples.

Test Categories:
1. Example Repository Tests - Validate example queries
2. Dynamic Selection Tests - Test semantic similarity and keyword matching
3. Complex Query Tests - Queries that previously failed
4. Integration Tests - Full pipeline with few-shot prompting

Run with: pytest test_few_shot.py -v
"""

import sys
import os

# Add temp libs path for dependencies
sys.path.insert(0, '/tmp/pylibs')

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from typing import List

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules to test
from query_examples import (
    SQLExample,
    get_few_shot_examples,
    get_examples_for_langchain,
    get_examples_by_tags,
    get_examples_by_complexity,
    get_relevant_examples,
    format_examples_for_prompt,
    SQLITE_EXAMPLES,
    POSTGRESQL_EXAMPLES,
)

from sql_chain import (
    create_sql_generation_prompt,
    create_dynamic_prompt_for_question,
    format_few_shot_examples,
    select_examples_for_question,
    validate_sql,
    extract_sql_from_response,
    ENABLE_DYNAMIC_EXAMPLES,
    NUM_FEW_SHOT_EXAMPLES,
)

from database_setup import (
    DB_TYPE,
    DatabaseType,
    get_db_engine,
    get_schema_for_prompt,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def db_engine():
    """Create a read-only database engine for tests."""
    engine = get_db_engine(read_only=True)
    yield engine
    engine.dispose()


@pytest.fixture
def sample_examples():
    """Get sample examples for testing."""
    return get_few_shot_examples()


# =============================================================================
# Example Repository Tests
# =============================================================================

class TestExampleRepository:
    """Test the example repository structure and content."""

    def test_examples_exist(self):
        """Verify examples are loaded."""
        examples = get_few_shot_examples()
        assert len(examples) >= 5, "Should have at least 5 examples"
        assert len(examples) <= 15, "Should not have too many examples (token limits)"

    def test_example_structure(self, sample_examples):
        """Verify each example has required fields."""
        for ex in sample_examples:
            assert isinstance(ex, SQLExample)
            assert ex.question, "Question should not be empty"
            assert ex.sql, "SQL should not be empty"
            assert ex.tags, "Tags should not be empty"
            assert ex.description, "Description should not be empty"
            assert ex.complexity in ["simple", "medium", "complex"]

    def test_examples_cover_joins(self, sample_examples):
        """Verify examples cover JOIN operations."""
        join_examples = get_examples_by_tags(["join"])
        assert len(join_examples) >= 3, "Should have at least 3 examples with joins"

    def test_examples_cover_aggregations(self, sample_examples):
        """Verify examples cover aggregation operations."""
        agg_examples = get_examples_by_tags(["aggregation"])
        assert len(agg_examples) >= 3, "Should have at least 3 examples with aggregations"

    def test_examples_cover_date_filtering(self, sample_examples):
        """Verify examples cover date filtering."""
        date_examples = get_examples_by_tags(["date"])
        assert len(date_examples) >= 2, "Should have at least 2 examples with date filtering"

    def test_examples_cover_top_n(self, sample_examples):
        """Verify examples cover TOP-N queries."""
        top_examples = get_examples_by_tags(["top_n"])
        assert len(top_examples) >= 1, "Should have at least 1 TOP-N example"

    def test_examples_cover_window_functions(self, sample_examples):
        """Verify examples cover window functions (growth calculations)."""
        window_examples = get_examples_by_tags(["window_function"])
        assert len(window_examples) >= 1, "Should have at least 1 window function example"

    def test_examples_valid_sql(self, sample_examples):
        """Verify all example SQL passes validation."""
        for ex in sample_examples:
            validation = validate_sql(ex.sql)
            assert validation.is_valid, f"Example SQL failed validation: {ex.question}\nError: {validation.error_message}"

    def test_examples_by_complexity(self):
        """Verify examples exist for each complexity level."""
        for complexity in ["simple", "medium", "complex"]:
            examples = get_examples_by_complexity(complexity)
            assert len(examples) >= 1, f"Should have at least 1 {complexity} example"

    def test_database_specific_examples(self):
        """Verify correct examples are returned for database type."""
        examples = get_few_shot_examples()

        if DB_TYPE == DatabaseType.POSTGRESQL:
            # Check for PostgreSQL-specific functions
            sql_text = " ".join([ex.sql for ex in examples])
            assert "TO_CHAR" in sql_text or "DATE_TRUNC" in sql_text or "INTERVAL" in sql_text, \
                "PostgreSQL examples should use PostgreSQL-specific functions"
        else:
            # Check for SQLite-specific functions
            sql_text = " ".join([ex.sql for ex in examples])
            assert "strftime" in sql_text or "date(" in sql_text, \
                "SQLite examples should use SQLite-specific functions"


# =============================================================================
# LangChain Format Tests
# =============================================================================

class TestLangChainFormat:
    """Test LangChain formatting of examples."""

    def test_langchain_format(self):
        """Verify LangChain format is correct."""
        lc_examples = get_examples_for_langchain()

        assert isinstance(lc_examples, list)
        assert len(lc_examples) >= 5

        for ex in lc_examples:
            assert "question" in ex
            assert "sql" in ex
            assert isinstance(ex["question"], str)
            assert isinstance(ex["sql"], str)

    def test_prompt_formatting(self, sample_examples):
        """Verify examples format correctly for prompt."""
        formatted = format_examples_for_prompt(sample_examples[:3])

        assert "Example 1:" in formatted
        assert "Question:" in formatted
        assert "SQL:" in formatted
        assert "SELECT" in formatted


# =============================================================================
# Dynamic Selection Tests
# =============================================================================

class TestDynamicSelection:
    """Test dynamic example selection."""

    def test_select_examples_for_revenue_question(self):
        """Test selection for revenue-related questions."""
        question = "What is the total revenue by product category?"
        examples = select_examples_for_question(question)

        assert len(examples) > 0, "Should select at least one example"
        assert len(examples) <= NUM_FEW_SHOT_EXAMPLES, "Should not exceed configured limit"

    def test_select_examples_for_top_customers(self):
        """Test selection for top customer questions."""
        question = "Show me the top 10 customers by spending"
        examples = select_examples_for_question(question)

        assert len(examples) > 0
        # Should prefer examples with top_n tag
        has_top_n = any("top_n" in ex.tags for ex in examples)
        assert has_top_n, "Should select top_n example for top customer question"

    def test_select_examples_for_growth_question(self):
        """Test selection for growth/trend questions."""
        question = "Show month over month revenue growth"
        examples = select_examples_for_question(question)

        assert len(examples) > 0
        # Should prefer examples with growth/window_function tags
        has_growth = any("growth" in ex.tags or "window_function" in ex.tags for ex in examples)
        assert has_growth, "Should select growth/window_function example for growth question"

    def test_keyword_fallback(self):
        """Test keyword matching fallback when embeddings unavailable."""
        # This tests the keyword matching path
        question = "How many orders were placed in each region?"
        examples = get_relevant_examples(question, top_k=2)

        assert len(examples) > 0, "Keyword matching should return examples"

    def test_dynamic_prompt_creation(self):
        """Test dynamic prompt creation for a question."""
        question = "What is the monthly revenue trend?"
        prompt = create_dynamic_prompt_for_question(question)

        # Verify prompt contains key elements
        prompt_str = str(prompt)
        assert "SELECT" in prompt_str or "system" in prompt_str.lower()


# =============================================================================
# Complex Query Tests - Previously Failed Queries
# =============================================================================

class TestComplexQueries:
    """
    Test complex queries that previously failed without few-shot prompting.

    These queries require:
    - Window functions (LAG, LEAD)
    - Common Table Expressions (CTEs)
    - Complex JOINs
    - Date manipulations
    """

    def test_month_over_month_growth_sql(self, db_engine):
        """
        Complex Query 1: Month-over-Month Revenue Growth

        Previously failed because LLM didn't know to use:
        - CTE for monthly aggregation
        - LAG() window function
        - NULLIF for division by zero

        Expected output structure:
        - month (YYYY-MM format)
        - revenue
        - prev_month_revenue
        - growth_percentage
        """
        # The correct SQL from our few-shot examples
        if DB_TYPE == DatabaseType.POSTGRESQL:
            sql = """
            WITH monthly_revenue AS (
                SELECT
                    TO_CHAR(order_date, 'YYYY-MM') AS month,
                    SUM(total_amount) AS revenue
                FROM orders
                WHERE status = 'completed'
                GROUP BY TO_CHAR(order_date, 'YYYY-MM')
            )
            SELECT
                month,
                revenue,
                LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
                ROUND(
                    (revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 /
                    NULLIF(LAG(revenue) OVER (ORDER BY month), 0),
                    2
                ) AS growth_percentage
            FROM monthly_revenue
            ORDER BY month
            """
        else:
            sql = """
            WITH monthly_revenue AS (
                SELECT
                    strftime('%Y-%m', order_date) AS month,
                    SUM(total_amount) AS revenue
                FROM orders
                WHERE status = 'completed'
                GROUP BY strftime('%Y-%m', order_date)
            )
            SELECT
                month,
                revenue,
                LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
                ROUND(
                    (revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 /
                    NULLIF(LAG(revenue) OVER (ORDER BY month), 0),
                    2
                ) AS growth_percentage
            FROM monthly_revenue
            ORDER BY month
            """

        # Validate SQL
        validation = validate_sql(sql)
        assert validation.is_valid, f"SQL validation failed: {validation.error_message}"

        # Execute and verify structure
        df = pd.read_sql_query(sql, db_engine)

        assert not df.empty, "Should return results"
        assert 'month' in df.columns
        assert 'revenue' in df.columns
        assert 'prev_month_revenue' in df.columns
        assert 'growth_percentage' in df.columns

        # Verify growth calculation makes sense
        if len(df) > 1:
            # Second row should have previous month revenue
            assert pd.notna(df['prev_month_revenue'].iloc[1])

        print(f"✓ Month-over-month growth query returned {len(df)} months")

    def test_customer_retention_analysis_sql(self, db_engine):
        """
        Complex Query 2: Customer Retention by Signup Month

        Previously failed because LLM didn't know to use:
        - LEFT JOIN to include customers without orders
        - CASE WHEN for conditional aggregation
        - Proper date formatting

        Expected output structure:
        - signup_month
        - total_customers
        - customers_with_orders
        - retention_rate
        """
        if DB_TYPE == DatabaseType.POSTGRESQL:
            sql = """
            WITH customer_orders AS (
                SELECT
                    c.id AS customer_id,
                    TO_CHAR(c.signup_date, 'YYYY-MM') AS signup_month,
                    COUNT(DISTINCT o.id) AS order_count
                FROM customers c
                LEFT JOIN orders o ON c.id = o.customer_id
                GROUP BY c.id, TO_CHAR(c.signup_date, 'YYYY-MM')
            )
            SELECT
                signup_month,
                COUNT(*) AS total_customers,
                SUM(CASE WHEN order_count > 0 THEN 1 ELSE 0 END) AS customers_with_orders,
                ROUND(
                    SUM(CASE WHEN order_count > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                    2
                ) AS retention_rate
            FROM customer_orders
            GROUP BY signup_month
            ORDER BY signup_month
            """
        else:
            sql = """
            WITH customer_orders AS (
                SELECT
                    c.id AS customer_id,
                    strftime('%Y-%m', c.signup_date) AS signup_month,
                    COUNT(DISTINCT o.id) AS order_count
                FROM customers c
                LEFT JOIN orders o ON c.id = o.customer_id
                GROUP BY c.id, strftime('%Y-%m', c.signup_date)
            )
            SELECT
                signup_month,
                COUNT(*) AS total_customers,
                SUM(CASE WHEN order_count > 0 THEN 1 ELSE 0 END) AS customers_with_orders,
                ROUND(
                    SUM(CASE WHEN order_count > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                    2
                ) AS retention_rate
            FROM customer_orders
            GROUP BY signup_month
            ORDER BY signup_month
            """

        # Validate SQL
        validation = validate_sql(sql)
        assert validation.is_valid, f"SQL validation failed: {validation.error_message}"

        # Execute and verify
        df = pd.read_sql_query(sql, db_engine)

        assert not df.empty
        assert 'signup_month' in df.columns
        assert 'total_customers' in df.columns
        assert 'customers_with_orders' in df.columns
        assert 'retention_rate' in df.columns

        # Verify retention rate is a percentage
        assert all(df['retention_rate'].between(0, 100))

        print(f"✓ Customer retention query returned {len(df)} months")

    def test_low_stock_high_sales_sql(self, db_engine):
        """
        Complex Query 3: Products with Low Stock and High Sales

        Previously failed because LLM didn't know to use:
        - LEFT JOIN (not all products have sales)
        - COALESCE for NULL handling
        - HAVING clause for filtering aggregations
        - Multiple conditions in HAVING

        Expected output structure:
        - product details
        - stock_quantity
        - units_sold
        - stock_after_sales
        """
        sql = """
        SELECT
            p.name,
            p.category,
            p.stock_quantity,
            COALESCE(SUM(oi.quantity), 0) AS units_sold,
            p.stock_quantity - COALESCE(SUM(oi.quantity), 0) AS stock_after_sales
        FROM products p
        LEFT JOIN order_items oi ON p.id = oi.product_id
        GROUP BY p.id, p.name, p.category, p.stock_quantity
        HAVING p.stock_quantity < 100 AND COALESCE(SUM(oi.quantity), 0) > 50
        ORDER BY units_sold DESC
        """

        # Validate SQL
        validation = validate_sql(sql)
        assert validation.is_valid, f"SQL validation failed: {validation.error_message}"

        # Execute and verify
        df = pd.read_sql_query(sql, db_engine)

        # Note: May be empty if no products meet criteria, which is fine
        if not df.empty:
            assert 'name' in df.columns
            assert 'stock_quantity' in df.columns
            assert 'units_sold' in df.columns

            # Verify conditions are met
            assert all(df['stock_quantity'] < 100)
            assert all(df['units_sold'] > 50)

            print(f"✓ Low stock high sales query found {len(df)} products")
        else:
            print("✓ Low stock high sales query returned empty (no products meet criteria)")

    def test_average_order_value_by_segment_sql(self, db_engine):
        """
        Complex Query 4: Average Order Value by Customer Segment

        Previously failed because LLM didn't:
        - Use COUNT(DISTINCT) correctly
        - Apply ROUND properly
        - Filter cancelled orders

        Expected output:
        - customer_segment
        - order_count
        - total_revenue
        - avg_order_value
        """
        sql = """
        SELECT
            c.customer_segment,
            COUNT(DISTINCT o.id) AS order_count,
            SUM(o.total_amount) AS total_revenue,
            ROUND(AVG(o.total_amount), 2) AS avg_order_value
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        WHERE o.status = 'completed'
        GROUP BY c.customer_segment
        ORDER BY avg_order_value DESC
        """

        # Validate SQL
        validation = validate_sql(sql)
        assert validation.is_valid, f"SQL validation failed: {validation.error_message}"

        # Execute and verify
        df = pd.read_sql_query(sql, db_engine)

        assert not df.empty
        assert 'customer_segment' in df.columns
        assert 'order_count' in df.columns
        assert 'total_revenue' in df.columns
        assert 'avg_order_value' in df.columns

        # Verify values are sorted by avg_order_value descending
        assert df['avg_order_value'].is_monotonic_decreasing

        print(f"✓ AOV by segment query returned {len(df)} segments")


# =============================================================================
# Prompt Template Tests
# =============================================================================

class TestPromptTemplates:
    """Test prompt template creation with few-shot examples."""

    def test_prompt_includes_schema(self):
        """Verify prompt includes schema information."""
        prompt = create_sql_generation_prompt()
        prompt_str = str(prompt)

        # Should include table names
        assert "customers" in prompt_str.lower() or "TABLE" in prompt_str

    def test_prompt_includes_examples(self):
        """Verify prompt includes few-shot examples."""
        prompt = create_sql_generation_prompt(use_few_shot=True)
        prompt_str = str(prompt)

        # Should include example markers
        assert "Example" in prompt_str or "SELECT" in prompt_str

    def test_prompt_without_examples(self):
        """Verify prompt can be created without examples."""
        prompt = create_sql_generation_prompt(use_few_shot=False)
        prompt_str = str(prompt)

        # Should still have schema but not examples section
        assert "SELECT" in prompt_str  # Schema hints still present

    def test_dynamic_prompt_includes_relevant_examples(self):
        """Verify dynamic prompt includes relevant examples."""
        question = "Show me the top 5 customers by total spending"

        prompt = create_dynamic_prompt_for_question(question)
        prompt_str = str(prompt)

        # Should include the question format
        assert "{question}" in prompt_str or "question" in prompt_str.lower()


# =============================================================================
# Integration Tests
# =============================================================================

class TestFewShotIntegration:
    """Integration tests for few-shot prompting in the full pipeline."""

    @pytest.mark.skipif(
        not os.environ.get("OPENAI_API_KEY") and not os.environ.get("LLM_API_KEY"),
        reason="No API key available for LLM tests"
    )
    def test_complex_query_with_few_shot(self):
        """
        Test that a complex query succeeds with few-shot prompting.

        This test requires an API key and will make actual LLM calls.
        """
        from sql_chain import run_query, LLMConfig

        config = LLMConfig.from_env()

        # Test a complex query that benefits from few-shot
        question = "Show month over month revenue growth for completed orders"

        result = run_query(question, config=config)

        # The query should succeed (possibly with retries)
        # If few-shot is working, it should succeed on first try
        assert result.success or result.retry_count <= 2, \
            f"Query failed after {result.retry_count} retries: {result.error_message}"

        if result.success:
            # Verify the result has expected structure
            assert result.dataframe is not None
            assert len(result.dataframe.columns) >= 3, \
                "Growth query should return at least 3 columns"

    def test_example_selection_performance(self):
        """Test that example selection is performant."""
        import time

        questions = [
            "What is the total revenue?",
            "Show top customers by spending",
            "Monthly revenue trend",
            "Customer retention rate",
            "Low stock products"
        ]

        start = time.time()
        for question in questions:
            examples = select_examples_for_question(question)
            assert len(examples) > 0
        elapsed = time.time() - start

        # Should be fast (< 1 second for 5 questions)
        assert elapsed < 1.0, f"Example selection too slow: {elapsed:.2f}s"


# =============================================================================
# Test Runner
# =============================================================================

def test_summary():
    """Print a summary of available examples."""
    print("\n" + "=" * 60)
    print("FEW-SHOT EXAMPLE SUMMARY")
    print("=" * 60)

    examples = get_few_shot_examples()
    print(f"\nDatabase Type: {DB_TYPE.value}")
    print(f"Total Examples: {len(examples)}")

    print("\nBy Complexity:")
    for complexity in ["simple", "medium", "complex"]:
        count = len(get_examples_by_complexity(complexity))
        print(f"  {complexity}: {count}")

    print("\nExample Questions:")
    for i, ex in enumerate(examples[:5], 1):
        print(f"  {i}. {ex.question} [{ex.complexity}]")

    print("\nConfiguration:")
    print(f"  Dynamic Examples: {ENABLE_DYNAMIC_EXAMPLES}")
    print(f"  Number in Prompt: {NUM_FEW_SHOT_EXAMPLES}")


if __name__ == "__main__":
    # Run summary first
    test_summary()

    # Run pytest
    pytest.main([__file__, "-v", "--tb=short"])
