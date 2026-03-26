"""
Natural Language Business Intelligence Dashboard - Query Examples
==================================================================

This module provides few-shot examples for the SQL generation LLM.
These examples help guide the LLM to generate correct SQL for complex
business queries, reducing errors and improving accuracy.

Features:
- Curated examples covering common business questions
- Support for both SQLite and PostgreSQL syntax
- Semantic similarity-based example selection (optional)
- Examples covering joins, aggregations, date filtering, and window functions

Example Categories:
1. Simple aggregations
2. Multi-table joins
3. Date-based filtering and grouping
4. Ranking and top-N queries
5. Month-over-month comparisons (window functions)
6. Complex business logic

Usage:
    from query_examples import get_few_shot_examples, get_relevant_examples

    # Get all examples
    examples = get_few_shot_examples()

    # Get examples relevant to a specific question
    relevant = get_relevant_examples("show me top customers by revenue")
"""

import os
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

# Database type detection
from database_setup import DB_TYPE, DatabaseType


# =============================================================================
# Example Data Structures
# =============================================================================

@dataclass
class SQLExample:
    """
    Represents a single few-shot example for SQL generation.

    Attributes:
        question: Natural language question from user
        sql: Correct SQL query that answers the question
        tags: List of tags for categorization (e.g., 'join', 'aggregation', 'date')
        description: Brief explanation of the SQL technique used
        complexity: 'simple', 'medium', or 'complex'
    """
    question: str
    sql: str
    tags: List[str]
    description: str
    complexity: str = "medium"


# =============================================================================
# SQLite Examples
# =============================================================================

SQLITE_EXAMPLES: List[SQLExample] = [
    # =========================================================================
    # Simple Aggregations
    # =========================================================================
    SQLExample(
        question="What is the total revenue by product category?",
        sql="""SELECT
    p.category,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.category
ORDER BY total_revenue DESC""",
        tags=["aggregation", "join", "group_by"],
        description="Simple aggregation with JOIN and GROUP BY",
        complexity="simple"
    ),

    SQLExample(
        question="How many orders were placed in each region?",
        sql="""SELECT
    c.region,
    COUNT(o.id) AS order_count
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.region
ORDER BY order_count DESC""",
        tags=["aggregation", "join", "group_by"],
        description="COUNT aggregation with JOIN",
        complexity="simple"
    ),

    # =========================================================================
    # Top-N Queries
    # =========================================================================
    SQLExample(
        question="Show me the top 10 customers by total order amount",
        sql="""SELECT
    c.name,
    c.email,
    SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.status != 'cancelled'
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 10""",
        tags=["top_n", "aggregation", "join", "filtering"],
        description="Top-N query with filtering cancelled orders",
        complexity="simple"
    ),

    SQLExample(
        question="What are the top 5 best-selling products?",
        sql="""SELECT
    p.name,
    p.category,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category
ORDER BY total_units_sold DESC
LIMIT 5""",
        tags=["top_n", "aggregation", "join"],
        description="Top-N products by quantity sold",
        complexity="simple"
    ),

    # =========================================================================
    # Date-Based Queries
    # =========================================================================
    SQLExample(
        question="What is the monthly revenue trend?",
        sql="""SELECT
    strftime('%Y-%m', order_date) AS month,
    SUM(total_amount) AS monthly_revenue
FROM orders
WHERE status = 'completed'
GROUP BY strftime('%Y-%m', order_date)
ORDER BY month""",
        tags=["date", "aggregation", "trend"],
        description="Monthly aggregation using SQLite strftime",
        complexity="medium"
    ),

    SQLExample(
        question="Show me orders from the last 30 days",
        sql="""SELECT
    o.id,
    o.order_date,
    o.total_amount,
    o.status,
    c.name AS customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= date('now', '-30 days')
ORDER BY o.order_date DESC""",
        tags=["date", "join", "filtering"],
        description="Date filtering with SQLite date function",
        complexity="medium"
    ),

    # =========================================================================
    # Month-Over-Month Growth (Window Functions)
    # =========================================================================
    SQLExample(
        question="Show month-over-month revenue growth",
        sql="""WITH monthly_revenue AS (
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
ORDER BY month""",
        tags=["window_function", "cte", "date", "growth"],
        description="Month-over-month growth using window functions and CTE",
        complexity="complex"
    ),

    # =========================================================================
    # Complex Business Queries
    # =========================================================================
    SQLExample(
        question="What is the average order value by customer segment?",
        sql="""SELECT
    c.customer_segment,
    COUNT(DISTINCT o.id) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_segment
ORDER BY avg_order_value DESC""",
        tags=["aggregation", "join", "business_metric"],
        description="Business KPI calculation by segment",
        complexity="medium"
    ),

    SQLExample(
        question="Which products have low stock and high sales?",
        sql="""SELECT
    p.name,
    p.category,
    p.stock_quantity,
    COALESCE(SUM(oi.quantity), 0) AS units_sold,
    p.stock_quantity - COALESCE(SUM(oi.quantity), 0) AS stock_after_sales
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category, p.stock_quantity
HAVING p.stock_quantity < 100 AND COALESCE(SUM(oi.quantity), 0) > 50
ORDER BY units_sold DESC""",
        tags=["join", "aggregation", "having", "business_logic"],
        description="Inventory analysis with HAVING clause",
        complexity="complex"
    ),

    SQLExample(
        question="Show customer retention by signup month",
        sql="""WITH customer_orders AS (
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
ORDER BY signup_month""",
        tags=["cte", "case_when", "aggregation", "retention"],
        description="Customer retention analysis using CTE and CASE",
        complexity="complex"
    ),
]


# =============================================================================
# PostgreSQL Examples
# =============================================================================

POSTGRESQL_EXAMPLES: List[SQLExample] = [
    # =========================================================================
    # Simple Aggregations
    # =========================================================================
    SQLExample(
        question="What is the total revenue by product category?",
        sql="""SELECT
    p.category,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.category
ORDER BY total_revenue DESC""",
        tags=["aggregation", "join", "group_by"],
        description="Simple aggregation with JOIN and GROUP BY",
        complexity="simple"
    ),

    SQLExample(
        question="How many orders were placed in each region?",
        sql="""SELECT
    c.region,
    COUNT(o.id) AS order_count
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.region
ORDER BY order_count DESC""",
        tags=["aggregation", "join", "group_by"],
        description="COUNT aggregation with JOIN",
        complexity="simple"
    ),

    # =========================================================================
    # Top-N Queries
    # =========================================================================
    SQLExample(
        question="Show me the top 10 customers by total order amount",
        sql="""SELECT
    c.name,
    c.email,
    SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.status != 'cancelled'
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 10""",
        tags=["top_n", "aggregation", "join", "filtering"],
        description="Top-N query with filtering cancelled orders",
        complexity="simple"
    ),

    SQLExample(
        question="What are the top 5 best-selling products?",
        sql="""SELECT
    p.name,
    p.category,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category
ORDER BY total_units_sold DESC
LIMIT 5""",
        tags=["top_n", "aggregation", "join"],
        description="Top-N products by quantity sold",
        complexity="simple"
    ),

    # =========================================================================
    # Date-Based Queries (PostgreSQL specific)
    # =========================================================================
    SQLExample(
        question="What is the monthly revenue trend?",
        sql="""SELECT
    TO_CHAR(order_date, 'YYYY-MM') AS month,
    SUM(total_amount) AS monthly_revenue
FROM orders
WHERE status = 'completed'
GROUP BY TO_CHAR(order_date, 'YYYY-MM')
ORDER BY month""",
        tags=["date", "aggregation", "trend"],
        description="Monthly aggregation using PostgreSQL TO_CHAR",
        complexity="medium"
    ),

    SQLExample(
        question="Show me orders from the last 30 days",
        sql="""SELECT
    o.id,
    o.order_date,
    o.total_amount,
    o.status,
    c.name AS customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY o.order_date DESC""",
        tags=["date", "join", "filtering"],
        description="Date filtering with PostgreSQL INTERVAL",
        complexity="medium"
    ),

    # =========================================================================
    # Month-Over-Month Growth (Window Functions)
    # =========================================================================
    SQLExample(
        question="Show month-over-month revenue growth",
        sql="""WITH monthly_revenue AS (
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
ORDER BY month""",
        tags=["window_function", "cte", "date", "growth"],
        description="Month-over-month growth using window functions and CTE",
        complexity="complex"
    ),

    # =========================================================================
    # Complex Business Queries
    # =========================================================================
    SQLExample(
        question="What is the average order value by customer segment?",
        sql="""SELECT
    c.customer_segment,
    COUNT(DISTINCT o.id) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    ROUND(AVG(o.total_amount)::numeric, 2) AS avg_order_value
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_segment
ORDER BY avg_order_value DESC""",
        tags=["aggregation", "join", "business_metric"],
        description="Business KPI calculation by segment",
        complexity="medium"
    ),

    SQLExample(
        question="Which products have low stock and high sales?",
        sql="""SELECT
    p.name,
    p.category,
    p.stock_quantity,
    COALESCE(SUM(oi.quantity), 0) AS units_sold,
    p.stock_quantity - COALESCE(SUM(oi.quantity), 0) AS stock_after_sales
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category, p.stock_quantity
HAVING p.stock_quantity < 100 AND COALESCE(SUM(oi.quantity), 0) > 50
ORDER BY units_sold DESC""",
        tags=["join", "aggregation", "having", "business_logic"],
        description="Inventory analysis with HAVING clause",
        complexity="complex"
    ),

    SQLExample(
        question="Show customer retention by signup month",
        sql="""WITH customer_orders AS (
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
ORDER BY signup_month""",
        tags=["cte", "case_when", "aggregation", "retention"],
        description="Customer retention analysis using CTE and CASE",
        complexity="complex"
    ),
]


# =============================================================================
# Public API
# =============================================================================

def get_few_shot_examples() -> List[SQLExample]:
    """
    Get all few-shot examples for the current database type.

    Returns:
        List of SQLExample objects appropriate for the configured database
    """
    if DB_TYPE == DatabaseType.POSTGRESQL:
        return POSTGRESQL_EXAMPLES
    return SQLITE_EXAMPLES


def get_examples_for_langchain() -> List[Dict[str, str]]:
    """
    Get examples formatted for LangChain FewShotPromptTemplate.

    Returns:
        List of dictionaries with 'question' and 'sql' keys
    """
    examples = get_few_shot_examples()
    return [
        {"question": ex.question, "sql": ex.sql}
        for ex in examples
    ]


def get_examples_by_tags(tags: List[str], match_all: bool = False) -> List[SQLExample]:
    """
    Get examples filtered by tags.

    Args:
        tags: List of tags to filter by
        match_all: If True, example must have all tags; if False, any tag matches

    Returns:
        List of matching SQLExample objects
    """
    examples = get_few_shot_examples()

    if not tags:
        return examples

    matched = []
    for ex in examples:
        if match_all:
            if all(tag in ex.tags for tag in tags):
                matched.append(ex)
        else:
            if any(tag in ex.tags for tag in tags):
                matched.append(ex)

    return matched


def get_examples_by_complexity(complexity: str) -> List[SQLExample]:
    """
    Get examples filtered by complexity level.

    Args:
        complexity: 'simple', 'medium', or 'complex'

    Returns:
        List of SQLExample objects matching the complexity
    """
    examples = get_few_shot_examples()
    return [ex for ex in examples if ex.complexity == complexity]


# =============================================================================
# Semantic Similarity (Optional - requires embeddings)
# =============================================================================

# Cache for embeddings to avoid recomputing
_EMBEDDING_CACHE: Dict[str, List[float]] = {}


def _compute_embedding(text: str) -> Optional[List[float]]:
    """
    Compute embedding for text using available embedding model.

    This is an optional feature that requires an embedding model.
    If no embedding model is available, returns None.

    Args:
        text: Text to embed

    Returns:
        List of floats representing the embedding, or None if unavailable
    """
    try:
        # Try to use OpenAI embeddings if available
        import openai
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
        base_url = os.environ.get("LLM_BASE_URL")

        if not api_key:
            return None

        client = openai.OpenAI(api_key=api_key, base_url=base_url)

        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )

        return response.data[0].embedding

    except Exception:
        return None


def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Compute cosine similarity between two vectors.

    Args:
        vec1: First vector
        vec2: Second vector

    Returns:
        Cosine similarity score between -1 and 1
    """
    if len(vec1) != len(vec2):
        return 0.0

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = sum(a * a for a in vec1) ** 0.5
    norm2 = sum(b * b for b in vec2) ** 0.5

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot_product / (norm1 * norm2)


def get_relevant_examples(
    question: str,
    top_k: int = 3,
    min_similarity: float = 0.5
) -> List[SQLExample]:
    """
    Get the most relevant examples for a given question using semantic similarity.

    This function uses embeddings to find examples that are semantically similar
    to the user's question. If embeddings are not available, falls back to
    keyword matching.

    Args:
        question: User's natural language question
        top_k: Maximum number of examples to return
        min_similarity: Minimum similarity threshold (0 to 1)

    Returns:
        List of most relevant SQLExample objects
    """
    examples = get_few_shot_examples()

    # Try semantic similarity first
    question_embedding = _compute_embedding(question)

    if question_embedding:
        # Use semantic similarity
        scored_examples: List[Tuple[float, SQLExample]] = []

        for ex in examples:
            # Get or compute embedding for example question
            if ex.question not in _EMBEDDING_CACHE:
                _EMBEDDING_CACHE[ex.question] = _compute_embedding(ex.question)

            ex_embedding = _EMBEDDING_CACHE[ex.question]

            if ex_embedding:
                similarity = _cosine_similarity(question_embedding, ex_embedding)
                if similarity >= min_similarity:
                    scored_examples.append((similarity, ex))

        # Sort by similarity (descending) and take top_k
        scored_examples.sort(key=lambda x: x[0], reverse=True)
        return [ex for _, ex in scored_examples[:top_k]]

    # Fallback to keyword matching
    question_lower = question.lower()
    keyword_scores: List[Tuple[int, SQLExample]] = []

    # Define keyword groups for matching
    keyword_groups = {
        "revenue": ["revenue", "sales", "total", "amount", "money"],
        "customer": ["customer", "client", "user", "buyer"],
        "product": ["product", "item", "stock", "inventory"],
        "order": ["order", "purchase", "transaction"],
        "date": ["month", "daily", "weekly", "yearly", "date", "time", "trend"],
        "top": ["top", "best", "highest", "most"],
        "growth": ["growth", "increase", "compare", "over", "change"],
        "segment": ["segment", "category", "region", "group"],
    }

    for ex in examples:
        score = 0
        ex_lower = ex.question.lower()

        for group, keywords in keyword_groups.items():
            if any(kw in question_lower for kw in keywords):
                if any(kw in ex_lower for kw in keywords):
                    score += 1

        if score > 0:
            keyword_scores.append((score, ex))

    # Sort by score (descending) and take top_k
    keyword_scores.sort(key=lambda x: x[0], reverse=True)
    return [ex for _, ex in keyword_scores[:top_k]]


def format_examples_for_prompt(examples: List[SQLExample]) -> str:
    """
    Format examples for inclusion in an LLM prompt.

    Args:
        examples: List of SQLExample objects to format

    Returns:
        Formatted string with examples
    """
    if not examples:
        return ""

    formatted = "\nHere are some example queries to guide you:\n\n"

    for i, ex in enumerate(examples, 1):
        formatted += f"Example {i}:\n"
        formatted += f"Question: {ex.question}\n"
        formatted += f"SQL: {ex.sql}\n\n"

    return formatted


# =============================================================================
# Test Functions
# =============================================================================

def test_examples():
    """Test that all examples are valid."""
    print("=" * 60)
    print("Query Examples Test")
    print("=" * 60)

    print(f"\nDatabase Type: {DB_TYPE.value}")
    print(f"SQLite Examples: {len(SQLITE_EXAMPLES)}")
    print(f"PostgreSQL Examples: {len(POSTGRESQL_EXAMPLES)}")

    examples = get_few_shot_examples()
    print(f"\nActive Examples: {len(examples)}")

    # Test by complexity
    print("\nBy Complexity:")
    for complexity in ["simple", "medium", "complex"]:
        count = len(get_examples_by_complexity(complexity))
        print(f"  {complexity}: {count}")

    # Test by tags
    print("\nBy Tags:")
    all_tags = set()
    for ex in examples:
        all_tags.update(ex.tags)

    for tag in sorted(all_tags):
        count = len(get_examples_by_tags([tag]))
        print(f"  {tag}: {count}")

    # Test LangChain format
    lc_examples = get_examples_for_langchain()
    print(f"\nLangChain Format: {len(lc_examples)} examples ready")

    # Test keyword-based relevance
    print("\nKeyword-based relevance test:")
    test_questions = [
        "Show me monthly revenue",
        "Who are the top customers?",
        "Compare sales growth over time"
    ]

    for q in test_questions:
        relevant = get_relevant_examples(q, top_k=2)
        print(f"\n  Question: '{q}'")
        for ex in relevant:
            print(f"    → {ex.question}")


if __name__ == "__main__":
    test_examples()
