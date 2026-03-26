#!/usr/bin/env python3
"""
NL-BI Dashboard - Cache Performance Benchmark
==============================================

This script measures the performance difference between cached and uncached
queries to validate the caching implementation.

Features:
- Compares first-run (uncached) vs subsequent-run (cached) query times
- Tests multiple query types (simple, complex, with joins)
- Reports cache hit rate and performance improvement
- Validates the <10s latency requirement

Usage:
    python benchmark_cache.py

Environment Variables:
    OPENAI_API_KEY or LLM_API_KEY - Required for LLM queries
    ENABLE_CACHE - Set to 'true' (default) or 'false' to disable caching
"""

import sys
import os
import time
import statistics
from typing import List, Dict, Any, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules
from sql_chain import (
    run_query, LLMConfig, clear_cache, get_cache_stats,
    ENABLE_CACHE, MAX_RETRIES
)
from database_setup import ensure_query_logs_table


# =============================================================================
# Test Questions
# =============================================================================

TEST_QUESTIONS = [
    # Simple queries
    ("What is the total revenue?", "simple"),
    ("How many customers are there?", "simple"),
    ("What is the average order value?", "simple"),
    
    # Medium complexity
    ("Show me the top 5 customers by total order amount", "medium"),
    ("What is the total revenue by product category?", "medium"),
    ("How many orders were placed in each region?", "medium"),
    
    # Complex queries
    ("Show me the monthly revenue trend for the last 6 months", "complex"),
    ("Which products have the highest sales volume?", "complex"),
    ("What is the average order value by customer segment?", "complex"),
    
    # Join queries
    ("Show me customers who have placed orders over $500", "join"),
    ("List products that have never been ordered", "join"),
]


# =============================================================================
# Benchmark Functions
# =============================================================================

def check_llm_config() -> bool:
    """Check if LLM is configured."""
    config = LLMConfig.from_env()
    return config.api_key is not None


def run_benchmark(
    questions: List[Tuple[str, str]],
    runs_per_question: int = 3
) -> Dict[str, Any]:
    """
    Run benchmark comparing cached vs uncached query performance.
    
    Args:
        questions: List of (question, category) tuples
        runs_per_question: Number of times to run each question
        
    Returns:
        Dictionary with benchmark results
    """
    results = {
        "questions": [],
        "total_uncached_time": 0,
        "total_cached_time": 0,
        "total_queries": 0,
        "cache_hits": 0,
        "cache_misses": 0,
    }
    
    print("\n" + "=" * 70)
    print("RUNNING BENCHMARK: Cached vs Uncached Query Performance")
    print("=" * 70)
    
    for question, category in questions:
        print(f"\n--- Testing: {question[:50]}... [{category}] ---")
        
        question_results = {
            "question": question,
            "category": category,
            "uncached_times": [],
            "cached_times": [],
        }
        
        # Clear cache before first run
        clear_cache()
        
        # Run 1: Uncached (first run)
        print("  Run 1 (uncached): ", end="", flush=True)
        start = time.time()
        result = run_query(question)
        uncached_time = time.time() - start
        
        question_results["uncached_times"].append(uncached_time)
        results["total_uncached_time"] += uncached_time
        results["cache_misses"] += 1
        results["total_queries"] += 1
        
        if result.success:
            print(f"{uncached_time*1000:.0f}ms ✓ ({len(result.dataframe)} rows)")
        else:
            print(f"{uncached_time*1000:.0f}ms ✗ ({result.error_message[:50]})")
        
        # Runs 2-N: Cached (subsequent runs)
        for i in range(runs_per_question - 1):
            print(f"  Run {i+2} (cached): ", end="", flush=True)
            start = time.time()
            result = run_query(question)
            cached_time = time.time() - start
            
            question_results["cached_times"].append(cached_time)
            results["total_cached_time"] += cached_time
            results["total_queries"] += 1
            
            if result.from_cache:
                results["cache_hits"] += 1
            
            if result.success:
                print(f"{cached_time*1000:.0f}ms {'📦' if result.from_cache else '🔄'} ({len(result.dataframe)} rows)")
            else:
                print(f"{cached_time*1000:.0f}ms ✗ ({result.error_message[:50]})")
        
        results["questions"].append(question_results)
    
    return results


def print_benchmark_summary(results: Dict[str, Any]) -> None:
    """Print a summary of benchmark results."""
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY")
    print("=" * 70)
    
    # Calculate statistics
    total_uncached = results["total_uncached_time"]
    total_cached = results["total_cached_time"]
    total_queries = results["total_queries"]
    cache_hits = results["cache_hits"]
    cache_misses = results["cache_misses"]
    
    # Calculate per-question averages
    uncached_times = []
    cached_times = []
    
    for q in results["questions"]:
        if q["uncached_times"]:
            uncached_times.extend(q["uncached_times"])
        if q["cached_times"]:
            cached_times.extend(q["cached_times"])
    
    avg_uncached = statistics.mean(uncached_times) if uncached_times else 0
    avg_cached = statistics.mean(cached_times) if cached_times else 0
    
    # Calculate speedup
    speedup = avg_uncached / avg_cached if avg_cached > 0 else 0
    time_saved = total_uncached - total_cached
    hit_rate = (cache_hits / total_queries * 100) if total_queries > 0 else 0
    
    # Print summary
    print(f"\n📊 Total Queries: {total_queries}")
    print(f"📦 Cache Hits: {cache_hits} | Cache Misses: {cache_misses}")
    print(f"📈 Cache Hit Rate: {hit_rate:.1f}%")
    
    print(f"\n⏱️  Performance Metrics:")
    print(f"   Avg Uncached Query: {avg_uncached*1000:.0f}ms")
    print(f"   Avg Cached Query:   {avg_cached*1000:.0f}ms")
    print(f"   Speedup Factor:     {speedup:.1f}x")
    print(f"   Time Saved:         {time_saved*1000:.0f}ms")
    
    # Check latency requirement
    print(f"\n✅ Latency Check:")
    if avg_cached < 10.0:
        print(f"   ✓ Cached queries meet <10s requirement ({avg_cached:.2f}s)")
    else:
        print(f"   ✗ Cached queries exceed 10s ({avg_cached:.2f}s)")
    
    if avg_uncached < 10.0:
        print(f"   ✓ Uncached queries meet <10s requirement ({avg_uncached:.2f}s)")
    else:
        print(f"   ⚠ Uncached queries may exceed 10s ({avg_uncached:.2f}s)")
    
    # Category breakdown
    print(f"\n📋 Performance by Category:")
    categories = {}
    for q in results["questions"]:
        cat = q["category"]
        if cat not in categories:
            categories[cat] = {"uncached": [], "cached": []}
        categories[cat]["uncached"].extend(q["uncached_times"])
        categories[cat]["cached"].extend(q["cached_times"])
    
    for cat, times in sorted(categories.items()):
        avg_u = statistics.mean(times["uncached"]) * 1000 if times["uncached"] else 0
        avg_c = statistics.mean(times["cached"]) * 1000 if times["cached"] else 0
        speedup = avg_u / avg_c if avg_c > 0 else 0
        print(f"   {cat:10s}: {avg_u:6.0f}ms → {avg_c:6.0f}ms ({speedup:.1f}x speedup)")
    
    # Cache stats from internal cache
    cache_stats = get_cache_stats()
    print(f"\n📦 Internal Cache Stats:")
    print(f"   Size: {cache_stats['cache_size']}/{cache_stats['max_entries']}")
    print(f"   Total Queries: {cache_stats['total_queries']}")
    print(f"   Hit Rate: {cache_stats['hit_rate_percent']}%")


def run_single_query_benchmark() -> None:
    """
    Run a simple benchmark showing cached vs uncached for a single query.
    This is useful for quick validation.
    """
    print("\n" + "=" * 70)
    print("SINGLE QUERY BENCHMARK")
    print("=" * 70)
    
    question = "What is the total revenue by product category?"
    
    # Clear cache
    clear_cache()
    
    print(f"\nQuestion: {question}")
    
    # First run (uncached)
    print("\n🔴 First Run (Uncached):")
    start = time.time()
    result1 = run_query(question)
    time1 = time.time() - start
    
    print(f"   Time: {time1*1000:.0f}ms")
    print(f"   Success: {result1.success}")
    print(f"   Rows: {len(result1.dataframe) if result1.dataframe is not None else 0}")
    print(f"   From Cache: {result1.from_cache}")
    
    # Second run (should be cached)
    print("\n🟢 Second Run (Cached):")
    start = time.time()
    result2 = run_query(question)
    time2 = time.time() - start
    
    print(f"   Time: {time2*1000:.0f}ms")
    print(f"   Success: {result2.success}")
    print(f"   Rows: {len(result2.dataframe) if result2.dataframe is not None else 0}")
    print(f"   From Cache: {result2.from_cache}")
    
    # Third run (should still be cached)
    print("\n🟢 Third Run (Cached):")
    start = time.time()
    result3 = run_query(question)
    time3 = time.time() - start
    
    print(f"   Time: {time3*1000:.0f}ms")
    print(f"   Success: {result3.success}")
    print(f"   Rows: {len(result3.dataframe) if result3.dataframe is not None else 0}")
    print(f"   From Cache: {result3.from_cache}")
    
    # Summary
    speedup = time1 / time2 if time2 > 0 else 0
    print("\n" + "-" * 40)
    print(f"📊 Speedup: {speedup:.1f}x")
    print(f"   Uncached: {time1*1000:.0f}ms")
    print(f"   Cached:   {time2*1000:.0f}ms")
    print(f"   Saved:    {(time1-time2)*1000:.0f}ms per query")


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main benchmark entry point."""
    print("=" * 70)
    print("NL-BI Dashboard - Cache Performance Benchmark")
    print("=" * 70)
    
    # Check caching is enabled
    print(f"\nCache Enabled: {ENABLE_CACHE}")
    
    # Check LLM configuration
    if not check_llm_config():
        print("\n❌ Error: No LLM API key configured.")
        print("\nSet environment variable:")
        print("  export OPENAI_API_KEY='your-key'")
        print("  or")
        print("  export LLM_API_KEY='your-key'")
        return 1
    
    print("LLM API Key: ✓ Configured")
    
    # Ensure database tables exist
    try:
        ensure_query_logs_table()
    except Exception as e:
        print(f"⚠️ Warning: Could not ensure query_logs table: {e}")
    
    # Run benchmarks
    import argparse
    parser = argparse.ArgumentParser(description="Cache benchmark")
    parser.add_argument("--single", action="store_true", help="Run single query benchmark")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark (fewer questions)")
    args = parser.parse_args()
    
    if args.single:
        run_single_query_benchmark()
    else:
        questions = TEST_QUESTIONS[:5] if args.quick else TEST_QUESTIONS
        results = run_benchmark(questions, runs_per_question=3)
        print_benchmark_summary(results)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
