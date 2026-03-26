"""
Natural Language Business Intelligence Dashboard - Streamlit Frontend
======================================================================

This module implements the user interface for the NL-BI Dashboard using
Streamlit. It integrates the SQL chain and visualization engine to provide
a chat-like interface for querying business data.

Features:
- Natural language query input
- Suggested questions for quick exploration
- Query history in sidebar
- Interactive Plotly charts
- SQL transparency view
- Feedback mechanism (thumbs up/down)
- Loading states for user feedback
- Error handling with retry suggestions

Layout (per PRD Section 8):
- Sidebar: Query history, Suggested Questions
- Main: Chat input, Chart display, Data table toggle, SQL view
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import os
from pathlib import Path

# Import our modules
from sql_chain import run_query, QueryResult, LLMConfig, get_llm, show_llm_config
from visualization import generate_chart, get_chart_recommendation, ChartType


# =============================================================================
# Page Configuration
# =============================================================================

st.set_page_config(
    page_title="NL-BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': """
        **NL-BI Dashboard** - Natural Language Business Intelligence
        
        Query your database using plain English. No SQL knowledge required.
        
        Built with ❤️ using LangChain, Streamlit, and Plotly.
        """
    }
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Main container */
    .main .block-container {
        padding-top: 2rem;
    }
    
    /* Chat input styling */
    .stChatInput {
        border-radius: 12px;
    }
    
    /* Chart container */
    .chart-container {
        background-color: white;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    /* Sidebar styling */
    .sidebar .sidebar-content {
        background-color: #f8fafc;
    }
    
    /* Suggested question buttons */
    .suggested-btn {
        width: 100%;
        text-align: left;
        margin-bottom: 0.5rem;
    }
    
    /* Feedback buttons */
    .feedback-container {
        display: flex;
        gap: 0.5rem;
        margin-top: 1rem;
    }
    
    /* SQL code block */
    .sql-code {
        background-color: #1e293b;
        color: #e2e8f0;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Monaco', 'Menlo', monospace;
        font-size: 0.875rem;
        overflow-x: auto;
    }
    
    /* Loading spinner */
    .loading-text {
        color: #64748b;
        font-style: italic;
    }
    
    /* KPI card styling */
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1e293b;
    }
    
    .kpi-label {
        font-size: 1rem;
        color: #64748b;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Session State Management
# =============================================================================

def init_session_state():
    """Initialize session state variables."""
    if "query_history" not in st.session_state:
        st.session_state.query_history = []
    
    if "feedback_log" not in st.session_state:
        st.session_state.feedback_log = []
    
    if "current_result" not in st.session_state:
        st.session_state.current_result = None
    
    if "show_sql" not in st.session_state:
        st.session_state.show_sql = False
    
    if "show_table" not in st.session_state:
        st.session_state.show_table = False
    
    if "llm_configured" not in st.session_state:
        # Check if LLM is configured
        config = LLMConfig.from_env()
        st.session_state.llm_configured = config.api_key is not None


# =============================================================================
# Suggested Questions (from PRD Section 8)
# =============================================================================

SUGGESTED_QUESTIONS = [
    "📈 What were total sales by region?",
    "🏆 Show top 5 customers by order amount",
    "📦 What is the total revenue by product category?",
    "📅 How many orders were placed this month?",
    "💰 What is the average order value?",
    "🚚 Orders by shipping method",
    "📊 Sales trend over time",
    "🏷️ Products with low stock (less than 50 units)",
    "👥 Customers by segment",
    "⭐ Top selling products by quantity",
]


# =============================================================================
# Helper Functions
# =============================================================================

def format_datetime(dt: datetime) -> str:
    """Format datetime for display."""
    return dt.strftime("%H:%M:%S") if dt else ""


def save_query_to_history(question: str, result: QueryResult):
    """Save query to session history."""
    history_entry = {
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "success": result.success,
        "sql": result.sql_query,
        "row_count": len(result.dataframe) if result.dataframe is not None else 0,
        "error": result.error_message if not result.success else None
    }
    
    # Add to beginning of list (most recent first)
    st.session_state.query_history.insert(0, history_entry)
    
    # Keep only last 20 queries
    if len(st.session_state.query_history) > 20:
        st.session_state.query_history = st.session_state.query_history[:20]


def save_feedback(question: str, sql: str, is_positive: bool, comment: str = ""):
    """Save user feedback."""
    feedback_entry = {
        "timestamp": datetime.now().isoformat(),
        "question": question,
        "sql": sql,
        "is_positive": is_positive,
        "comment": comment
    }
    
    st.session_state.feedback_log.append(feedback_entry)


def get_status_emoji(success: bool) -> str:
    """Get emoji for query status."""
    return "✅" if success else "❌"


def render_sql_view(sql: Optional[str]):
    """Render the SQL transparency view."""
    if not sql:
        return
    
    with st.expander("🔍 View Generated SQL", expanded=False):
        st.markdown("**Generated SQL Query:**")
        st.code(sql, language="sql", line_numbers=True)
        
        st.caption(
            "💡 This SQL was generated by the AI based on your question. "
            "You can verify it for accuracy or use it as a reference."
        )


def render_feedback_buttons(question: str, sql: Optional[str]):
    """Render thumbs up/down feedback buttons."""
    if not sql:
        return
    
    st.markdown("---")
    st.markdown("**Was this result helpful?**")
    
    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        if st.button("👍 Helpful", key="feedback_positive", use_container_width=True):
            save_feedback(question, sql, is_positive=True)
            st.success("Thanks for your feedback! 🎉")
            st.rerun()
    
    with col2:
        if st.button("👎 Not Helpful", key="feedback_negative", use_container_width=True):
            save_feedback(question, sql, is_positive=False)
            st.warning("Thanks for the feedback. We'll use it to improve! 📝")
            st.rerun()


def render_suggested_questions():
    """Render suggested questions in sidebar."""
    st.sidebar.markdown("### 💡 Suggested Questions")
    st.sidebar.caption("Click to try these common queries:")
    
    for question in SUGGESTED_QUESTIONS[:6]:  # Show top 6
        # Remove emoji for the query
        clean_question = question.split(" ", 1)[1] if " " in question else question
        
        if st.sidebar.button(
            question, 
            key=f"suggest_{hash(question)}",
            use_container_width=True
        ):
            # Set the question in session state to trigger query
            st.session_state.suggested_question = clean_question
            st.rerun()


def render_query_history():
    """Render query history in sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📜 Recent Queries")
    
    if not st.session_state.query_history:
        st.sidebar.caption("No queries yet. Try asking a question!")
        return
    
    for i, entry in enumerate(st.session_state.query_history[:10]):
        status = get_status_emoji(entry["success"])
        timestamp = format_datetime(datetime.fromisoformat(entry["timestamp"]))
        
        with st.sidebar.expander(f"{status} {entry['question'][:30]}...", expanded=False):
            st.caption(f"⏰ {timestamp}")
            st.caption(f"📊 {entry['row_count']} rows returned")
            
            if entry["success"] and entry["sql"]:
                st.code(entry["sql"], language="sql")
            elif entry["error"]:
                st.error(f"Error: {entry['error']}")
            
            # Button to re-run this query
            if st.button("🔄 Re-run", key=f"rerun_{i}"):
                st.session_state.suggested_question = entry["question"]
                st.rerun()


def render_result(result: QueryResult, question: str):
    """Render query result with chart and options."""
    if not result.success:
        st.error(f"❌ **Query Failed**: {result.error_message}")
        
        if result.sql_query:
            with st.expander("View attempted SQL"):
                st.code(result.sql_query, language="sql")
        
        st.info("💡 Try rephrasing your question or check the suggested queries for examples.")
        return
    
    # Success - display results
    st.success(f"✅ Query executed successfully! ({len(result.dataframe)} rows)")
    
    # Generate and display chart
    try:
        fig = generate_chart(result.dataframe, question)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.warning(f"Chart generation failed: {e}. Showing table view instead.")
        st.dataframe(result.dataframe, use_container_width=True)
    
    # Data table toggle
    with st.expander("📋 View Data Table", expanded=False):
        st.dataframe(
            result.dataframe,
            use_container_width=True,
            hide_index=True
        )
        
        # Download button
        csv = result.dataframe.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # SQL transparency view
    render_sql_view(result.sql_query)
    
    # Feedback buttons
    render_feedback_buttons(question, result.sql_query)


def render_welcome_screen():
    """Render the welcome screen when no query has been made."""
    st.markdown("""
    ## 👋 Welcome to NL-BI Dashboard!
    
    **Ask questions about your business data in plain English.**
    
    ### 🎯 Examples of what you can ask:
    - *"What were total sales by region?"*
    - *"Show me the top 5 customers by order amount"*
    - *"What is the revenue trend over time?"*
    - *"Which products have low stock?"*
    
    ---
    """)
    
    # Show quick stats
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(label="Customers", value="60", delta="Active")
    with col2:
        st.metric(label="Products", value="60", delta="In catalog")
    with col3:
        st.metric(label="Orders", value="200", delta="Total")
    with col4:
        st.metric(label="Revenue", value="$136K+", delta="Completed")
    
    st.markdown("---")
    st.info("💡 **Tip**: Start by clicking a suggested question from the sidebar, or type your own question below!")


def check_llm_configuration():
    """Check if LLM is properly configured and show warning if not."""
    config = LLMConfig.from_env()
    
    if not config.api_key:
        st.error("⚠️ **LLM Not Configured**")
        
        with st.expander("How to configure your LLM provider", expanded=True):
            st.markdown("""
            ### Configuration Options
            
            **Option 1: Using OpenAI**
            ```bash
            export OPENAI_API_KEY='your-openai-api-key'
            ```
            
            **Option 2: Using a custom endpoint (Ollama, vLLM, Groq, etc.)**
            ```bash
            export LLM_API_KEY='your-key'
            export LLM_BASE_URL='http://localhost:11434/v1'
            export LLM_MODEL='llama3'
            ```
            
            **Option 3: Create a `.env` file**
            ```env
            OPENAI_API_KEY=your-key-here
            # Or for custom endpoints:
            LLM_API_KEY=ollama
            LLM_BASE_URL=http://localhost:11434/v1
            LLM_MODEL=llama3
            ```
            
            ### Supported Providers
            - **OpenAI** - Default, requires API key
            - **Ollama** - Free local LLM
            - **Groq** - Fast inference API
            - **Together AI** - Open-source models
            - **Any OpenAI-compatible endpoint**
            """)
        
        return False
    
    return True


# =============================================================================
# Settings Panel
# =============================================================================

def render_settings_panel():
    """Render settings panel in sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚙️ Settings")
    
    config = LLMConfig.from_env()
    
    # Show current provider
    provider_info = config.get_provider_info()
    st.sidebar.caption(f"**Provider:** {provider_info}")
    st.sidebar.caption(f"**Model:** {config.model}")
    
    # Chart preference
    chart_preference = st.sidebar.selectbox(
        "Default Chart Style",
        ["Auto-detect", "Bar Chart", "Line Chart", "Pie Chart", "Table Only"],
        key="chart_preference"
    )
    
    # Max rows display
    max_rows = st.sidebar.slider(
        "Max rows in table",
        min_value=10,
        max_value=100,
        value=50,
        key="max_rows"
    )


# =============================================================================
# Main Application
# =============================================================================

def main():
    """Main application entry point."""
    # Initialize session state
    init_session_state()
    
    # App title and header
    st.title("📊 NL-BI Dashboard")
    st.markdown("**Query your business data using natural language**")
    st.markdown("---")
    
    # Check LLM configuration
    llm_ready = check_llm_configuration()
    
    # Render sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/300x100/3b82f6/white?text=NL-BI+Dashboard", use_container_width=True)
        st.markdown("### 🏠 Navigation")
        st.caption("Natural Language → SQL → Insights")
        
        render_suggested_questions()
        render_query_history()
        render_settings_panel()
    
    # Handle suggested question click
    if "suggested_question" in st.session_state:
        question = st.session_state.suggested_question
        del st.session_state.suggested_question
        
        # Process the query
        if llm_ready:
            with st.spinner("🤔 Thinking..."):
                result = run_query(question)
            
            st.session_state.current_result = result
            save_query_to_history(question, result)
        else:
            st.session_state.current_result = None
    
    # Main content area
    if st.session_state.current_result:
        # Show previous result
        last_question = st.session_state.query_history[0]["question"] if st.session_state.query_history else ""
        render_result(st.session_state.current_result, last_question)
    else:
        # Show welcome screen
        render_welcome_screen()
    
    # Chat input
    st.markdown("---")
    
    if llm_ready:
        # Use st.chat_input for a chat-like experience
        if prompt := st.chat_input("Ask a question about your data..."):
            # Show user message
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Process query with loading states
            with st.chat_message("assistant"):
                status_placeholder = st.empty()
                
                # Step 1: Generating SQL
                status_placeholder.info("🔄 Generating SQL query...")
                
                try:
                    result = run_query(prompt)
                    
                    # Step 2: Show completion
                    status_placeholder.empty()
                    
                    # Save and display result
                    st.session_state.current_result = result
                    save_query_to_history(prompt, result)
                    
                    # Render result
                    render_result(result, prompt)
                    
                except Exception as e:
                    status_placeholder.empty()
                    st.error(f"❌ An error occurred: {str(e)}")
                    st.info("Please try rephrasing your question or check the logs for details.")
    else:
        # Show disabled input with message
        st.text_input(
            "Ask a question about your data...",
            value="Configure LLM to enable queries...",
            disabled=True,
            key="disabled_input"
        )
        st.warning("⚠️ Please configure your LLM provider to start querying.")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    main()
