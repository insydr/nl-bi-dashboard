"""
Natural Language Business Intelligence Dashboard - Visualization Engine
========================================================================

This module implements automatic chart generation from query results using
Plotly Express. It intelligently analyzes DataFrame structure to select
the most appropriate visualization type.

Features:
- Automatic chart type selection based on data analysis
- Support for: Line, Bar, Pie, KPI Card, Scatter, Table
- Time series detection and formatting
- Interactive hover tooltips
- Fallback to table view on errors
- Custom color schemes and styling

Chart Selection Logic:
1. Single value → KPI Card
2. Time series (datetime + numeric) → Line Chart
3. Categorical + numeric (few categories) → Bar Chart
4. Categorical + numeric (proportions) → Pie Chart
5. Two numeric columns → Scatter Plot
6. Default fallback → Table View
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Optional, Tuple, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import re


# =============================================================================
# Configuration & Constants
# =============================================================================

class ChartType(Enum):
    """Supported chart types."""
    KPI_CARD = "kpi_card"
    LINE = "line"
    BAR = "bar"
    PIE = "pie"
    SCATTER = "scatter"
    TABLE = "table"
    GROUPED_BAR = "grouped_bar"
    STACKED_BAR = "stacked_bar"


# Color palette for consistent branding
COLOR_PALETTE = [
    "#3b82f6",  # Blue
    "#10b981",  # Emerald
    "#f59e0b",  # Amber
    "#ef4444",  # Red
    "#8b5cf6",  # Purple
    "#06b6d4",  # Cyan
    "#f97316",  # Orange
    "#84cc16",  # Lime
    "#ec4899",  # Pink
    "#6366f1",  # Indigo
]

# Chart styling defaults
CHART_TEMPLATE = "plotly_white"
FONT_FAMILY = "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
TITLE_FONT_SIZE = 18
AXIS_FONT_SIZE = 12


# =============================================================================
# Data Analysis Functions
# =============================================================================

@dataclass
class ColumnAnalysis:
    """Analysis result for a DataFrame column."""
    name: str
    dtype: str
    is_numeric: bool
    is_datetime: bool
    is_categorical: bool
    is_unique_key: bool
    unique_count: int
    null_count: int
    sample_values: List[Any]


@dataclass
class DataFrameAnalysis:
    """Complete analysis of a DataFrame for chart selection."""
    row_count: int
    column_count: int
    columns: List[ColumnAnalysis]
    numeric_columns: List[str]
    datetime_columns: List[str]
    categorical_columns: List[str]
    has_time_series: bool
    is_single_value: bool
    recommended_chart: ChartType


def analyze_column(series: pd.Series) -> ColumnAnalysis:
    """
    Analyze a single DataFrame column.
    
    Args:
        series: pandas Series to analyze
        
    Returns:
        ColumnAnalysis with column metadata
    """
    dtype_str = str(series.dtype)
    
    # Check if numeric
    is_numeric = pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)
    
    # Check if datetime
    is_datetime = pd.api.types.is_datetime64_any_dtype(series)
    
    # Try to detect string dates
    if not is_datetime and series.dtype == object:
        try:
            # Sample first few non-null values
            sample = series.dropna().head(10)
            if len(sample) > 0:
                # Try to parse as dates
                pd.to_datetime(sample, errors='raise')
                is_datetime = True
        except (ValueError, TypeError):
            pass
    
    # Check if categorical
    unique_count = series.nunique()
    total_count = len(series)
    
    is_categorical = (
        not is_numeric and 
        not is_datetime and 
        unique_count <= max(20, total_count * 0.5)  # Allow up to 50% unique or 20 categories
    )
    
    # Check if this is a unique key (ID column)
    # Only mark as unique key if:
    # 1. All values are unique AND
    # 2. Dataset has more than 10 rows (avoid false positives on small datasets) OR
    # 3. Column name suggests it's an ID
    is_unique_key = False
    if unique_count == total_count and total_count > 1:
        # Check if column name suggests it's an ID
        col_name_lower = str(series.name).lower()
        id_patterns = ['id', '_id', 'key', 'uuid', 'guid', 'pk']
        looks_like_id = any(pattern in col_name_lower for pattern in id_patterns)
        
        # Only mark as unique key if dataset is large or column looks like an ID
        if total_count > 10 or looks_like_id:
            is_unique_key = True
    
    return ColumnAnalysis(
        name=series.name,
        dtype=dtype_str,
        is_numeric=is_numeric,
        is_datetime=is_datetime,
        is_categorical=is_categorical,
        is_unique_key=is_unique_key,
        unique_count=unique_count,
        null_count=series.isna().sum(),
        sample_values=series.dropna().head(5).tolist()
    )


def analyze_dataframe(df: pd.DataFrame) -> DataFrameAnalysis:
    """
    Perform comprehensive analysis of a DataFrame for chart selection.
    
    Args:
        df: pandas DataFrame to analyze
        
    Returns:
        DataFrameAnalysis with recommendations
    """
    if df.empty:
        return DataFrameAnalysis(
            row_count=0,
            column_count=0,
            columns=[],
            numeric_columns=[],
            datetime_columns=[],
            categorical_columns=[],
            has_time_series=False,
            is_single_value=False,
            recommended_chart=ChartType.TABLE
        )
    
    # Analyze each column
    columns = [analyze_column(df[col]) for col in df.columns]
    
    # Categorize columns
    numeric_columns = [c.name for c in columns if c.is_numeric]
    datetime_columns = [c.name for c in columns if c.is_datetime]
    categorical_columns = [c.name for c in columns if c.is_categorical and not c.is_unique_key]
    
    # Detect time series
    has_time_series = (
        len(datetime_columns) > 0 and 
        len(numeric_columns) > 0 and
        len(df) > 1
    )
    
    # Check for single value (KPI)
    is_single_value = (
        len(df) == 1 and 
        len(numeric_columns) == 1 and
        len(df.columns) <= 2
    )
    
    # Determine recommended chart type
    recommended_chart = _determine_chart_type(
        df, columns, numeric_columns, datetime_columns, categorical_columns
    )
    
    return DataFrameAnalysis(
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        numeric_columns=numeric_columns,
        datetime_columns=datetime_columns,
        categorical_columns=categorical_columns,
        has_time_series=has_time_series,
        is_single_value=is_single_value,
        recommended_chart=recommended_chart
    )


def _determine_chart_type(
    df: pd.DataFrame,
    columns: List[ColumnAnalysis],
    numeric_columns: List[str],
    datetime_columns: List[str],
    categorical_columns: List[str]
) -> ChartType:
    """
    Determine the best chart type based on DataFrame analysis.
    
    Selection Logic:
    1. Empty DataFrame → Table
    2. Single row, single numeric → KPI Card
    3. Datetime + Numeric → Line Chart
    4. Categorical + Numeric:
       - Few categories (2-6) + looks like distribution → Pie Chart
       - Otherwise → Bar Chart
    5. Two numeric columns → Scatter Plot
    6. Default → Table
    """
    if df.empty:
        return ChartType.TABLE
    
    # Single value KPI
    if len(df) == 1 and len(numeric_columns) >= 1:
        # Check if this looks like an aggregate result
        if len(df.columns) <= 3:
            return ChartType.KPI_CARD
    
    # Time series - Line Chart
    if datetime_columns and numeric_columns:
        # Sort by datetime to ensure proper line chart
        return ChartType.LINE
    
    # Categorical data
    if categorical_columns and numeric_columns:
        cat_col = categorical_columns[0]
        num_col = numeric_columns[0]
        unique_cats = df[cat_col].nunique()
        
        # Check if this looks like a distribution/proportion (for pie chart)
        # Pie charts are best for:
        # - Few categories (2-6)
        # - Values are non-negative (parts of a whole)
        # - Column name suggests distribution (status, type, category)
        # - OR values are counts/percentages
        if 2 <= unique_cats <= 6:
            total = df[num_col].sum()
            all_positive = all(df[num_col] >= 0)
            
            # Check if column name suggests it's a distribution
            cat_col_lower = cat_col.lower()
            distribution_keywords = ['status', 'type', 'segment', 'class', 
                                    'tier', 'priority', 'state', 'outcome']
            is_distribution = any(kw in cat_col_lower for kw in distribution_keywords)
            
            # Check if numeric column is a count (but not total/sum)
            num_col_lower = num_col.lower()
            # Be more specific - avoid matching "total_sales" as a count
            count_keywords = ['count', 'number', 'qty', 'quantity', 'num', 'amount']
            # Exclude "total" and "sum" as they indicate aggregated comparisons
            exclude_keywords = ['total', 'sum', 'revenue', 'sales', 'amount']
            is_count = (any(kw in num_col_lower for kw in count_keywords) and 
                       not any(kw in num_col_lower for kw in exclude_keywords))
            
            # Use pie chart for distributions with positive values
            # Pie charts are best for showing "parts of a whole" (status breakdown)
            # Bar charts are better for comparisons (sales by region)
            if all_positive and total > 0 and is_distribution:
                return ChartType.PIE
        
        # Bar chart for comparisons (default for categorical data)
        if unique_cats <= 15:
            return ChartType.BAR
        
        # Too many categories for a bar chart
        return ChartType.TABLE
    
    # Scatter plot for two numeric columns
    if len(numeric_columns) >= 2 and len(df) > 5:
        return ChartType.SCATTER
    
    # Single numeric column - check value distribution
    if len(numeric_columns) == 1 and len(df) > 1:
        unique_vals = df[numeric_columns[0]].nunique()
        if unique_vals <= 15 and unique_vals < len(df):
            return ChartType.BAR
    
    # Default to table
    return ChartType.TABLE


# =============================================================================
# Chart Generation Functions
# =============================================================================

def create_kpi_card(
    df: pd.DataFrame,
    analysis: DataFrameAnalysis,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create a KPI card showing a single metric value.
    
    Args:
        df: DataFrame with single row
        analysis: DataFrame analysis results
        title: Optional title for the card
        
    Returns:
        Plotly Figure object
    """
    # Get the numeric column
    numeric_col = analysis.numeric_columns[0]
    value = df[numeric_col].iloc[0]
    
    # Format the value
    if isinstance(value, (int, np.integer)):
        formatted_value = f"{value:,}"
    elif isinstance(value, (float, np.floating)):
        if abs(value) >= 1_000_000:
            formatted_value = f"${value/1_000_000:.2f}M"
        elif abs(value) >= 1_000:
            formatted_value = f"${value/1_000:.2f}K"
        else:
            formatted_value = f"{value:,.2f}"
    else:
        formatted_value = str(value)
    
    # Get label from column name or query
    label = title or numeric_col.replace("_", " ").title()
    
    # Create indicator figure
    fig = go.Figure()
    
    fig.add_trace(go.Indicator(
        mode="number",
        value=value,
        number={
            "font": {"size": 48, "family": FONT_FAMILY, "color": "#1e293b"},
            "valueformat": ",.2f" if isinstance(value, float) else ","
        },
        title={
            "text": label,
            "font": {"size": 16, "family": FONT_FAMILY, "color": "#64748b"}
        }
    ))
    
    fig.update_layout(
        height=200,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor="white",
    )
    
    return fig


def create_line_chart(
    df: pd.DataFrame,
    analysis: DataFrameAnalysis,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create a line chart for time series data.
    
    Args:
        df: DataFrame with datetime and numeric columns
        analysis: DataFrame analysis results
        title: Optional title for the chart
        
    Returns:
        Plotly Figure object
    """
    # Get column names
    date_col = analysis.datetime_columns[0]
    
    # Sort by date
    df = df.sort_values(date_col)
    
    # Create line chart
    fig = px.line(
        df,
        x=date_col,
        y=analysis.numeric_columns[:3],  # Limit to 3 lines
        title=title,
        markers=True,
        color_discrete_sequence=COLOR_PALETTE
    )
    
    # Update styling
    fig.update_traces(
        mode="lines+markers",
        hovertemplate=None
    )
    
    fig.update_layout(
        template=CHART_TEMPLATE,
        font=dict(family=FONT_FAMILY),
        title_font_size=TITLE_FONT_SIZE,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(
            title=date_col.replace("_", " ").title(),
            tickformat="%b %Y" if len(df) > 30 else "%b %d",
            gridcolor="#f1f5f9"
        ),
        yaxis=dict(
            gridcolor="#f1f5f9"
        ),
        margin=dict(l=60, r=20, t=60, b=60),
        height=400
    )
    
    # Add range slider for long time series
    if len(df) > 30:
        fig.update_xaxes(rangeslider_visible=True)
    
    return fig


def create_bar_chart(
    df: pd.DataFrame,
    analysis: DataFrameAnalysis,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create a bar chart for categorical data.
    
    Args:
        df: DataFrame with categorical and numeric columns
        analysis: DataFrame analysis results
        title: Optional title for the chart
        
    Returns:
        Plotly Figure object
    """
    # Get column names
    cat_col = analysis.categorical_columns[0] if analysis.categorical_columns else df.columns[0]
    num_col = analysis.numeric_columns[0]
    
    # Sort by numeric value (descending)
    df = df.sort_values(num_col, ascending=True)
    
    # Limit to top 15 categories
    if len(df) > 15:
        df = df.tail(15)
    
    # Create bar chart
    fig = px.bar(
        df,
        x=num_col,
        y=cat_col,
        orientation="h",
        title=title,
        color=num_col,
        color_continuous_scale="Blues",
        text=num_col
    )
    
    # Update styling
    fig.update_traces(
        texttemplate="%{text:,.0f}",
        textposition="outside",
        hovertemplate=f"<b>%{cat_col}</b><br>{num_col}: %{{x:,.2f}}<extra></extra>"
    )
    
    fig.update_layout(
        template=CHART_TEMPLATE,
        font=dict(family=FONT_FAMILY),
        title_font_size=TITLE_FONT_SIZE,
        showlegend=False,
        xaxis=dict(
            title=num_col.replace("_", " ").title(),
            gridcolor="#f1f5f9"
        ),
        yaxis=dict(
            title="",
            gridcolor="#f1f5f9"
        ),
        coloraxis_showscale=False,
        margin=dict(l=120, r=40, t=60, b=60),
        height=max(300, len(df) * 30 + 100)
    )
    
    return fig


def create_pie_chart(
    df: pd.DataFrame,
    analysis: DataFrameAnalysis,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create a pie chart for proportional data.
    
    Args:
        df: DataFrame with categorical and numeric columns
        analysis: DataFrame analysis results
        title: Optional title for the chart
        
    Returns:
        Plotly Figure object
    """
    # Get column names
    cat_col = analysis.categorical_columns[0]
    num_col = analysis.numeric_columns[0]
    
    # Create pie chart
    fig = px.pie(
        df,
        names=cat_col,
        values=num_col,
        title=title,
        color_discrete_sequence=COLOR_PALETTE
    )
    
    # Update styling
    fig.update_traces(
        textposition="inside",
        textinfo="percent+label",
        hovertemplate=f"<b>%{cat_col}</b><br>{num_col}: %{{value:,.2f}}<br>Percent: %{{percent}}<extra></extra>"
    )
    
    fig.update_layout(
        template=CHART_TEMPLATE,
        font=dict(family=FONT_FAMILY),
        title_font_size=TITLE_FONT_SIZE,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=20, r=20, t=60, b=80),
        height=400
    )
    
    return fig


def create_scatter_plot(
    df: pd.DataFrame,
    analysis: DataFrameAnalysis,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create a scatter plot for two numeric columns.
    
    Args:
        df: DataFrame with two numeric columns
        analysis: DataFrame analysis results
        title: Optional title for the chart
        
    Returns:
        Plotly Figure object
    """
    x_col = analysis.numeric_columns[0]
    y_col = analysis.numeric_columns[1]
    
    # Create scatter plot
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        title=title,
        trendline="ols",
        color_discrete_sequence=[COLOR_PALETTE[0]]
    )
    
    # Update styling
    fig.update_traces(
        marker=dict(size=10, opacity=0.7),
        hovertemplate=f"<b>{x_col}:</b> %{{x:,.2f}}<br><b>{y_col}:</b> %{{y:,.2f}}<extra></extra>"
    )
    
    fig.update_layout(
        template=CHART_TEMPLATE,
        font=dict(family=FONT_FAMILY),
        title_font_size=TITLE_FONT_SIZE,
        xaxis=dict(
            title=x_col.replace("_", " ").title(),
            gridcolor="#f1f5f9"
        ),
        yaxis=dict(
            title=y_col.replace("_", " ").title(),
            gridcolor="#f1f5f9"
        ),
        margin=dict(l=60, r=20, t=60, b=60),
        height=400
    )
    
    return fig


def create_table_view(
    df: pd.DataFrame,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create a table view as fallback.
    
    Args:
        df: DataFrame to display
        title: Optional title for the table
        
    Returns:
        Plotly Figure object with table
    """
    # Format column names
    column_names = [col.replace("_", " ").title() for col in df.columns]
    
    # Create table
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=column_names,
            fill_color="#f8fafc",
            font=dict(color="#1e293b", size=12, family=FONT_FAMILY),
            align="left",
            height=36
        ),
        cells=dict(
            values=[df[col] for col in df.columns],
            fill_color="white",
            font=dict(color="#334155", size=11, family=FONT_FAMILY),
            align="left",
            height=30,
            format=[",.2f" if pd.api.types.is_numeric_dtype(df[col]) else "" for col in df.columns]
        )
    )])
    
    fig.update_layout(
        title=title,
        title_font_size=TITLE_FONT_SIZE,
        margin=dict(l=20, r=20, t=60, b=20),
        height=min(600, len(df) * 35 + 80)
    )
    
    return fig


# =============================================================================
# Main Chart Generation Function
# =============================================================================

def generate_chart(
    df: pd.DataFrame,
    query: Optional[str] = None,
    chart_type: Optional[ChartType] = None,
    title: Optional[str] = None
) -> go.Figure:
    """
    Generate an appropriate chart from a DataFrame.
    
    This function analyzes the DataFrame structure and automatically selects
    the most appropriate visualization type, or uses a specified chart type.
    
    Args:
        df: pandas DataFrame with query results
        query: Original natural language query (used for title/hints)
        chart_type: Force a specific chart type (optional)
        title: Custom chart title (optional)
        
    Returns:
        Plotly Figure object ready for display
        
    Examples:
        >>> df = pd.DataFrame({
        ...     'region': ['North', 'South', 'East', 'West'],
        ...     'sales': [100000, 85000, 75000, 90000]
        ... })
        >>> fig = generate_chart(df, "Sales by region")
        
        >>> # Force specific chart type
        >>> fig = generate_chart(df, chart_type=ChartType.PIE)
    """
    # Handle empty DataFrame
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            font=dict(size=16, color="#64748b")
        )
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20)
        )
        return fig
    
    # Analyze DataFrame
    analysis = analyze_dataframe(df)
    
    # Determine chart type
    if chart_type is None:
        chart_type = analysis.recommended_chart
    
    # Generate title if not provided
    if title is None and query:
        title = _generate_title(query, chart_type)
    
    # Try to create the chart, fall back to table on error
    try:
        if chart_type == ChartType.KPI_CARD:
            return create_kpi_card(df, analysis, title)
        
        elif chart_type == ChartType.LINE:
            return create_line_chart(df, analysis, title)
        
        elif chart_type == ChartType.BAR:
            return create_bar_chart(df, analysis, title)
        
        elif chart_type == ChartType.PIE:
            return create_pie_chart(df, analysis, title)
        
        elif chart_type == ChartType.SCATTER:
            return create_scatter_plot(df, analysis, title)
        
        else:  # TABLE or default
            return create_table_view(df, title)
            
    except Exception as e:
        # Fall back to table view on any error
        print(f"Chart generation failed: {e}. Falling back to table view.")
        return create_table_view(df, title or "Results")


def _generate_title(query: str, chart_type: ChartType) -> str:
    """
    Generate a chart title from the query.
    
    Args:
        query: Original natural language query
        chart_type: Type of chart being generated
        
    Returns:
        Formatted title string
    """
    # Clean up query for title
    title = query.strip().rstrip("?.").strip()
    
    # Capitalize first letter
    if title:
        title = title[0].upper() + title[1:]
    
    return title


def get_chart_recommendation(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get chart recommendation details without generating the chart.
    
    Useful for debugging or showing users why a certain chart was selected.
    
    Args:
        df: pandas DataFrame to analyze
        
    Returns:
        Dictionary with analysis details and recommendations
    """
    analysis = analyze_dataframe(df)
    
    return {
        "row_count": analysis.row_count,
        "column_count": analysis.column_count,
        "numeric_columns": analysis.numeric_columns,
        "datetime_columns": analysis.datetime_columns,
        "categorical_columns": analysis.categorical_columns,
        "has_time_series": analysis.has_time_series,
        "is_single_value": analysis.is_single_value,
        "recommended_chart": analysis.recommended_chart.value,
        "column_details": [
            {
                "name": c.name,
                "type": c.dtype,
                "is_numeric": c.is_numeric,
                "is_datetime": c.is_datetime,
                "is_categorical": c.is_categorical,
                "unique_count": c.unique_count
            }
            for c in analysis.columns
        ]
    }


# =============================================================================
# Test Functions
# =============================================================================

def test_visualization():
    """Run test cases for different query result types."""
    
    print("\n" + "=" * 80)
    print("VISUALIZATION ENGINE TESTS")
    print("=" * 80)
    
    # Test Case 1: KPI Card - Single Value
    print("\n📊 Test 1: KPI Card (Single Aggregate Value)")
    print("-" * 40)
    df1 = pd.DataFrame({
        'total_revenue': [136897.75]
    })
    recommendation1 = get_chart_recommendation(df1)
    print(f"   Recommended: {recommendation1['recommended_chart']}")
    fig1 = generate_chart(df1, "What is the total revenue?")
    print(f"   Generated: KPI Card with value $136,897.75")
    
    # Test Case 2: Bar Chart - Categorical Data
    print("\n📊 Test 2: Bar Chart (Categorical + Numeric)")
    print("-" * 40)
    df2 = pd.DataFrame({
        'region': ['North America', 'Europe', 'Asia Pacific', 'Latin America', 'Middle East'],
        'total_sales': [45000, 38000, 32000, 18000, 12000]
    })
    recommendation2 = get_chart_recommendation(df2)
    print(f"   Recommended: {recommendation2['recommended_chart']}")
    print(f"   Categories: {recommendation2['categorical_columns']}")
    print(f"   Numeric: {recommendation2['numeric_columns']}")
    
    # Test Case 3: Line Chart - Time Series
    print("\n📊 Test 3: Line Chart (Time Series)")
    print("-" * 40)
    dates = pd.date_range(start='2023-01-01', periods=12, freq='ME')
    df3 = pd.DataFrame({
        'order_date': dates,
        'total_amount': [45000, 52000, 48000, 61000, 55000, 67000, 
                        72000, 68000, 75000, 81000, 78000, 92000]
    })
    recommendation3 = get_chart_recommendation(df3)
    print(f"   Recommended: {recommendation3['recommended_chart']}")
    print(f"   Datetime columns: {recommendation3['datetime_columns']}")
    print(f"   Time series detected: {recommendation3['has_time_series']}")
    
    # Test Case 4: Pie Chart - Proportions
    print("\n📊 Test 4: Pie Chart (Proportions)")
    print("-" * 40)
    df4 = pd.DataFrame({
        'status': ['completed', 'pending', 'shipped', 'cancelled', 'refunded'],
        'count': [130, 20, 30, 14, 6]
    })
    recommendation4 = get_chart_recommendation(df4)
    print(f"   Recommended: {recommendation4['recommended_chart']}")
    print(f"   Categorical: {recommendation4['categorical_columns']}")
    print(f"   Numeric: {recommendation4['numeric_columns']}")
    
    # Test Case 5: Scatter Plot - Two Numeric
    print("\n📊 Test 5: Scatter Plot (Two Numeric Columns)")
    print("-" * 40)
    np.random.seed(42)
    df5 = pd.DataFrame({
        'price': np.random.uniform(10, 500, 50),
        'quantity_sold': np.random.randint(1, 100, 50)
    })
    # Add some correlation
    df5['quantity_sold'] = df5['quantity_sold'] - (df5['price'] / 10).astype(int)
    recommendation5 = get_chart_recommendation(df5)
    print(f"   Recommended: {recommendation5['recommended_chart']}")
    print(f"   Numeric columns: {recommendation5['numeric_columns']}")
    print(f"   Row count: {recommendation5['row_count']}")
    
    # Test Case 6: Table - Complex Data
    print("\n📊 Test 6: Table View (Complex Data)")
    print("-" * 40)
    df6 = pd.DataFrame({
        'id': range(1, 21),
        'name': [f'Customer {i}' for i in range(1, 21)],
        'email': [f'customer{i}@email.com' for i in range(1, 21)],
        'total_orders': np.random.randint(1, 20, 20),
        'total_spent': np.random.uniform(100, 5000, 20)
    })
    recommendation6 = get_chart_recommendation(df6)
    print(f"   Recommended: {recommendation6['recommended_chart']}")
    print(f"   Columns: {recommendation6['column_count']}")
    print(f"   Rows: {recommendation6['row_count']}")
    
    # Test Case 7: Empty DataFrame
    print("\n📊 Test 7: Empty DataFrame")
    print("-" * 40)
    df7 = pd.DataFrame()
    recommendation7 = get_chart_recommendation(df7)
    print(f"   Recommended: {recommendation7['recommended_chart']}")
    
    # Test Case 8: Multi-category Bar Chart
    print("\n📊 Test 8: Grouped Data (Multiple Categories)")
    print("-" * 40)
    df8 = pd.DataFrame({
        'category': ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food'],
        'total_revenue': [125000, 89000, 67000, 45000, 23000, 34000, 18000],
        'order_count': [450, 890, 340, 280, 560, 420, 890]
    })
    recommendation8 = get_chart_recommendation(df8)
    print(f"   Recommended: {recommendation8['recommended_chart']}")
    print(f"   Categories: {recommendation8['categorical_columns']}")
    print(f"   Numeric columns: {recommendation8['numeric_columns']}")
    
    print("\n" + "=" * 80)
    print("✅ All visualization tests completed")
    print("=" * 80)
    
    return True


def save_test_charts(output_dir: str = "."):
    """Generate and save test charts as HTML files."""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    
    test_cases = [
        ("kpi_card", pd.DataFrame({'total_revenue': [136897.75]}), "What is the total revenue?"),
        ("bar_chart", pd.DataFrame({
            'region': ['North America', 'Europe', 'Asia Pacific', 'Latin America', 'Middle East'],
            'total_sales': [45000, 38000, 32000, 18000, 12000]
        }), "Sales by region"),
        ("line_chart", pd.DataFrame({
            'order_date': pd.date_range(start='2023-01-01', periods=12, freq='ME'),
            'total_amount': [45000, 52000, 48000, 61000, 55000, 67000, 
                            72000, 68000, 75000, 81000, 78000, 92000]
        }), "Monthly sales trend"),
        ("pie_chart", pd.DataFrame({
            'status': ['completed', 'pending', 'shipped', 'cancelled', 'refunded'],
            'count': [130, 20, 30, 14, 6]
        }), "Order status distribution"),
    ]
    
    for name, df, query in test_cases:
        fig = generate_chart(df, query)
        output_path = os.path.join(output_dir, f"{name}.html")
        fig.write_html(output_path)
        print(f"Saved: {output_path}")


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import sys
    
    print("=" * 80)
    print("NL-BI Dashboard - Visualization Engine Tests")
    print("=" * 80)
    
    # Run visualization tests
    test_visualization()
    
    # Optionally save test charts
    save = len(sys.argv) > 1 and sys.argv[1] == "--save"
    if save:
        print("\n📁 Saving test charts...")
        save_test_charts("/home/z/my-project/nlbi-dashboard/test_charts")
    
    sys.exit(0)
