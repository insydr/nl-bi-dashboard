"""
Natural Language Business Intelligence Dashboard - Database Setup
==================================================================

This module provides database abstraction for the NL-BI Dashboard,
supporting both SQLite (MVP) and PostgreSQL (Production).

Features:
- SQLAlchemy-based database abstraction
- Automatic schema creation with foreign key constraints
- Sample data generation for development/testing
- Read-only connection support for security
- Schema introspection for LLM context

Supported Databases:
- SQLite (MVP): File-based, no setup required
- PostgreSQL (Production): Docker-based, supports concurrency

Security:
- SQLite: Read-only mode via URI parameter
- PostgreSQL: Dedicated read-only user with SELECT permissions only

Environment Variables:
    DB_TYPE: 'sqlite' or 'postgresql' (default: sqlite)
    DB_HOST: PostgreSQL host (default: localhost)
    DB_PORT: PostgreSQL port (default: 5432)
    DB_NAME: Database name (default: nlbi_dashboard)
    DB_USER: Database user (default: nlbi_readonly)
    DB_PASSWORD: Database password
    DB_ADMIN_USER: Admin user for schema creation (default: admin)
    DB_ADMIN_PASSWORD: Admin password
"""

import os
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from contextlib import contextmanager
from enum import Enum

# SQLAlchemy imports
from sqlalchemy import (
    create_engine, Engine, text, MetaData, Table, Column,
    Integer, String, Float, Date, ForeignKey, CheckConstraint,
    Index, select, inspect
)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

# Import python-dotenv for environment loading
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# =============================================================================
# Configuration
# =============================================================================

class DatabaseType(Enum):
    """Supported database types."""
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


# Database type from environment
DB_TYPE = DatabaseType(os.environ.get("DB_TYPE", "sqlite").lower())

# SQLite configuration
SQLITE_DB_PATH = Path(__file__).parent / "data" / "ecommerce.db"

# PostgreSQL configuration from environment
PG_HOST = os.environ.get("DB_HOST", "localhost")
PG_PORT = int(os.environ.get("DB_PORT", "5432"))
PG_NAME = os.environ.get("DB_NAME", "nlbi_dashboard")
PG_USER = os.environ.get("DB_USER", "nlbi_readonly")
PG_PASSWORD = os.environ.get("DB_PASSWORD", "nlbi_readonly_secret_2024")

# Admin credentials for schema creation
PG_ADMIN_USER = os.environ.get("DB_ADMIN_USER", "admin")
PG_ADMIN_PASSWORD = os.environ.get("DB_ADMIN_PASSWORD", "admin_secret_2024")

# Table definitions with allow-list for validation (per PRD FR-08)
ALLOWED_TABLES = {
    "customers": ["id", "name", "email", "signup_date", "region", "customer_segment"],
    "products": ["id", "name", "category", "price", "stock_quantity", "supplier"],
    "orders": ["id", "customer_id", "order_date", "total_amount", "status", "shipping_method"],
    "order_items": ["id", "order_id", "product_id", "quantity", "unit_price"],
}

# SQL keywords blocklist (per PRD FR-09)
BLOCKED_KEYWORDS = [
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE",
    "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE", "CALL"
]

# SQLAlchemy metadata
metadata = MetaData()
Base = declarative_base()


# =============================================================================
# Database Engine Factory
# =============================================================================

def get_connection_string(
    read_only: bool = True,
    admin: bool = False,
    db_type: Optional[DatabaseType] = None
) -> str:
    """
    Generate database connection string based on configuration.

    Args:
        read_only: If True, use read-only connection (SQLite URI mode or readonly user)
        admin: If True, use admin credentials (for schema creation)
        db_type: Override database type

    Returns:
        SQLAlchemy connection string

    Examples:
        >>> # SQLite read-only
        >>> get_connection_string(read_only=True)
        'sqlite:///data/ecommerce.db?mode=ro&uri=true'

        >>> # PostgreSQL read-only
        >>> get_connection_string(read_only=True, db_type=DatabaseType.POSTGRESQL)
        'postgresql://nlbi_readonly:***@localhost:5432/nlbi_dashboard'
    """
    db = db_type or DB_TYPE

    if db == DatabaseType.SQLITE:
        db_path = SQLITE_DB_PATH
        if read_only:
            return f"sqlite:///{db_path}?mode=ro&uri=true"
        else:
            return f"sqlite:///{db_path}"

    elif db == DatabaseType.POSTGRESQL:
        user = PG_ADMIN_USER if admin else PG_USER
        password = PG_ADMIN_PASSWORD if admin else PG_PASSWORD
        return f"postgresql://{user}:{password}@{PG_HOST}:{PG_PORT}/{PG_NAME}"

    else:
        raise ValueError(f"Unsupported database type: {db}")


def create_db_engine(
    read_only: bool = True,
    admin: bool = False,
    db_type: Optional[DatabaseType] = None,
    echo: bool = False
) -> Engine:
    """
    Create SQLAlchemy database engine.

    Args:
        read_only: Use read-only connection
        admin: Use admin credentials
        db_type: Override database type
        echo: Echo SQL statements (for debugging)

    Returns:
        SQLAlchemy Engine instance
    """
    conn_str = get_connection_string(read_only=read_only, admin=admin, db_type=db_type)

    # Engine configuration
    engine_kwargs = {
        "echo": echo,
        "pool_pre_ping": True,  # Verify connections before use
    }

    # PostgreSQL-specific configuration
    if (db_type or DB_TYPE) == DatabaseType.POSTGRESQL:
        engine_kwargs.update({
            "pool_size": 5,
            "max_overflow": 10,
            "pool_recycle": 3600,  # Recycle connections after 1 hour
        })
        # SSL mode for production
        if os.environ.get("DB_SSL_MODE", "prefer") != "disable":
            engine_kwargs["connect_args"] = {"sslmode": os.environ.get("DB_SSL_MODE", "prefer")}

    return create_engine(conn_str, **engine_kwargs)


# =============================================================================
# Schema Definition (SQLAlchemy)
# =============================================================================

def define_schema() -> Dict[str, Table]:
    """
    Define database schema using SQLAlchemy Core.

    Returns:
        Dictionary of Table objects keyed by table name
    """
    tables = {}

    # Customers table
    tables["customers"] = Table(
        "customers", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(255), nullable=False),
        Column("email", String(255), nullable=False, unique=True),
        Column("signup_date", Date, nullable=False),
        Column("region", String(100), nullable=False),
        Column("customer_segment", String(50), nullable=False),

        # Indexes
        Index("idx_customers_region", "region"),
        Index("idx_customers_segment", "customer_segment"),
    )

    # Products table
    tables["products"] = Table(
        "products", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(255), nullable=False),
        Column("category", String(100), nullable=False),
        Column("price", Float, nullable=False),
        Column("stock_quantity", Integer, nullable=False, default=0),
        Column("supplier", String(255), nullable=False),

        # Constraints
        CheckConstraint("price >= 0", name="ck_products_price"),
        CheckConstraint("stock_quantity >= 0", name="ck_products_stock"),

        # Indexes
        Index("idx_products_category", "category"),
        Index("idx_products_supplier", "supplier"),
    )

    # Orders table
    tables["orders"] = Table(
        "orders", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("customer_id", Integer, ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False),
        Column("order_date", Date, nullable=False),
        Column("total_amount", Float, nullable=False),
        Column("status", String(50), nullable=False, default="pending"),
        Column("shipping_method", String(50), nullable=False),

        # Constraints
        CheckConstraint("total_amount >= 0", name="ck_orders_total"),

        # Indexes
        Index("idx_orders_customer", "customer_id"),
        Index("idx_orders_date", "order_date"),
        Index("idx_orders_status", "status"),
    )

    # Order Items table
    tables["order_items"] = Table(
        "order_items", metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False),
        Column("product_id", Integer, ForeignKey("products.id", ondelete="RESTRICT"), nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("unit_price", Float, nullable=False),

        # Constraints
        CheckConstraint("quantity > 0", name="ck_order_items_quantity"),
        CheckConstraint("unit_price >= 0", name="ck_order_items_price"),

        # Indexes
        Index("idx_order_items_order", "order_id"),
        Index("idx_order_items_product", "product_id"),
    )

    return tables


# Define schema at module level
SCHEMA_TABLES = define_schema()


# =============================================================================
# Sample Data Generators
# =============================================================================

# Realistic sample data pools
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker"
]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
CUSTOMER_SEGMENTS = ["Enterprise", "Small Business", "Consumer", "Government"]
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food & Beverage"]
SUPPLIERS = ["Global Supplies Co.", "Premier Distributors", "Quality First Inc.", "Direct Source LLC", "Alpha Wholesale"]
ORDER_STATUSES = ["completed", "pending", "shipped", "cancelled", "refunded"]
SHIPPING_METHODS = ["Standard", "Express", "Next Day", "Economy", "Same Day"]

PRODUCT_NAMES = {
    "Electronics": [
        "Wireless Bluetooth Headphones", "4K Smart TV 55 inch", "Laptop Stand Aluminum",
        "USB-C Hub 7-in-1", "Mechanical Gaming Keyboard", "Wireless Mouse Ergonomic",
        "Portable Power Bank 20000mAh", "Smart Watch Fitness Tracker", "Tablet 10 inch",
        "Noise Cancelling Earbuds", "Webcam HD 1080p", "External SSD 1TB",
        "Smart Home Speaker", "Gaming Monitor 27 inch", "Wireless Charger Pad"
    ],
    "Clothing": [
        "Classic Cotton T-Shirt", "Denim Jeans Slim Fit", "Running Shoes Pro",
        "Winter Jacket Waterproof", "Business Casual Shirt", "Yoga Pants Premium",
        "Wool Sweater Classic", "Sports Shorts Athletic", "Leather Belt Premium",
        "Sun Hat Summer Edition", "Wool Scarf Winter", "Athletic Socks Pack"
    ],
    "Home & Garden": [
        "Indoor Plant Pot Set", "LED Desk Lamp Modern", "Memory Foam Pillow",
        "Stainless Steel Cookware Set", "Robot Vacuum Cleaner", "Air Purifier HEPA",
        "Coffee Maker Programmable", "Bed Sheet Set Egyptian Cotton", "Garden Tool Set",
        "Outdoor Solar Lights", "Kitchen Organizer Rack", "Bath Towel Set Premium"
    ],
    "Sports": [
        "Yoga Mat Premium", "Dumbbell Set Adjustable", "Resistance Bands Set",
        "Running Hydration Vest", "Cycling Helmet Pro", "Tennis Racket Pro",
        "Basketball Official Size", "Soccer Ball Match Quality", "Golf Club Set",
        "Camping Tent 4-Person", "Hiking Backpack 40L"
    ],
    "Books": [
        "Python Programming Guide", "Data Science Handbook", "Business Strategy Essentials",
        "Leadership Mastery", "Financial Intelligence", "Marketing in Digital Age",
        "The Art of Negotiation", "Startup Playbook", "Project Management Pro"
    ],
    "Toys": [
        "Building Blocks Set 500pc", "Remote Control Car Pro", "Educational Science Kit",
        "Board Game Strategy", "Puzzle 1000 Pieces", "Stuffed Animal Collection",
        "Art Set for Kids", "Drone with Camera"
    ],
    "Food & Beverage": [
        "Organic Coffee Beans 1kg", "Premium Tea Collection", "Dark Chocolate Assorted",
        "Olive Oil Extra Virgin", "Honey Organic Raw", "Snack Box Premium",
        "Spice Set Gourmet", "Protein Bars Pack"
    ]
}


def generate_customers(count: int = 60) -> List[Dict[str, Any]]:
    """Generate realistic customer records."""
    customers = []
    base_date = datetime(2022, 1, 1)

    for i in range(1, count + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@email.com"

        # Spread signup dates over 2 years
        signup_date = base_date + timedelta(days=random.randint(0, 730))

        customers.append({
            "id": i,
            "name": name,
            "email": email,
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "region": random.choice(REGIONS),
            "customer_segment": random.choice(CUSTOMER_SEGMENTS)
        })

    return customers


def generate_products(count: int = 60) -> List[Dict[str, Any]]:
    """Generate realistic product records."""
    products = []
    product_id = 1

    for category, names in PRODUCT_NAMES.items():
        for name in names:
            if product_id > count:
                break

            # Price ranges by category
            if category == "Electronics":
                price = round(random.uniform(29.99, 899.99), 2)
                stock = random.randint(50, 500)
            elif category == "Clothing":
                price = round(random.uniform(14.99, 149.99), 2)
                stock = random.randint(100, 1000)
            elif category == "Home & Garden":
                price = round(random.uniform(19.99, 399.99), 2)
                stock = random.randint(30, 300)
            elif category == "Sports":
                price = round(random.uniform(24.99, 299.99), 2)
                stock = random.randint(40, 400)
            elif category == "Books":
                price = round(random.uniform(9.99, 49.99), 2)
                stock = random.randint(50, 800)
            elif category == "Toys":
                price = round(random.uniform(12.99, 199.99), 2)
                stock = random.randint(60, 600)
            else:  # Food & Beverage
                price = round(random.uniform(4.99, 79.99), 2)
                stock = random.randint(100, 1500)

            products.append({
                "id": product_id,
                "name": name,
                "category": category,
                "price": price,
                "stock_quantity": stock,
                "supplier": random.choice(SUPPLIERS)
            })
            product_id += 1

    return products


def generate_orders(customers: List[Dict], count: int = 200) -> List[Dict[str, Any]]:
    """Generate realistic order records."""
    orders = []
    base_date = datetime(2023, 1, 1)

    for i in range(1, count + 1):
        customer = random.choice(customers)
        order_date = base_date + timedelta(days=random.randint(0, 450))

        # Order amounts vary by customer segment
        if customer["customer_segment"] == "Enterprise":
            total_amount = round(random.uniform(500, 5000), 2)
        elif customer["customer_segment"] == "Small Business":
            total_amount = round(random.uniform(100, 1500), 2)
        else:
            total_amount = round(random.uniform(25, 500), 2)

        # Status distribution (most orders completed)
        status = random.choices(
            ORDER_STATUSES,
            weights=[0.65, 0.10, 0.15, 0.07, 0.03]
        )[0]

        orders.append({
            "id": i,
            "customer_id": customer["id"],
            "order_date": order_date.strftime("%Y-%m-%d"),
            "total_amount": total_amount,
            "status": status,
            "shipping_method": random.choice(SHIPPING_METHODS)
        })

    return orders


def generate_order_items(orders: List[Dict], products: List[Dict], items_per_order: tuple = (1, 5)) -> List[Dict[str, Any]]:
    """Generate order items linking orders to products."""
    order_items = []
    item_id = 1

    for order in orders:
        # Skip cancelled/refunded orders mostly
        if order["status"] in ["cancelled", "refunded"] and random.random() < 0.7:
            continue

        num_items = random.randint(*items_per_order)

        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 5)

            # Unit price might have small discount from list price
            unit_price = round(product["price"] * random.uniform(0.9, 1.0), 2)

            order_items.append({
                "id": item_id,
                "order_id": order["id"],
                "product_id": product["id"],
                "quantity": quantity,
                "unit_price": unit_price
            })
            item_id += 1

    return order_items


# =============================================================================
# Schema Management
# =============================================================================

def create_schema(engine: Engine) -> None:
    """
    Create database schema using SQLAlchemy metadata.

    Args:
        engine: SQLAlchemy engine with admin privileges
    """
    # Create all tables defined in metadata
    metadata.create_all(engine)

    print("✓ Schema created successfully")

    # Print created tables
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"  Tables: {', '.join(tables)}")


def populate_data(engine: Engine) -> None:
    """
    Populate database with sample data.

    Args:
        engine: SQLAlchemy engine with write privileges
    """
    # Generate sample data
    print("Generating sample data...")
    customers = generate_customers(60)
    products = generate_products(60)
    orders = generate_orders(customers, 200)
    order_items = generate_order_items(orders, products)

    # Insert data using raw SQL for efficiency
    with engine.begin() as conn:
        # Insert customers
        conn.execute(
            text("""
                INSERT INTO customers (id, name, email, signup_date, region, customer_segment)
                VALUES (:id, :name, :email, :signup_date, :region, :customer_segment)
            """),
            customers
        )
        print(f"✓ Inserted {len(customers)} customers")

        # Insert products
        conn.execute(
            text("""
                INSERT INTO products (id, name, category, price, stock_quantity, supplier)
                VALUES (:id, :name, :category, :price, :stock_quantity, :supplier)
            """),
            products
        )
        print(f"✓ Inserted {len(products)} products")

        # Insert orders
        conn.execute(
            text("""
                INSERT INTO orders (id, customer_id, order_date, total_amount, status, shipping_method)
                VALUES (:id, :customer_id, :order_date, :total_amount, :status, :shipping_method)
            """),
            orders
        )
        print(f"✓ Inserted {len(orders)} orders")

        # Insert order items
        conn.execute(
            text("""
                INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
                VALUES (:id, :order_id, :product_id, :quantity, :unit_price)
            """),
            order_items
        )
        print(f"✓ Inserted {len(order_items)} order items")


# =============================================================================
# Connection Management
# =============================================================================

# Global engine cache
_engines: Dict[str, Engine] = {}


def get_db_engine(read_only: bool = True, admin: bool = False) -> Engine:
    """
    Get cached database engine.

    Args:
        read_only: Use read-only connection
        admin: Use admin credentials

    Returns:
        SQLAlchemy Engine instance
    """
    cache_key = f"{'ro' if read_only else 'rw'}_{'admin' if admin else 'user'}"

    if cache_key not in _engines:
        _engines[cache_key] = create_db_engine(read_only=read_only, admin=admin)

    return _engines[cache_key]


def get_db_connection(read_only: bool = True) -> Engine:
    """
    Get database connection (SQLAlchemy engine).

    This function maintains backward compatibility with the SQLite version.

    Args:
        read_only: If True, returns a read-only connection.

    Returns:
        SQLAlchemy Engine instance

    Example:
        >>> engine = get_db_connection(read_only=True)
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT * FROM customers LIMIT 5"))
        ...     print(result.fetchall())
    """
    engine = get_db_engine(read_only=read_only)

    # For SQLite, verify the database exists
    if DB_TYPE == DatabaseType.SQLITE:
        if not SQLITE_DB_PATH.exists():
            raise FileNotFoundError(
                f"Database not found at {SQLITE_DB_PATH}. "
                "Run 'python database_setup.py' to create it."
            )

    return engine


@contextmanager
def get_db_session(read_only: bool = True):
    """
    Context manager for SQLAlchemy session.

    Usage:
        with get_db_session(read_only=True) as session:
            result = session.execute(text("SELECT * FROM customers"))
            data = result.fetchall()
    """
    engine = get_db_engine(read_only=read_only)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        yield session
        if not read_only:
            session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_db_cursor(read_only: bool = True):
    """
    Context manager for database cursor (backward compatibility).

    Usage:
        with get_db_cursor(read_only=True) as cursor:
            cursor.execute("SELECT * FROM customers")
            results = cursor.fetchall()
    """
    engine = get_db_engine(read_only=read_only)
    conn = engine.connect()

    try:
        # Return a connection-like object that supports cursor operations
        yield conn
    finally:
        conn.close()


# =============================================================================
# Schema Introspection (for LLM context)
# =============================================================================

def get_schema_info() -> Dict[str, Any]:
    """
    Get database schema information for LLM context.

    This function returns table/column metadata WITHOUT any actual data,
    which is safe to send to LLM APIs (per PRD - no PII to external APIs).

    Returns:
        Dict with table names, columns, types, and sample valid values.
    """
    engine = get_db_engine(read_only=True)
    inspector = inspect(engine)

    schema_info = {}

    with engine.connect() as conn:
        for table_name in ALLOWED_TABLES.keys():
            # Get column info
            columns = inspector.get_columns(table_name)

            schema_info[table_name] = {
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable", True),
                        "primary_key": col.get("primary_key", False)
                    }
                    for col in columns
                ],
                "sample_values": {}
            }

            # Get sample distinct values for categorical columns
            for col in columns:
                col_name = col["name"]
                if col_name in ["region", "category", "status", "customer_segment",
                               "shipping_method", "supplier"]:
                    try:
                        result = conn.execute(
                            text(f"SELECT DISTINCT {col_name} FROM {table_name} LIMIT 10")
                        )
                        values = [row[0] for row in result.fetchall()]
                        schema_info[table_name]["sample_values"][col_name] = values
                    except Exception:
                        pass

    return schema_info


def get_schema_for_prompt() -> str:
    """
    Generate a formatted schema description for LLM prompts.

    This creates a concise, LLM-friendly description of the database schema
    that can be included in the system prompt for SQL generation.

    Note: Returns database-specific syntax hints based on DB_TYPE.
    """
    schema_info = get_schema_info()

    prompt = "You have access to the following database tables:\n\n"

    for table_name, info in schema_info.items():
        prompt += f"TABLE {table_name}:\n"
        for col in info["columns"]:
            pk_marker = " (PRIMARY KEY)" if col["primary_key"] else ""
            prompt += f"  - {col['name']}: {col['type']}{pk_marker}\n"

        if info["sample_values"]:
            prompt += "  Valid values:\n"
            for col, values in info["sample_values"].items():
                prompt += f"    {col}: {', '.join(map(str, values))}\n"
        prompt += "\n"

    # Add foreign key relationships
    prompt += "RELATIONSHIPS:\n"
    prompt += "  - orders.customer_id → customers.id\n"
    prompt += "  - order_items.order_id → orders.id\n"
    prompt += "  - order_items.product_id → products.id\n"

    # Add database-specific notes
    if DB_TYPE == DatabaseType.POSTGRESQL:
        prompt += "\nDATABASE-SPECIFIC NOTES (PostgreSQL):\n"
        prompt += "  - Date functions: Use DATE_TRUNC(), TO_CHAR(), EXTRACT()\n"
        prompt += "  - Boolean values: Use TRUE/FALSE\n"
        prompt += "  - String functions: Use || for concatenation, ILIKE for case-insensitive matching\n"
        prompt += "  - Use proper PostgreSQL syntax for all operations\n"
    else:
        prompt += "\nDATABASE-SPECIFIC NOTES (SQLite):\n"
        prompt += "  - Date functions: Use date(), strftime(), datetime()\n"
        prompt += "  - String functions: Use || for concatenation, LIKE for pattern matching\n"
        prompt += "  - Boolean values: SQLite uses 0 and 1 for false/true\n"

    return prompt


# =============================================================================
# Database Initialization
# =============================================================================

def init_database(db_type: Optional[DatabaseType] = None, force: bool = False) -> None:
    """
    Initialize the database with schema and sample data.

    Args:
        db_type: Database type to initialize (defaults to DB_TYPE)
        force: Force recreation even if database exists
    """
    db = db_type or DB_TYPE

    print("=" * 60)
    print(f"NL-BI Dashboard - Database Setup ({db.value.upper()})")
    print("=" * 60)

    if db == DatabaseType.SQLITE:
        # Ensure data directory exists
        SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Remove existing database if present or forced
        if SQLITE_DB_PATH.exists():
            if force:
                print(f"Removing existing database at {SQLITE_DB_PATH}")
                SQLITE_DB_PATH.unlink()
            else:
                print(f"Database already exists at {SQLITE_DB_PATH}")
                print("Use --force to recreate")
                return

        # Create admin engine for schema creation
        engine = create_db_engine(read_only=False, admin=True, db_type=db)

    elif db == DatabaseType.POSTGRESQL:
        # For PostgreSQL, connect as admin to create schema
        print(f"Connecting to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_NAME}")
        engine = create_db_engine(read_only=False, admin=True, db_type=db)

    else:
        raise ValueError(f"Unsupported database type: {db}")

    try:
        create_schema(engine)
        populate_data(engine)

        # Verify data
        with engine.connect() as conn:
            for table in ALLOWED_TABLES.keys():
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  {table}: {count} rows")

        print("\n✅ Database initialization complete!")

        if db == DatabaseType.SQLITE:
            print(f"   Database location: {SQLITE_DB_PATH}")

    finally:
        engine.dispose()


def verify_read_only() -> None:
    """Verify that read-only connection properly blocks writes."""
    print("\n🔍 Verifying read-only enforcement...")

    engine = get_db_engine(read_only=True)

    try:
        with engine.connect() as conn:
            # This should fail
            conn.execute(text(
                "INSERT INTO customers (name, email, signup_date, region, customer_segment) "
                "VALUES ('Test', 'test@test.com', '2024-01-01', 'Test', 'Test')"
            ))
            print("❌ ERROR: Write operation succeeded on read-only connection!")

    except Exception as e:
        error_msg = str(e).lower()
        if "readonly" in error_msg or "read-only" in error_msg or "permission" in error_msg:
            print("✅ Read-only enforcement working correctly!")
            print(f"   Error message: {e}")
        else:
            print(f"⚠️ Unexpected error: {e}")
    finally:
        engine.dispose()


def test_connection() -> bool:
    """
    Test database connection and verify tables exist.

    Returns:
        True if connection successful and tables exist, False otherwise
    """
    print("\n🔍 Testing database connection...")

    try:
        engine = get_db_engine(read_only=True)
        inspector = inspect(engine)

        # Check for expected tables
        existing_tables = set(inspector.get_table_names())
        expected_tables = set(ALLOWED_TABLES.keys())

        missing_tables = expected_tables - existing_tables

        if missing_tables:
            print(f"❌ Missing tables: {missing_tables}")
            return False

        print(f"✅ Connection successful!")
        print(f"   Database type: {DB_TYPE.value}")
        print(f"   Tables found: {', '.join(existing_tables)}")

        # Test a simple query
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM customers"))
            count = result.scalar()
            print(f"   Customers count: {count}")

        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NL-BI Dashboard Database Setup")
    parser.add_argument(
        "--db-type",
        choices=["sqlite", "postgresql"],
        default=DB_TYPE.value,
        help="Database type to use"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force recreation of database"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test database connection only"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify read-only enforcement"
    )

    args = parser.parse_args()

    # Convert string to enum
    selected_db = DatabaseType(args.db_type)

    if args.test:
        success = test_connection()
        exit(0 if success else 1)

    if args.verify:
        verify_read_only()
        exit(0)

    # Initialize database
    init_database(db_type=selected_db, force=args.force)

    # Verify read-only mode
    verify_read_only()

    # Show schema info
    print("\n📋 Schema for LLM Prompt:")
    print("-" * 40)
    print(get_schema_for_prompt())
