"""
Natural Language Business Intelligence Dashboard - Database Setup
================================================================

This module initializes the SQLite database with sample e-commerce data
for the NL-BI Dashboard MVP.

Features:
- Creates normalized schema with foreign key constraints
- Populates tables with realistic sample data (50+ rows per table)
- Provides read-only and read-write connection factories
- Includes schema introspection utilities for LLM context

Security Note:
SQLite doesn't have user-level permissions like PostgreSQL.
Read-only access is simulated via connection factory methods.
In production (PostgreSQL), we would use a dedicated read-only user.
"""

import sqlite3
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
from contextlib import contextmanager

# =============================================================================
# Configuration
# =============================================================================

# Database file path
DB_PATH = Path(__file__).parent / "data" / "ecommerce.db"

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
# Schema Creation
# =============================================================================

def create_schema(conn: sqlite3.Connection) -> None:
    """Create database tables with foreign key constraints."""
    cursor = conn.cursor()
    
    # Enable foreign key support (disabled by default in SQLite)
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            signup_date DATE NOT NULL,
            region TEXT NOT NULL,
            customer_segment TEXT NOT NULL
        );
    """)
    
    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL CHECK(price >= 0),
            stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK(stock_quantity >= 0),
            supplier TEXT NOT NULL
        );
    """)
    
    # Create orders table with foreign key to customers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            total_amount REAL NOT NULL CHECK(total_amount >= 0),
            status TEXT NOT NULL DEFAULT 'pending',
            shipping_method TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE RESTRICT
        );
    """)
    
    # Create order_items table with foreign keys to orders and products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK(quantity > 0),
            unit_price REAL NOT NULL CHECK(unit_price >= 0),
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT
        );
    """)
    
    # Create indexes for common query patterns
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_region ON customers(region);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);")
    
    conn.commit()
    print("✓ Schema created successfully")


def populate_data(conn: sqlite3.Connection) -> None:
    """Populate tables with sample data."""
    cursor = conn.cursor()
    
    # Generate sample data
    print("Generating sample data...")
    customers = generate_customers(60)
    products = generate_products(60)
    orders = generate_orders(customers, 200)
    order_items = generate_order_items(orders, products)
    
    # Insert customers
    cursor.executemany("""
        INSERT INTO customers (id, name, email, signup_date, region, customer_segment)
        VALUES (:id, :name, :email, :signup_date, :region, :customer_segment)
    """, customers)
    print(f"✓ Inserted {len(customers)} customers")
    
    # Insert products
    cursor.executemany("""
        INSERT INTO products (id, name, category, price, stock_quantity, supplier)
        VALUES (:id, :name, :category, :price, :stock_quantity, :supplier)
    """, products)
    print(f"✓ Inserted {len(products)} products")
    
    # Insert orders
    cursor.executemany("""
        INSERT INTO orders (id, customer_id, order_date, total_amount, status, shipping_method)
        VALUES (:id, :customer_id, :order_date, :total_amount, :status, :shipping_method)
    """, orders)
    print(f"✓ Inserted {len(orders)} orders")
    
    # Insert order items
    cursor.executemany("""
        INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
        VALUES (:id, :order_id, :product_id, :quantity, :unit_price)
    """, order_items)
    print(f"✓ Inserted {len(order_items)} order items")
    
    conn.commit()


# =============================================================================
# Connection Management
# =============================================================================

def get_db_connection(read_only: bool = True) -> sqlite3.Connection:
    """
    Get a database connection.
    
    Args:
        read_only: If True, returns a read-only connection (recommended for queries).
                   If False, returns a read-write connection (for admin tasks).
    
    Returns:
        sqlite3.Connection: Database connection object
    
    Security Note (per PRD FR-07):
        For SQLite, read-only mode is enforced via URI parameter.
        In PostgreSQL production, this would be a separate DB user with SELECT only.
    
    Example:
        >>> conn = get_db_connection(read_only=True)
        >>> cursor = conn.execute("SELECT * FROM customers LIMIT 5")
        >>> print(cursor.fetchall())
    """
    db_file = DB_PATH
    
    if not db_file.exists():
        raise FileNotFoundError(
            f"Database not found at {db_file}. "
            "Run 'python database_setup.py' to create it."
        )
    
    if read_only:
        # SQLite read-only mode via URI
        # This prevents any INSERT/UPDATE/DELETE operations at the driver level
        conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    else:
        # Read-write mode for administrative tasks
        conn = sqlite3.connect(str(db_file))
    
    # Enable foreign key enforcement
    conn.execute("PRAGMA foreign_keys = ON;")
    
    # Return rows as dictionaries for easier handling
    conn.row_factory = sqlite3.Row
    
    return conn


@contextmanager
def get_db_cursor(read_only: bool = True):
    """
    Context manager for database cursor with automatic cleanup.
    
    Usage:
        with get_db_cursor(read_only=True) as cursor:
            cursor.execute("SELECT * FROM customers")
            results = cursor.fetchall()
    """
    conn = get_db_connection(read_only=read_only)
    try:
        cursor = conn.cursor()
        yield cursor
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
    with get_db_cursor(read_only=True) as cursor:
        schema_info = {}
        
        for table_name in ALLOWED_TABLES.keys():
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            schema_info[table_name] = {
                "columns": [
                    {
                        "name": col[1],
                        "type": col[2],
                        "nullable": not col[3],
                        "primary_key": bool(col[5])
                    }
                    for col in columns
                ],
                "sample_values": {}
            }
            
            # Get sample distinct values for categorical columns
            for col in columns:
                col_name = col[1]
                if col_name in ["region", "category", "status", "customer_segment", 
                               "shipping_method", "supplier"]:
                    cursor.execute(
                        f"SELECT DISTINCT {col_name} FROM {table_name} LIMIT 10;"
                    )
                    values = [row[0] for row in cursor.fetchall()]
                    schema_info[table_name]["sample_values"][col_name] = values
        
        return schema_info


def get_schema_for_prompt() -> str:
    """
    Generate a formatted schema description for LLM prompts.
    
    This creates a concise, LLM-friendly description of the database schema
    that can be included in the system prompt for SQL generation.
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
    
    return prompt


# =============================================================================
# Database Initialization
# =============================================================================

def init_database() -> None:
    """Initialize the database with schema and sample data."""
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database if present
    if DB_PATH.exists():
        print(f"Removing existing database at {DB_PATH}")
        DB_PATH.unlink()
    
    # Create new database
    print(f"Creating database at {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    
    try:
        create_schema(conn)
        populate_data(conn)
        
        # Verify data
        cursor = conn.cursor()
        for table in ALLOWED_TABLES.keys():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count} rows")
        
        print("\n✅ Database initialization complete!")
        print(f"   Database location: {DB_PATH}")
        
    finally:
        conn.close()


def verify_read_only() -> None:
    """Verify that read-only connection properly blocks writes."""
    print("\n🔍 Verifying read-only enforcement...")
    
    try:
        conn = get_db_connection(read_only=True)
        cursor = conn.cursor()
        
        # This should fail
        cursor.execute("INSERT INTO customers (name, email, signup_date, region, customer_segment) "
                      "VALUES ('Test', 'test@test.com', '2024-01-01', 'Test', 'Test')")
        print("❌ ERROR: Write operation succeeded on read-only connection!")
        
    except sqlite3.OperationalError as e:
        if "readonly database" in str(e).lower():
            print("✅ Read-only enforcement working correctly!")
            print(f"   Error message: {e}")
        else:
            print(f"⚠️ Unexpected error: {e}")
    finally:
        try:
            conn.close()
        except:
            pass


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("NL-BI Dashboard - Database Setup")
    print("=" * 60)
    
    # Initialize database
    init_database()
    
    # Verify read-only mode
    verify_read_only()
    
    # Show schema info
    print("\n📋 Schema for LLM Prompt:")
    print("-" * 40)
    print(get_schema_for_prompt())
