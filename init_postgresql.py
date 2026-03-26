#!/usr/bin/env python3
"""
NL-BI Dashboard - PostgreSQL Migration Script
==============================================

This script initializes the PostgreSQL database with:
1. Creates tables (customers, products, orders, order_items, query_logs)
2. Populates with sample e-commerce data

Usage:
    python init_postgresql.py [--skip-data]

Options:
    --skip-data    Only create schema, skip sample data population
"""

import os
import sys
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration from environment
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "nlbi_dashboard")
DB_USER = os.environ.get("DB_ADMIN_USER", os.environ.get("DB_USER", "admin"))
DB_PASSWORD = os.environ.get("DB_ADMIN_PASSWORD", os.environ.get("DB_PASSWORD", "admin"))
DB_SSL_MODE = os.environ.get("DB_SSL_MODE", "require")


def get_connection_string():
    """Build PostgreSQL connection string."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSL_MODE}"


def test_connection():
    """Test database connection."""
    import sqlalchemy
    from sqlalchemy import create_engine, text
    
    print("\n🔍 Testing PostgreSQL connection...")
    print(f"   Host: {DB_HOST}")
    print(f"   Port: {DB_PORT}")
    print(f"   Database: {DB_NAME}")
    print(f"   User: {DB_USER}")
    print(f"   SSL Mode: {DB_SSL_MODE}")
    
    conn_str = get_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"\n✅ Connection successful!")
            print(f"   PostgreSQL version: {version[:50]}...")
            return True
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return False
    finally:
        engine.dispose()


def create_schema():
    """Create database schema."""
    from sqlalchemy import create_engine, text
    
    print("\n📋 Creating database schema...")
    
    conn_str = get_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True)
    
    # SQL statements for schema creation
    schema_sql = """
    -- Customers table
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        signup_date DATE NOT NULL,
        region VARCHAR(100) NOT NULL,
        customer_segment VARCHAR(50) NOT NULL
    );
    
    -- Products table
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(100) NOT NULL,
        price DOUBLE PRECISION NOT NULL CHECK (price >= 0),
        stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
        supplier VARCHAR(255) NOT NULL
    );
    
    -- Orders table
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE RESTRICT,
        order_date DATE NOT NULL,
        total_amount DOUBLE PRECISION NOT NULL CHECK (total_amount >= 0),
        status VARCHAR(50) NOT NULL DEFAULT 'pending',
        shipping_method VARCHAR(50) NOT NULL
    );
    
    -- Order Items table
    CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
        product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price DOUBLE PRECISION NOT NULL CHECK (unit_price >= 0)
    );
    
    -- Query Logs table
    CREATE TABLE IF NOT EXISTS query_logs (
        id SERIAL PRIMARY KEY,
        user_question VARCHAR(1000) NOT NULL,
        generated_sql VARCHAR(5000),
        timestamp VARCHAR(50) NOT NULL,
        feedback VARCHAR(20),
        row_count INTEGER,
        success INTEGER NOT NULL DEFAULT 1,
        error_message VARCHAR(500),
        execution_time_ms INTEGER
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_customers_region ON customers(region);
    CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(customer_segment);
    CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
    CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
    CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
    CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
    CREATE INDEX IF NOT EXISTS idx_query_logs_timestamp ON query_logs(timestamp);
    """
    
    try:
        with engine.connect() as conn:
            # Execute each statement separately
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            for stmt in statements:
                if stmt:
                    conn.execute(text(stmt))
            conn.commit()
            print("✅ Schema created successfully!")
            return True
    except Exception as e:
        print(f"❌ Schema creation failed: {e}")
        return False
    finally:
        engine.dispose()


def populate_sample_data():
    """Populate database with sample data."""
    import random
    from sqlalchemy import create_engine, text
    
    print("\n📊 Populating sample data...")
    
    # Sample data pools
    FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                   "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"]
    LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"]
    REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"]
    SEGMENTS = ["Enterprise", "Small Business", "Consumer", "Government"]
    CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food & Beverage"]
    SUPPLIERS = ["Global Supplies Co.", "Premier Distributors", "Quality First Inc.", "Direct Source LLC"]
    STATUSES = ["completed", "pending", "shipped", "cancelled", "refunded"]
    SHIPPING = ["Standard", "Express", "Next Day", "Economy", "Same Day"]
    
    PRODUCT_NAMES = {
        "Electronics": ["Wireless Headphones", "4K Smart TV", "Laptop Stand", "USB-C Hub", "Gaming Keyboard"],
        "Clothing": ["Cotton T-Shirt", "Denim Jeans", "Running Shoes", "Winter Jacket", "Business Shirt"],
        "Home & Garden": ["Plant Pot Set", "LED Desk Lamp", "Memory Foam Pillow", "Cookware Set", "Robot Vacuum"],
        "Sports": ["Yoga Mat", "Dumbbell Set", "Resistance Bands", "Cycling Helmet", "Tennis Racket"],
        "Books": ["Python Guide", "Data Science Handbook", "Business Strategy", "Leadership Mastery"],
        "Toys": ["Building Blocks", "RC Car", "Science Kit", "Board Game", "Puzzle 1000pc"],
        "Food & Beverage": ["Organic Coffee", "Premium Tea", "Dark Chocolate", "Olive Oil", "Organic Honey"]
    }
    
    conn_str = get_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Generate and insert customers
            print("   Generating customers...")
            base_date = datetime(2022, 1, 1)
            customers = []
            for i in range(1, 61):
                first = random.choice(FIRST_NAMES)
                last = random.choice(LAST_NAMES)
                signup = base_date + timedelta(days=random.randint(0, 730))
                customers.append({
                    "id": i,
                    "name": f"{first} {last}",
                    "email": f"{first.lower()}.{last.lower()}{random.randint(1,99)}@email.com",
                    "signup_date": signup.strftime("%Y-%m-%d"),
                    "region": random.choice(REGIONS),
                    "segment": random.choice(SEGMENTS)
                })
            
            conn.execute(text("""
                INSERT INTO customers (id, name, email, signup_date, region, customer_segment)
                VALUES (:id, :name, :email, :signup_date, :region, :segment)
                ON CONFLICT (id) DO NOTHING
            """), customers)
            print(f"   ✅ Inserted {len(customers)} customers")
            
            # Generate and insert products
            print("   Generating products...")
            products = []
            pid = 1
            for category, names in PRODUCT_NAMES.items():
                for name in names:
                    price = round(random.uniform(9.99, 899.99), 2)
                    stock = random.randint(50, 500)
                    products.append({
                        "id": pid,
                        "name": name,
                        "category": category,
                        "price": price,
                        "stock_quantity": stock,
                        "supplier": random.choice(SUPPLIERS)
                    })
                    pid += 1
            
            conn.execute(text("""
                INSERT INTO products (id, name, category, price, stock_quantity, supplier)
                VALUES (:id, :name, :category, :price, :stock_quantity, :supplier)
                ON CONFLICT (id) DO NOTHING
            """), products)
            print(f"   ✅ Inserted {len(products)} products")
            
            # Generate and insert orders
            print("   Generating orders...")
            base_date = datetime(2023, 1, 1)
            orders = []
            for i in range(1, 201):
                customer = random.choice(customers)
                order_date = base_date + timedelta(days=random.randint(0, 450))
                amount = round(random.uniform(25, 5000), 2)
                orders.append({
                    "id": i,
                    "customer_id": customer["id"],
                    "order_date": order_date.strftime("%Y-%m-%d"),
                    "total_amount": amount,
                    "status": random.choices(STATUSES, weights=[0.65, 0.10, 0.15, 0.07, 0.03])[0],
                    "shipping_method": random.choice(SHIPPING)
                })
            
            conn.execute(text("""
                INSERT INTO orders (id, customer_id, order_date, total_amount, status, shipping_method)
                VALUES (:id, :customer_id, :order_date, :total_amount, :status, :shipping_method)
                ON CONFLICT (id) DO NOTHING
            """), orders)
            print(f"   ✅ Inserted {len(orders)} orders")
            
            # Generate and insert order items
            print("   Generating order items...")
            order_items = []
            item_id = 1
            for order in orders:
                if order["status"] in ["cancelled", "refunded"] and random.random() < 0.7:
                    continue
                for _ in range(random.randint(1, 5)):
                    product = random.choice(products)
                    order_items.append({
                        "id": item_id,
                        "order_id": order["id"],
                        "product_id": product["id"],
                        "quantity": random.randint(1, 5),
                        "unit_price": round(product["price"] * random.uniform(0.9, 1.0), 2)
                    })
                    item_id += 1
            
            conn.execute(text("""
                INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
                VALUES (:id, :order_id, :product_id, :quantity, :unit_price)
                ON CONFLICT (id) DO NOTHING
            """), order_items)
            print(f"   ✅ Inserted {len(order_items)} order items")
            
            conn.commit()
            return True
            
    except Exception as e:
        print(f"❌ Data population failed: {e}")
        return False
    finally:
        engine.dispose()


def verify_data():
    """Verify data was inserted correctly."""
    from sqlalchemy import create_engine, text
    
    print("\n🔍 Verifying data...")
    
    conn_str = get_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True)
    
    tables = ["customers", "products", "orders", "order_items", "query_logs"]
    
    try:
        with engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"   {table}: {count} rows")
        print("\n✅ Migration completed successfully!")
        return True
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        engine.dispose()


def main():
    """Main migration function."""
    print("=" * 60)
    print("NL-BI Dashboard - PostgreSQL Migration")
    print("=" * 60)
    
    skip_data = "--skip-data" in sys.argv
    
    # Step 1: Test connection
    if not test_connection():
        print("\n❌ Migration aborted: Could not connect to database")
        sys.exit(1)
    
    # Step 2: Create schema
    if not create_schema():
        print("\n❌ Migration aborted: Schema creation failed")
        sys.exit(1)
    
    # Step 3: Populate data (unless --skip-data)
    if not skip_data:
        if not populate_sample_data():
            print("\n❌ Migration aborted: Data population failed")
            sys.exit(1)
    
    # Step 4: Verify
    verify_data()
    
    print("\n" + "=" * 60)
    print("🎉 PostgreSQL migration complete!")
    print("=" * 60)
    print("\nTo run the dashboard with PostgreSQL:")
    print("   streamlit run app.py")
    print("\nEnvironment is already configured in .env file")


if __name__ == "__main__":
    main()
