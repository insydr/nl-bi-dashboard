#!/usr/bin/env python3
"""
NL-BI Dashboard - Database Connection Test Script
==================================================

This script tests database connectivity and verifies the setup.

Usage:
    python test_db_connection.py [--db-type TYPE]

Examples:
    # Test SQLite connection (default)
    python test_db_connection.py

    # Test PostgreSQL connection
    python test_db_connection.py --db-type postgresql

    # Test with verbose output
    python test_db_connection.py --verbose
"""

import argparse
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

# Import sqlalchemy here
from sqlalchemy import text, inspect


def test_sqlite_connection():
    """Test SQLite database connection."""
    print("\n" + "=" * 60)
    print("Testing SQLite Connection")
    print("=" * 60)

    from database_setup import SQLITE_DB_PATH, get_db_engine, DB_TYPE, DatabaseType

    # Check if database file exists
    print(f"\nDatabase path: {SQLITE_DB_PATH}")
    
    if not SQLITE_DB_PATH.exists():
        print("❌ Database file does not exist!")
        print("\nTo create the database, run:")
        print("    python database_setup.py")
        return False

    print(f"✅ Database file exists ({SQLITE_DB_PATH.stat().st_size / 1024:.1f} KB)")

    # Test connection
    try:
        engine = get_db_engine(read_only=True)
        
        with engine.connect() as conn:
            # Test a simple query
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        print("✅ Connection successful!")
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

    # Verify tables
    try:
        engine = get_db_engine(read_only=True)
        inspector = inspect(engine)
        
        tables = inspector.get_table_names()
        expected = ['customers', 'products', 'orders', 'order_items']
        
        print(f"\nTables found: {', '.join(tables)}")
        
        missing = set(expected) - set(tables)
        if missing:
            print(f"❌ Missing tables: {missing}")
            return False
        
        print("✅ All expected tables exist!")
        
        # Check row counts
        print("\nRow counts:")
        with engine.connect() as conn:
            for table in expected:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  - {table}: {count} rows")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Table verification failed: {e}")
        return False

    # Test read-only enforcement
    print("\nTesting read-only enforcement...")
    try:
        engine = get_db_engine(read_only=True)
        with engine.connect() as conn:
            conn.execute(text(
                "INSERT INTO customers (name, email, signup_date, region, customer_segment) "
                "VALUES ('Test', 'test@test.com', '2024-01-01', 'Test', 'Test')"
            ))
        print("❌ Write succeeded - read-only NOT enforced!")
        engine.dispose()
        return False
        
    except Exception as e:
        if "readonly" in str(e).lower() or "read-only" in str(e).lower():
            print(f"✅ Read-only enforced: {e}")
        else:
            print(f"⚠️ Unexpected error: {e}")
        try:
            engine.dispose()
        except:
            pass

    return True


def test_postgresql_connection():
    """Test PostgreSQL database connection."""
    print("\n" + "=" * 60)
    print("Testing PostgreSQL Connection")
    print("=" * 60)

    from database_setup import (
        get_db_engine, DB_TYPE, DatabaseType,
        PG_HOST, PG_PORT, PG_NAME, PG_USER
    )

    # Show connection info
    print(f"\nConnection details:")
    print(f"  Host: {PG_HOST}")
    print(f"  Port: {PG_PORT}")
    print(f"  Database: {PG_NAME}")
    print(f"  User: {PG_USER}")

    # Test connection
    try:
        print("\nAttempting connection...")
        engine = get_db_engine(read_only=True)
        
        with engine.connect() as conn:
            # Test a simple query
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to PostgreSQL!")
            print(f"   Version: {version[:50]}...")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Is PostgreSQL running? Check: docker-compose ps")
        print("  2. Are credentials correct? Check .env file")
        print("  3. Is the database created? Run: python database_setup.py --db-type postgresql")
        return False

    # Verify tables
    try:
        engine = get_db_engine(read_only=True)
        inspector = inspect(engine)
        
        tables = inspector.get_table_names()
        expected = ['customers', 'products', 'orders', 'order_items']
        
        print(f"\nTables found: {', '.join(tables)}")
        
        missing = set(expected) - set(tables)
        if missing:
            print(f"❌ Missing tables: {missing}")
            print("\nTo create tables, run:")
            print("    python database_setup.py --db-type postgresql --force")
            return False
        
        print("✅ All expected tables exist!")
        
        # Check row counts
        print("\nRow counts:")
        with engine.connect() as conn:
            for table in expected:
                result = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"'))
                count = result.scalar()
                print(f"  - {table}: {count} rows")
        
        engine.dispose()
        
    except Exception as e:
        print(f"❌ Table verification failed: {e}")
        try:
            engine.dispose()
        except:
            pass
        return False

    # Test read-only user permissions
    print("\nTesting read-only user permissions...")
    try:
        engine = get_db_engine(read_only=True)
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE TABLE _test_write (id INTEGER)"
            ))
        print("❌ Write succeeded - read-only user has too many permissions!")
        engine.dispose()
        return False
        
    except Exception as e:
        if "permission" in str(e).lower() or "denied" in str(e).lower():
            print(f"✅ Read-only user cannot write: permission denied")
        else:
            print(f"✅ Write blocked: {type(e).__name__}")
        try:
            engine.dispose()
        except:
            pass

    return True


def test_docker_postgres():
    """Check if Docker PostgreSQL container is running."""
    print("\n" + "=" * 60)
    print("Checking Docker PostgreSQL Container")
    print("=" * 60)

    import subprocess

    try:
        result = subprocess.run(
            ["docker-compose", "ps"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        if result.returncode != 0:
            print("⚠️ docker-compose not available or not running")
            return False
        
        if "nlbi-postgres" in result.stdout and "Up" in result.stdout:
            print("✅ PostgreSQL container is running")
            return True
        else:
            print("❌ PostgreSQL container is not running")
            print("\nTo start the container:")
            print("    docker-compose up -d postgres")
            return False
            
    except FileNotFoundError:
        print("⚠️ docker-compose not found")
        print("  Install Docker Desktop or Docker Compose to use PostgreSQL")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test NL-BI Dashboard database connection"
    )
    parser.add_argument(
        "--db-type",
        choices=["sqlite", "postgresql"],
        default="sqlite",
        help="Database type to test (default: sqlite)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Test all database types"
    )
    
    args = parser.parse_args()

    print("=" * 60)
    print("NL-BI Dashboard - Database Connection Test")
    print("=" * 60)

    success = True

    if args.all:
        # Test SQLite first
        if not test_sqlite_connection():
            success = False
        
        # Then test PostgreSQL
        if not test_docker_postgres():
            success = False
        elif not test_postgresql_connection():
            success = False
    elif args.db_type == "postgresql":
        # Check Docker first for PostgreSQL
        if not test_docker_postgres():
            print("\n⚠️ Docker PostgreSQL not available. Testing connection anyway...")
        
        if not test_postgresql_connection():
            success = False
    else:
        if not test_sqlite_connection():
            success = False

    print("\n" + "=" * 60)
    if success:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
