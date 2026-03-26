-- =============================================================================
-- NL-BI Dashboard - PostgreSQL Initialization Script
-- =============================================================================
-- This script runs automatically when the PostgreSQL container starts for the
-- first time. It creates:
--   1. The application database
--   2. A read-only user for the NL-BI Dashboard
--   3. Proper permissions for security
--
-- Security Note:
-- The nlbi_readonly user has ONLY SELECT permissions on all tables.
-- This prevents any data modification through the dashboard.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Step 1: Create the database (if not exists)
-- -----------------------------------------------------------------------------
-- The database is already created via POSTGRES_DB environment variable,
-- but we include this for completeness when running manually.

-- SELECT 'CREATE DATABASE nlbi_dashboard'
-- WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'nlbi_dashboard')\gexec

-- -----------------------------------------------------------------------------
-- Step 2: Create Read-Only User
-- -----------------------------------------------------------------------------
-- This user will be used by the NL-BI Dashboard application
-- It has ONLY SELECT permissions - no INSERT, UPDATE, DELETE, or DDL operations

CREATE USER nlbi_readonly WITH PASSWORD 'nlbi_readonly_secret_2024';

-- Comment on the user for documentation
COMMENT ON ROLE nlbi_readonly IS 'Read-only user for NL-BI Dashboard application. Has SELECT permissions only.';

-- -----------------------------------------------------------------------------
-- Step 3: Grant Connection Permissions
-- -----------------------------------------------------------------------------
-- Allow the user to connect to the database

GRANT CONNECT ON DATABASE nlbi_dashboard TO nlbi_readonly;

-- Grant usage on the public schema
GRANT USAGE ON SCHEMA public TO nlbi_readonly;

-- -----------------------------------------------------------------------------
-- Step 4: Grant SELECT on All Existing Tables
-- -----------------------------------------------------------------------------
-- Grant SELECT permission on all existing tables

GRANT SELECT ON ALL TABLES IN SCHEMA public TO nlbi_readonly;

-- Grant SELECT on all sequences (needed for some queries)
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO nlbi_readonly;

-- -----------------------------------------------------------------------------
-- Step 5: Set Default Privileges for Future Tables
-- -----------------------------------------------------------------------------
-- This ensures any tables created in the future automatically get SELECT
-- permissions for the read-only user

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO nlbi_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO nlbi_readonly;

-- -----------------------------------------------------------------------------
-- Step 6: Revoke Unwanted Permissions
-- -----------------------------------------------------------------------------
-- Explicitly revoke any unwanted permissions (defense in depth)

REVOKE CREATE ON SCHEMA public FROM nlbi_readonly;
REVOKE TEMPORARY ON DATABASE nlbi_dashboard FROM nlbi_readonly;

-- -----------------------------------------------------------------------------
-- Step 7: Verification
-- -----------------------------------------------------------------------------
-- Log success (this will appear in PostgreSQL logs)

DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'NL-BI Dashboard Database Initialized';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Database: nlbi_dashboard';
    RAISE NOTICE 'Read-only user: nlbi_readonly';
    RAISE NOTICE 'Permissions: SELECT only on all tables';
    RAISE NOTICE '============================================';
END $$;

-- -----------------------------------------------------------------------------
-- Optional: Create an admin user for write operations
-- -----------------------------------------------------------------------------
-- Uncomment the following lines if you need a separate admin user

-- CREATE USER nlbi_admin WITH PASSWORD 'nlbi_admin_secret_2024';
-- GRANT ALL PRIVILEGES ON DATABASE nlbi_dashboard TO nlbi_admin;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO nlbi_admin;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO nlbi_admin;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO nlbi_admin;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO nlbi_admin;
