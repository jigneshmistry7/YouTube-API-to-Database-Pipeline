-- Database initialization script
-- This script runs when PostgreSQL container starts

-- Create additional databases for different environments
CREATE DATABASE youtube_analytics_test;
CREATE DATABASE youtube_analytics_dev;

-- Create additional user for testing
CREATE USER test_user WITH PASSWORD 'test_password';
GRANT ALL PRIVILEGES ON DATABASE youtube_analytics_test TO test_user;
GRANT ALL PRIVILEGES ON DATABASE youtube_analytics_dev TO test_user;

-- Create read-only user for reporting
CREATE USER report_user WITH PASSWORD 'report_password';
GRANT CONNECT ON DATABASE youtube_analytics TO report_user;
GRANT USAGE ON SCHEMA public TO report_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO report_user;

-- Set up extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create schema for raw data (if needed)
CREATE SCHEMA IF NOT EXISTS raw_data;
GRANT USAGE ON SCHEMA raw_data TO youtube_user;
GRANT USAGE ON SCHEMA raw_data TO report_user;