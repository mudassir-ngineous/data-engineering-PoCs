-- Database setup script for Airflow
-- Run this in your PostgreSQL instance

-- Create Airflow database
CREATE DATABASE airflow;

-- Create Airflow user (optional - you can use existing user)
-- CREATE USER airflow_user WITH PASSWORD 'your_password';
-- GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow_user;

-- Connect to airflow database
\c airflow;

-- The Airflow init process will create all necessary tables automatically
-- This script is just to ensure the database exists

-- For TimescaleDB setup (if you need sample data for testing)
-- Uncomment and modify the following based on your actual schema:

/*
-- Sample table structure for TimescaleDB
CREATE TABLE IF NOT EXISTS sensor_data (
    timestamp_column TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    value_column DOUBLE PRECISION,
    location VARCHAR(50),
    metadata JSONB
);

-- Convert to hypertable (TimescaleDB specific)
SELECT create_hypertable('sensor_data', 'timestamp_column');

-- Add compression policy (optional)
ALTER TABLE sensor_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);

SELECT add_compression_policy('sensor_data', INTERVAL '7 days');

-- Insert sample data
INSERT INTO sensor_data (timestamp_column, sensor_id, value_column, location, metadata)
SELECT 
    NOW() - (random() * INTERVAL '30 days'),
    (random() * 100)::int,
    random() * 100,
    'Location_' || (random() * 10)::int,
    '{"type": "temperature", "unit": "celsius"}'::jsonb
FROM generate_series(1, 10000);
*/