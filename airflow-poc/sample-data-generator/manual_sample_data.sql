-- Manual Sample Data Creation for Airflow POC
-- Run this directly in psql if the Python scripts have issues

-- Connect to your database first:
-- psql -h localhost -U postgres -d local-intangles-core

-- Drop existing table if it exists
DROP TABLE IF EXISTS "sample-location-data";

-- Create the sample data table
CREATE TABLE "sample-location-data" (
    id SERIAL PRIMARY KEY,
    timestamp_column TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    speed DECIMAL(6, 2),
    temperature DECIMAL(5, 2),
    humidity DECIMAL(5, 2),
    battery_level INTEGER,
    city VARCHAR(50),
    country VARCHAR(50)
);

-- Create indexes for better performance
CREATE INDEX idx_sample_location_timestamp ON "sample-location-data" (timestamp_column DESC);
CREATE INDEX idx_sample_location_device ON "sample-location-data" (device_id);
CREATE INDEX idx_sample_location_city ON "sample-location-data" (city);

-- Insert sample data for Mumbai devices
INSERT INTO "sample-location-data" (timestamp_column, device_id, latitude, longitude, speed, temperature, humidity, battery_level, city, country)
SELECT 
    NOW() - (random() * INTERVAL '72 hours') as timestamp_column,
    'MUM_' || LPAD((ROW_NUMBER() OVER() % 5 + 1)::text, 3, '0') as device_id,
    19.0760 + (random() - 0.5) * 0.1 as latitude,
    72.8777 + (random() - 0.5) * 0.1 as longitude,
    random() * 60 as speed,
    20 + random() * 15 as temperature,
    40 + random() * 40 as humidity,
    20 + (random() * 80)::int as battery_level,
    'Mumbai' as city,
    'India' as country
FROM generate_series(1, 5000);

-- Insert sample data for Delhi devices  
INSERT INTO "sample-location-data" (timestamp_column, device_id, latitude, longitude, speed, temperature, humidity, battery_level, city, country)
SELECT 
    NOW() - (random() * INTERVAL '72 hours') as timestamp_column,
    'DEL_' || LPAD((ROW_NUMBER() OVER() % 5 + 1)::text, 3, '0') as device_id,
    28.6139 + (random() - 0.5) * 0.1 as latitude,
    77.2090 + (random() - 0.5) * 0.1 as longitude,
    random() * 60 as speed,
    18 + random() * 17 as temperature,
    30 + random() * 50 as humidity,
    20 + (random() * 80)::int as battery_level,
    'Delhi' as city,
    'India' as country
FROM generate_series(1, 5000);

-- Insert sample data for Bangalore devices
INSERT INTO "sample-location-data" (timestamp_column, device_id, latitude, longitude, speed, temperature, humidity, battery_level, city, country)
SELECT 
    NOW() - (random() * INTERVAL '72 hours') as timestamp_column,
    'BLR_' || LPAD((ROW_NUMBER() OVER() % 5 + 1)::text, 3, '0') as device_id,
    12.9716 + (random() - 0.5) * 0.1 as latitude,
    77.5946 + (random() - 0.5) * 0.1 as longitude,
    random() * 60 as speed,
    22 + random() * 13 as temperature,
    50 + random() * 30 as humidity,
    20 + (random() * 80)::int as battery_level,
    'Bangalore' as city,
    'India' as country
FROM generate_series(1, 5000);

-- Verify the data was created
SELECT 
    city,
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    MIN(timestamp_column) as earliest_record,
    MAX(timestamp_column) as latest_record
FROM "sample-location-data"
GROUP BY city
ORDER BY city;

-- Test the Airflow DAG query
SELECT 
    DATE_TRUNC('hour', timestamp_column) as hour_bucket,
    device_id,
    ROUND(AVG(latitude)::numeric, 6) as avg_latitude,
    ROUND(AVG(longitude)::numeric, 6) as avg_longitude,
    ROUND(AVG(speed)::numeric, 2) as avg_speed,
    ROUND(AVG(temperature)::numeric, 2) as avg_temperature,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
    ROUND(AVG(battery_level)::numeric, 0) as avg_battery_level,
    COUNT(*) as record_count,
    MAX(timestamp_column) as max_timestamp,
    MIN(timestamp_column) as min_timestamp,
    city,
    country
FROM "sample-location-data" 
WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', timestamp_column), device_id, city, country
ORDER BY hour_bucket DESC, device_id
LIMIT 10;

-- Display final summary
\echo ''
\echo '🎉 Sample data creation completed!'
\echo '📊 Created 15,000 records across 3 cities'
\echo '🏙️ Cities: Mumbai (5000), Delhi (5000), Bangalore (5000)'
\echo '📱 Devices: 15 total (5 per city)'
\echo '⏰ Time range: Last 72 hours'
\echo ''
\echo '✅ Ready for Airflow DAG testing!'