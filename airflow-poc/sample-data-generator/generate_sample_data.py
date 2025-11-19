#!/usr/bin/env python3
"""
Sample Geospatial Time-Series Data Generator for TimescaleDB
Generates 1 million records of sample location data with timestamps
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import argparse
import sys
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker for generating realistic data
fake = Faker()

def create_database_connection(host='localhost', port=5432, database='local-intangles-core', 
                             user='postgres', password=None):
    """Create database connection"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def create_sample_table(conn):
    """Create the sample-location-data table with proper schema"""
    create_table_sql = """
    DROP TABLE IF EXISTS "sample-location-data";
    
    CREATE TABLE "sample-location-data" (
        id SERIAL PRIMARY KEY,
        timestamp_column TIMESTAMPTZ NOT NULL,
        device_id VARCHAR(50) NOT NULL,
        latitude DECIMAL(10, 8) NOT NULL,
        longitude DECIMAL(11, 8) NOT NULL,
        altitude DECIMAL(8, 2),
        speed DECIMAL(6, 2),
        heading DECIMAL(5, 2),
        accuracy DECIMAL(6, 2),
        battery_level INTEGER,
        temperature DECIMAL(5, 2),
        humidity DECIMAL(5, 2),
        pressure DECIMAL(7, 2),
        location_name VARCHAR(100),
        country VARCHAR(50),
        city VARCHAR(50),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    -- Create basic indexes for better query performance
    CREATE INDEX idx_sample_location_device_time ON "sample-location-data" (device_id, timestamp_column DESC);
    CREATE INDEX idx_sample_location_timestamp ON "sample-location-data" (timestamp_column DESC);
    CREATE INDEX idx_sample_location_city ON "sample-location-data" (city);
    """
    
    # Try to create TimescaleDB hypertable (optional)
    timescale_sql = """
    -- Try to create hypertable (TimescaleDB extension)
    DO $$ 
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
            PERFORM create_hypertable('"sample-location-data"', 'timestamp_column', 
                                   chunk_time_interval => INTERVAL '1 day');
            
            -- Add compression policy (compress data older than 7 days)
            ALTER TABLE "sample-location-data" SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'device_id',
                timescaledb.compress_orderby = 'timestamp_column DESC'
            );
            
            PERFORM add_compression_policy('"sample-location-data"', INTERVAL '7 days');
        END IF;
    EXCEPTION 
        WHEN OTHERS THEN
            -- TimescaleDB not available, continue with regular table
            NULL;
    END $$;
    """
    
    try:
        cursor = conn.cursor()
        # Create basic table
        cursor.execute(create_table_sql)
        
        # Try to add TimescaleDB features if available
        cursor.execute(timescale_sql)
        
        conn.commit()
        cursor.close()
        logger.info("✅ Table 'sample-location-data' created successfully")
        return True
    except psycopg2.Error as e:
        logger.error(f"❌ Error creating table: {e}")
        conn.rollback()
        return False

def generate_device_routes():
    """Generate realistic device routes for major cities"""
    # Major cities with their approximate coordinates
    cities = [
        {"name": "Mumbai", "country": "India", "lat": 19.0760, "lon": 72.8777, "radius": 0.5},
        {"name": "Delhi", "country": "India", "lat": 28.6139, "lon": 77.2090, "radius": 0.4},
        {"name": "Bangalore", "country": "India", "lat": 12.9716, "lon": 77.5946, "radius": 0.3},
        {"name": "Chennai", "country": "India", "lat": 13.0827, "lon": 80.2707, "radius": 0.3},
        {"name": "Hyderabad", "country": "India", "lat": 17.3850, "lon": 78.4867, "radius": 0.3},
        {"name": "Pune", "country": "India", "lat": 18.5204, "lon": 73.8567, "radius": 0.25},
        {"name": "Kolkata", "country": "India", "lat": 22.5726, "lon": 88.3639, "radius": 0.3},
        {"name": "Ahmedabad", "country": "India", "lat": 23.0225, "lon": 72.5714, "radius": 0.25},
        {"name": "Jaipur", "country": "India", "lat": 26.9124, "lon": 75.7873, "radius": 0.2},
        {"name": "Surat", "country": "India", "lat": 21.1702, "lon": 72.8311, "radius": 0.2},
    ]
    
    routes = []
    for city in cities:
        # Generate 50-100 devices per city
        num_devices = random.randint(50, 100)
        for i in range(num_devices):
            device_id = f"{city['name'][:3].upper()}_{i+1:03d}"
            routes.append({
                'device_id': device_id,
                'city': city['name'],
                'country': city['country'],
                'base_lat': city['lat'],
                'base_lon': city['lon'],
                'radius': city['radius']
            })
    
    return routes

def generate_realistic_coordinates(base_lat, base_lon, radius, prev_lat=None, prev_lon=None):
    """Generate realistic GPS coordinates that simulate vehicle movement"""
    if prev_lat is None or prev_lon is None:
        # First point - random within city radius
        angle = random.uniform(0, 2 * np.pi)
        distance = random.uniform(0, radius)
        lat = base_lat + (distance * np.cos(angle))
        lon = base_lon + (distance * np.sin(angle))
    else:
        # Subsequent points - small movement from previous location
        max_movement = 0.002  # ~200 meters max movement per reading
        lat_change = random.uniform(-max_movement, max_movement)
        lon_change = random.uniform(-max_movement, max_movement)
        
        lat = prev_lat + lat_change
        lon = prev_lon + lon_change
        
        # Keep within city bounds
        if abs(lat - base_lat) > radius:
            lat = base_lat + (radius * 0.8 * (1 if lat > base_lat else -1))
        if abs(lon - base_lon) > radius:
            lon = base_lon + (radius * 0.8 * (1 if lon > base_lon else -1))
    
    return round(lat, 6), round(lon, 6)

def generate_sample_data(num_records=1000000):
    """Generate sample geospatial time-series data"""
    logger.info(f"🔄 Generating {num_records:,} sample records...")
    
    routes = generate_device_routes()
    data = []
    
    # Time range: last 30 days
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)
    
    # Track last position for each device to ensure realistic movement
    device_positions = {}
    
    # Calculate records per device
    records_per_device = num_records // len(routes)
    logger.info(f"📊 Generating ~{records_per_device:,} records per device across {len(routes)} devices")
    
    with tqdm(total=num_records, desc="Generating records") as pbar:
        for route in routes:
            device_id = route['device_id']
            
            for i in range(records_per_device):
                # Generate timestamp (more recent data has higher frequency)
                days_ago = random.uniform(0, 30)
                timestamp = end_time - timedelta(days=days_ago)
                
                # Get previous position for realistic movement
                prev_pos = device_positions.get(device_id, (None, None))
                
                # Generate coordinates
                lat, lon = generate_realistic_coordinates(
                    route['base_lat'], 
                    route['base_lon'], 
                    route['radius'],
                    prev_pos[0], 
                    prev_pos[1]
                )
                
                # Store current position for next iteration
                device_positions[device_id] = (lat, lon)
                
                # Generate other realistic sensor data
                record = {
                    'timestamp_column': timestamp,
                    'device_id': device_id,
                    'latitude': lat,
                    'longitude': lon,
                    'altitude': round(random.uniform(10, 500), 2),
                    'speed': round(random.uniform(0, 80), 2),  # km/h
                    'heading': round(random.uniform(0, 360), 2),
                    'accuracy': round(random.uniform(1, 15), 2),  # meters
                    'battery_level': random.randint(10, 100),
                    'temperature': round(random.uniform(15, 45), 2),  # Celsius
                    'humidity': round(random.uniform(30, 90), 2),  # %
                    'pressure': round(random.uniform(980, 1030), 2),  # hPa
                    'location_name': fake.street_address(),
                    'country': route['country'],
                    'city': route['city']
                }
                
                data.append(record)
                pbar.update(1)
    
    logger.info(f"✅ Generated {len(data):,} sample records")
    return data

def insert_data_batch(conn, data, batch_size=10000):
    """Insert data in batches for better performance"""
    logger.info(f"🔄 Inserting {len(data):,} records in batches of {batch_size:,}...")
    
    insert_sql = """
    INSERT INTO "sample-location-data" (
        timestamp_column, device_id, latitude, longitude, altitude, speed, 
        heading, accuracy, battery_level, temperature, humidity, pressure,
        location_name, country, city
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    cursor = conn.cursor()
    
    try:
        with tqdm(total=len(data), desc="Inserting records") as pbar:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # Prepare batch data
                batch_values = [
                    (
                        record['timestamp_column'],
                        record['device_id'],
                        record['latitude'],
                        record['longitude'],
                        record['altitude'],
                        record['speed'],
                        record['heading'],
                        record['accuracy'],
                        record['battery_level'],
                        record['temperature'],
                        record['humidity'],
                        record['pressure'],
                        record['location_name'],
                        record['country'],
                        record['city']
                    ) for record in batch
                ]
                
                # Execute batch insert
                cursor.executemany(insert_sql, batch_values)
                conn.commit()
                
                pbar.update(len(batch))
                
        logger.info("✅ Data insertion completed successfully")
        
    except psycopg2.Error as e:
        logger.error(f"❌ Error inserting data: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def create_sample_queries_file():
    """Create a file with sample queries to test the data"""
    queries = """
-- Sample queries for testing the sample-location-data table

-- 1. Get recent data for a specific device
SELECT * FROM "sample-location-data" 
WHERE device_id = 'MUM_001' 
ORDER BY timestamp_column DESC 
LIMIT 10;

-- 2. Count records per city
SELECT city, COUNT(*) as record_count
FROM "sample-location-data"
GROUP BY city
ORDER BY record_count DESC;

-- 3. Average speed by city (last 7 days)
SELECT city, AVG(speed) as avg_speed
FROM "sample-location-data"
WHERE timestamp_column >= NOW() - INTERVAL '7 days'
GROUP BY city
ORDER BY avg_speed DESC;

-- 4. Time-bucketed aggregation (hourly averages for last 24 hours)
SELECT 
    time_bucket('1 hour', timestamp_column) as hour_bucket,
    city,
    AVG(speed) as avg_speed,
    AVG(temperature) as avg_temperature,
    COUNT(*) as record_count
FROM "sample-location-data"
WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
GROUP BY hour_bucket, city
ORDER BY hour_bucket DESC, city;

-- 5. Find devices with low battery in specific area
SELECT device_id, latitude, longitude, battery_level, timestamp_column
FROM "sample-location-data"
WHERE battery_level < 20
AND latitude BETWEEN 19.0 AND 19.2
AND longitude BETWEEN 72.8 AND 73.0
ORDER BY timestamp_column DESC;

-- 6. Geospatial query - devices within 5km of a point (Mumbai center)
SELECT device_id, latitude, longitude, 
       earth_distance(ll_to_earth(19.0760, 72.8777), ll_to_earth(latitude, longitude)) as distance_meters
FROM "sample-location-data"
WHERE earth_box(ll_to_earth(19.0760, 72.8777), 5000) @> ll_to_earth(latitude, longitude)
AND timestamp_column >= NOW() - INTERVAL '1 hour'
ORDER BY distance_meters;

-- 7. Compressed chunks information
SELECT chunk_name, range_start, range_end, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sample-location-data'
ORDER BY range_start DESC;

-- 8. Data for Airflow ETL (last 24 hours, hourly buckets)
SELECT 
    time_bucket('1 hour', timestamp_column) as hour_bucket,
    device_id,
    AVG(latitude) as avg_latitude,
    AVG(longitude) as avg_longitude,
    AVG(speed) as avg_speed,
    AVG(temperature) as avg_temperature,
    COUNT(*) as record_count,
    MAX(timestamp_column) as max_timestamp,
    MIN(timestamp_column) as min_timestamp
FROM "sample-location-data" 
WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
GROUP BY hour_bucket, device_id
ORDER BY hour_bucket DESC, device_id;
"""
    
    with open('sample_queries.sql', 'w') as f:
        f.write(queries)
    
    logger.info("📝 Sample queries saved to 'sample_queries.sql'")

def main():
    parser = argparse.ArgumentParser(description='Generate sample geospatial time-series data')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='local-intangles-core', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--records', type=int, default=1000000, help='Number of records to generate')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for inserts')
    parser.add_argument('--skip-table-creation', action='store_true', help='Skip table creation')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting sample data generation...")
    logger.info(f"📊 Target: {args.records:,} records")
    logger.info(f"🎯 Database: {args.host}:{args.port}/{args.database}")
    
    # Get password if not provided
    if not args.password:
        import getpass
        args.password = getpass.getpass("Enter database password: ")
    
    # Connect to database
    conn = create_database_connection(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    if not conn:
        logger.error("❌ Failed to connect to database")
        sys.exit(1)
    
    try:
        # Create table if needed
        if not args.skip_table_creation:
            if not create_sample_table(conn):
                logger.error("❌ Failed to create table")
                sys.exit(1)
        
        # Generate sample data
        data = generate_sample_data(args.records)
        
        # Insert data
        insert_data_batch(conn, data, args.batch_size)
        
        # Create sample queries file
        create_sample_queries_file()
        
        # Final statistics
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM "sample-location-data"')
        total_records = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT device_id) as devices,
                   MIN(timestamp_column) as earliest,
                   MAX(timestamp_column) as latest
            FROM "sample-location-data"
        """)
        stats = cursor.fetchone()
        
        logger.info("🎉 Data generation completed successfully!")
        logger.info(f"📊 Total records: {total_records:,}")
        logger.info(f"🔢 Unique devices: {stats[0]}")
        logger.info(f"📅 Date range: {stats[1]} to {stats[2]}")
        
        cursor.close()
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()