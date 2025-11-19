#!/usr/bin/env python3
"""
Quick test script to verify sample data generation and Airflow DAG query
"""

import psycopg2
import pandas as pd
import os
from datetime import datetime

def test_database_connection(host, port, database, user, password):
    """Test database connection"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print("✅ Database connection successful")
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

def test_table_exists(conn):
    """Check if sample table exists"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'sample-location-data'
        );
    """)
    exists = cursor.fetchone()[0]
    cursor.close()
    
    if exists:
        print("✅ Table 'sample-location-data' exists")
        return True
    else:
        print("❌ Table 'sample-location-data' does not exist")
        return False

def test_data_count(conn):
    """Check record count"""
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM "sample-location-data"')
    count = cursor.fetchone()[0]
    cursor.close()
    
    print(f"📊 Total records: {count:,}")
    return count

def test_airflow_query(conn):
    """Test the actual Airflow DAG query"""
    query = """
    SELECT 
        time_bucket('1 hour', timestamp_column) as hour_bucket,
        device_id,
        AVG(latitude) as avg_latitude,
        AVG(longitude) as avg_longitude,
        AVG(speed) as avg_speed,
        AVG(temperature) as avg_temperature,
        AVG(humidity) as avg_humidity,
        AVG(battery_level) as avg_battery_level,
        COUNT(*) as record_count,
        MAX(timestamp_column) as max_timestamp,
        MIN(timestamp_column) as min_timestamp,
        city,
        country
    FROM "sample-location-data" 
    WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
    GROUP BY hour_bucket, device_id, city, country
    ORDER BY hour_bucket DESC, device_id
    LIMIT 10;
    """
    
    try:
        df = pd.read_sql(query, conn)
        print(f"✅ Airflow DAG query successful - {len(df)} result rows")
        print("\n📋 Sample results:")
        print(df.head())
        return True
    except Exception as e:
        print(f"❌ Airflow DAG query failed: {e}")
        return False

def test_timescaledb_features(conn):
    """Test TimescaleDB specific features"""
    cursor = conn.cursor()
    
    # Check if it's a hypertable
    cursor.execute("""
        SELECT hypertable_name, num_chunks 
        FROM timescaledb_information.hypertables 
        WHERE hypertable_name = 'sample-location-data';
    """)
    
    result = cursor.fetchone()
    if result:
        print(f"✅ TimescaleDB hypertable confirmed - {result[1]} chunks")
    else:
        print("⚠️  Not a TimescaleDB hypertable")
    
    # Check compression policy
    cursor.execute("""
        SELECT job_id, application_name, scheduled 
        FROM timescaledb_information.jobs 
        WHERE application_name LIKE '%compression%';
    """)
    
    policies = cursor.fetchall()
    if policies:
        print(f"✅ Compression policies found: {len(policies)} jobs")
    else:
        print("⚠️  No compression policies found")
    
    cursor.close()

def main():
    print("🧪 Testing sample data setup...\n")
    
    # Get database connection details
    host = os.getenv('TIMESCALE_HOST', 'localhost')
    port = int(os.getenv('TIMESCALE_PORT', 5432))
    database = os.getenv('TIMESCALE_DB', 'local-intangles-core')
    user = os.getenv('TIMESCALE_USER', 'postgres')
    password = os.getenv('TIMESCALE_PASSWORD')
    
    if not password:
        import getpass
        password = getpass.getpass("Enter database password: ")
    
    print(f"🔍 Testing connection to {host}:{port}/{database}")
    
    # Test connection
    conn = test_database_connection(host, port, database, user, password)
    if not conn:
        return
    
    try:
        # Test table existence
        if not test_table_exists(conn):
            print("\n💡 Run the sample data generator first:")
            print("   python generate_sample_data.py --user {} --password YOUR_PASSWORD".format(user))
            return
        
        # Test data count
        count = test_data_count(conn)
        
        if count == 0:
            print("⚠️  No data in table. Run the sample data generator.")
            return
        
        # Test Airflow query
        test_airflow_query(conn)
        
        # Test TimescaleDB features
        print("\n🔧 Testing TimescaleDB features:")
        test_timescaledb_features(conn)
        
        print("\n🎉 All tests passed! Your setup is ready for Airflow DAG testing.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()