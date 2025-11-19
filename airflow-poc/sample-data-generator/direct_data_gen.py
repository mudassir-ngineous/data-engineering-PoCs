#!/usr/bin/env python3
"""
Direct database connection test and simple data insertion
"""

print("🔍 Testing database connection and creating sample data...")

try:
    import psycopg2
    print("✅ psycopg2 imported successfully")
except ImportError as e:
    print(f"❌ psycopg2 import failed: {e}")
    exit(1)

try:
    from datetime import datetime, timedelta
    import random
    print("✅ Standard libraries imported")
except ImportError as e:
    print(f"❌ Standard library import failed: {e}")
    exit(1)

def main():
    print("🔗 Attempting database connection...")
    
    try:
        # Database connection
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='local-intangles-core',
            user='postgres',
            password='postgres123!@#'
        )
        print("✅ Database connection successful!")
        
        cursor = conn.cursor()
        
        # Drop and recreate table
        print("🗃️ Creating table...")
        cursor.execute('''
            DROP TABLE IF EXISTS "sample-location-data";
            
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
        ''')
        print("✅ Table created successfully!")
        
        # Insert sample data
        print("📝 Inserting sample data...")
        
        # Cities with coordinates
        cities = [
            {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "country": "India"},
            {"name": "Delhi", "lat": 28.6139, "lon": 77.2090, "country": "India"},
            {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946, "country": "India"},
        ]
        
        records_inserted = 0
        target_records = 10000  # 10k records for testing
        
        for city in cities:
            devices_per_city = 10
            records_per_device = target_records // (len(cities) * devices_per_city)
            
            for device_num in range(devices_per_city):
                device_id = f"{city['name'][:3].upper()}_{device_num+1:03d}"
                
                # Generate coordinates around city center
                base_lat = city['lat']
                base_lon = city['lon']
                
                for i in range(records_per_device):
                    # Generate timestamp (last 7 days)
                    days_ago = random.uniform(0, 7)
                    timestamp = datetime.now() - timedelta(days=days_ago)
                    
                    # Generate coordinates within city
                    lat_offset = random.uniform(-0.1, 0.1)  # ~10km radius
                    lon_offset = random.uniform(-0.1, 0.1)
                    latitude = base_lat + lat_offset
                    longitude = base_lon + lon_offset
                    
                    # Generate sensor data
                    speed = round(random.uniform(0, 80), 2)
                    temperature = round(random.uniform(20, 40), 2)
                    humidity = round(random.uniform(40, 80), 2)
                    battery_level = random.randint(20, 100)
                    
                    # Insert record
                    cursor.execute('''
                        INSERT INTO "sample-location-data" 
                        (timestamp_column, device_id, latitude, longitude, speed, temperature, humidity, battery_level, city, country)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (timestamp, device_id, latitude, longitude, speed, temperature, humidity, battery_level, city['name'], city['country']))
                    
                    records_inserted += 1
                    
                    # Progress indicator
                    if records_inserted % 1000 == 0:
                        print(f"📊 Inserted {records_inserted:,} records...")
        
        # Commit all changes
        conn.commit()
        print(f"✅ Successfully inserted {records_inserted:,} records!")
        
        # Verify data
        cursor.execute('SELECT COUNT(*) FROM "sample-location-data"')
        total_count = cursor.fetchone()[0]
        print(f"📋 Total records in table: {total_count:,}")
        
        # Show sample data
        cursor.execute('''
            SELECT device_id, city, COUNT(*) as records, 
                   MIN(timestamp_column) as earliest, 
                   MAX(timestamp_column) as latest
            FROM "sample-location-data" 
            GROUP BY device_id, city 
            ORDER BY city, device_id 
            LIMIT 5
        ''')
        
        print("\n📊 Sample data distribution:")
        for row in cursor.fetchall():
            print(f"   {row[0]} ({row[1]}): {row[2]} records, {row[3]} to {row[4]}")
        
        # Test the Airflow query
        print("\n🧪 Testing Airflow DAG query...")
        cursor.execute('''
            SELECT 
                DATE_TRUNC('hour', timestamp_column) as hour_bucket,
                device_id,
                AVG(latitude) as avg_latitude,
                AVG(longitude) as avg_longitude,
                AVG(speed) as avg_speed,
                AVG(temperature) as avg_temperature,
                AVG(humidity) as avg_humidity,
                AVG(battery_level) as avg_battery_level,
                COUNT(*) as record_count,
                city,
                country
            FROM "sample-location-data" 
            WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', timestamp_column), device_id, city, country
            ORDER BY hour_bucket DESC, device_id
            LIMIT 5
        ''')
        
        results = cursor.fetchall()
        print(f"✅ Airflow query returned {len(results)} rows")
        if results:
            print("   Sample result:", results[0][:5])  # Show first 5 columns
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Sample data generation completed successfully!")
        print("💡 You can now test your Airflow DAG with this data!")
        
        return 0
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)