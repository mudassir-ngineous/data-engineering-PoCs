#!/usr/bin/env python3
"""
Simple sample data generator - minimal version for debugging
"""

import psycopg2
import random
from datetime import datetime, timedelta

def main():
    print("Starting simple sample data generation...")
    
    # Database connection
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='local-intangles-core',
            user='postgres',
            password='postgres123!@#'
        )
        print("Connected to database successfully!")
        
        cursor = conn.cursor()
        
        # Create simple table
        cursor.execute("""
            DROP TABLE IF EXISTS "sample-location-data";
            CREATE TABLE "sample-location-data" (
                id SERIAL PRIMARY KEY,
                timestamp_column TIMESTAMPTZ NOT NULL,
                device_id VARCHAR(50) NOT NULL,
                latitude DECIMAL(10, 8) NOT NULL,
                longitude DECIMAL(11, 8) NOT NULL,
                speed DECIMAL(6, 2),
                temperature DECIMAL(5, 2),
                city VARCHAR(50)
            );
        """)
        print("Created table successfully!")
        
        # Insert sample data
        print("Inserting sample data...")
        for i in range(100):  # Just 100 records for testing
            timestamp = datetime.now() - timedelta(hours=random.randint(0, 24))
            device_id = f"DEVICE_{i%10:03d}"
            latitude = 19.0760 + (random.uniform(-0.1, 0.1))  # Mumbai area
            longitude = 72.8777 + (random.uniform(-0.1, 0.1))
            speed = random.uniform(0, 60)
            temperature = random.uniform(20, 35)
            city = "Mumbai"
            
            cursor.execute("""
                INSERT INTO "sample-location-data" 
                (timestamp_column, device_id, latitude, longitude, speed, temperature, city)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (timestamp, device_id, latitude, longitude, speed, temperature, city))
        
        conn.commit()
        print("Inserted 100 sample records!")
        
        # Verify data
        cursor.execute('SELECT COUNT(*) FROM "sample-location-data"')
        count = cursor.fetchone()[0]
        print(f"Total records in table: {count}")
        
        cursor.close()
        conn.close()
        print("Sample data generation completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)