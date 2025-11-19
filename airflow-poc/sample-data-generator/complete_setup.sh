#!/bin/bash

# Comprehensive Sample Data Setup Script for Airflow POC
# This script handles conda environment setup and data generation

set -e

echo "🚀 Starting comprehensive sample data setup..."

# Function to check command success
check_success() {
    if [ $? -eq 0 ]; then
        echo "✅ $1"
    else
        echo "❌ $1 failed"
        exit 1
    fi
}

# Step 1: Activate conda environment
echo "🔧 Setting up conda environment..."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
check_success "Conda environment activated"

# Step 2: Install required packages in conda environment
echo "📦 Installing required packages..."
pip install psycopg2-binary pandas numpy faker tqdm > /dev/null 2>&1
check_success "Python packages installed"

# Step 3: Test database connection
echo "🔍 Testing database connection..."
python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='local-intangles-core',
        user='postgres',
        password='postgres123!@#'
    )
    print('✅ Database connection successful')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    exit(1)
" || exit 1

# Step 4: Create and populate sample data
echo "📝 Creating sample data..."
python << 'EOF'
import psycopg2
from datetime import datetime, timedelta
import random

def create_sample_data():
    print("Creating sample data table...")
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='local-intangles-core',
        user='postgres',
        password='postgres123!@#'
    )
    
    cursor = conn.cursor()
    
    # Create table
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
    
    print("Table created successfully!")
    
    # Insert sample data
    cities = [
        {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
        {"name": "Delhi", "lat": 28.6139, "lon": 77.2090},
        {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946}
    ]
    
    total_records = 0
    for city in cities:
        for device_num in range(5):  # 5 devices per city
            device_id = f"{city['name'][:3].upper()}_{device_num+1:03d}"
            
            for _ in range(1000):  # 1000 records per device
                timestamp = datetime.now() - timedelta(hours=random.uniform(0, 72))
                lat = city['lat'] + random.uniform(-0.05, 0.05)
                lon = city['lon'] + random.uniform(-0.05, 0.05)
                speed = round(random.uniform(0, 60), 2)
                temp = round(random.uniform(20, 35), 2)
                humidity = round(random.uniform(40, 80), 2)
                battery = random.randint(20, 100)
                
                cursor.execute('''
                    INSERT INTO "sample-location-data" 
                    (timestamp_column, device_id, latitude, longitude, speed, temperature, humidity, battery_level, city, country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (timestamp, device_id, lat, lon, speed, temp, humidity, battery, city['name'], 'India'))
                
                total_records += 1
    
    conn.commit()
    print(f"Inserted {total_records:,} sample records!")
    
    # Verify data
    cursor.execute('SELECT COUNT(*) FROM "sample-location-data"')
    count = cursor.fetchone()[0]
    print(f"Total records verified: {count:,}")
    
    # Test Airflow query
    cursor.execute('''
        SELECT COUNT(*) FROM (
            SELECT 
                DATE_TRUNC('hour', timestamp_column) as hour_bucket,
                device_id,
                AVG(latitude) as avg_latitude,
                AVG(longitude) as avg_longitude,
                AVG(speed) as avg_speed,
                COUNT(*) as record_count
            FROM "sample-location-data" 
            WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
            GROUP BY DATE_TRUNC('hour', timestamp_column), device_id
        ) subquery
    ''')
    
    query_results = cursor.fetchone()[0]
    print(f"Airflow query test: {query_results} aggregated rows available")
    
    cursor.close()
    conn.close()
    
    return True

if __name__ == "__main__":
    create_sample_data()
    print("✅ Sample data creation completed!")
EOF

check_success "Sample data created"

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "📊 What was created:"
echo "   • Table: sample-location-data"
echo "   • Records: ~15,000 sample records"
echo "   • Cities: Mumbai, Delhi, Bangalore"
echo "   • Devices: 15 total (5 per city)"
echo "   • Time range: Last 72 hours"
echo ""
echo "🔄 Next steps:"
echo "   1. Update your .env file with correct database credentials"
echo "   2. Run: ./setup.sh (to start Airflow)"
echo "   3. Test your DAG at: http://localhost:8080"
echo ""