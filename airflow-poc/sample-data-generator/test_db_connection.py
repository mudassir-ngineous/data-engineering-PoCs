#!/usr/bin/env python3

import psycopg2
import sys

def test_connection():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='local-intangles-core',
            user='postgres',
            password='postgres123!@#'
        )
        print("✅ Database connection successful!")
        
        # Test TimescaleDB extension
        cursor = conn.cursor()
        cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'timescaledb';")
        result = cursor.fetchone()
        
        if result:
            print("✅ TimescaleDB extension found!")
        else:
            print("❌ TimescaleDB extension not found. Installing...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            conn.commit()
            print("✅ TimescaleDB extension installed!")
        
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing database connection...")
    if test_connection():
        print("🎉 Database setup is ready!")
        sys.exit(0)
    else:
        print("💡 Please check your database credentials and ensure PostgreSQL is running.")
        sys.exit(1)