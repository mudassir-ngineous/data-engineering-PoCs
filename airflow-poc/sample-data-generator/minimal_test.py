import psycopg2
import sys

print("Starting connection test...")

try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='local-intangles-core',
        user='postgres',
        password='postgres123!@#'
    )
    print("SUCCESS: Connected to database!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

print("Connection test completed!")