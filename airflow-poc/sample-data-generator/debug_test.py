#!/usr/bin/env python3

import sys
import os

# Write to file for debugging
with open('debug_output.txt', 'w') as f:
    f.write("Script started\n")
    f.flush()

try:
    import psycopg2
    with open('debug_output.txt', 'a') as f:
        f.write("psycopg2 imported successfully\n")
        f.flush()
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='local-intangles-core',
        user='postgres',
        password='postgres123!@#'
    )
    
    with open('debug_output.txt', 'a') as f:
        f.write("Database connection successful!\n")
        f.flush()
    
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    
    with open('debug_output.txt', 'a') as f:
        f.write(f"PostgreSQL version: {version}\n")
        f.flush()
    
    cursor.close()
    conn.close()
    
    with open('debug_output.txt', 'a') as f:
        f.write("Connection test completed successfully!\n")
        f.flush()
        
except Exception as e:
    with open('debug_output.txt', 'a') as f:
        f.write(f"Error: {str(e)}\n")
        f.flush()
        
print("Check debug_output.txt for results")