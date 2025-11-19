# Troubleshooting Guide for Sample Data Generation

## The Error You're Experiencing

Based on the exit code 1 and no output, the most likely issues are:

### 1. **Database Connection Issues**

First, verify PostgreSQL is running and accessible:

```bash
# Check if PostgreSQL service is running
brew services list | grep postgresql
# or
sudo systemctl status postgresql

# Test connection manually
psql -h localhost -U postgres -d local-intangles-core -c "SELECT version();"
```

If this fails, you need to:
- Start PostgreSQL service: `brew services start postgresql`
- Check if the database exists: `psql -h localhost -U postgres -l`
- Create the database if missing: `psql -h localhost -U postgres -c "CREATE DATABASE \"local-intangles-core\";"`

### 2. **Missing Python Packages**

Install required packages:

```bash
# Using conda (recommended for this project)
conda install psycopg2 pandas numpy -y
pip install faker tqdm

# Or using pip only
pip install psycopg2-binary pandas numpy faker tqdm
```

### 3. **Authentication Issues**

The password might be incorrect or user doesn't exist:

```bash
# Test authentication
psql -h localhost -U postgres -c "SELECT current_user;"

# If this fails, you may need to:
# 1. Check pg_hba.conf for authentication method
# 2. Reset postgres password
# 3. Create the user if it doesn't exist
```

## Quick Fix Solutions

### Option 1: Use the Simple Data Generator

I created a minimal version that's easier to debug:

```bash
python simple_data_gen.py
```

This creates just 100 records without complex dependencies.

### Option 2: Manual Database Setup

Create the table manually first:

```sql
-- Connect to your database
psql -h localhost -U postgres -d local-intangles-core

-- Create the table
CREATE TABLE IF NOT EXISTS "sample-location-data" (
    id SERIAL PRIMARY KEY,
    timestamp_column TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    speed DECIMAL(6, 2),
    temperature DECIMAL(5, 2),
    city VARCHAR(50)
);

-- Insert a few test records
INSERT INTO "sample-location-data" 
(timestamp_column, device_id, latitude, longitude, speed, temperature, city)
VALUES 
(NOW() - INTERVAL '1 hour', 'TEST_001', 19.0760, 72.8777, 25.5, 28.5, 'Mumbai'),
(NOW() - INTERVAL '2 hours', 'TEST_002', 19.0761, 72.8778, 30.2, 29.1, 'Mumbai'),
(NOW() - INTERVAL '3 hours', 'TEST_003', 19.0762, 72.8779, 15.8, 27.9, 'Mumbai');
```

### Option 3: Check Your Environment

1. **Verify Python version**:
   ```bash
   python --version  # Should be 3.x
   ```

2. **Test package imports**:
   ```bash
   python -c "import psycopg2, pandas, numpy; print('All packages available')"
   ```

3. **Check database connectivity**:
   ```bash
   python -c "
   import psycopg2
   try:
       conn = psycopg2.connect(host='localhost', user='postgres', database='local-intangles-core', password='postgres123!@#')
       print('Database connection successful')
       conn.close()
   except Exception as e:
       print(f'Connection failed: {e}')
   "
   ```

## Most Likely Solutions

Based on common issues, try these in order:

1. **Start PostgreSQL service**: `brew services start postgresql`
2. **Create database**: `createdb -U postgres local-intangles-core`
3. **Install packages**: `pip install psycopg2-binary pandas numpy faker tqdm`
4. **Use simple generator**: `python simple_data_gen.py`

## Next Steps

Once you identify the specific error:

1. **If database connection works**: Use the full sample generator
2. **If only simple data works**: That's fine for testing the Airflow DAG
3. **If nothing works**: We can modify the Airflow DAG to use a different data source

The main goal is to get some time-series data in your database for testing the ETL pipeline! 🎯