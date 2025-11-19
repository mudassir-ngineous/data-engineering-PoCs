# Sample Data Generator

This folder contains all tools and scripts for generating sample geospatial time-series data for testing the Airflow TimescaleDB to S3 pipeline.

## 📁 Contents

### Main Scripts
- **`manual_sample_data.sql`** - SQL script to generate 15K records directly in PostgreSQL (recommended)
- **`complete_setup.sh`** - Comprehensive automated setup with conda environment
- **`generate_sample_data.py`** - Full-featured Python generator (1M records capability)
- **`simple_data_gen.py`** - Minimal Python version for basic testing

### Test & Debug Scripts
- **`test_db_connection.py`** - Database connection tester
- **`test_sample_data.py`** - Verify generated data and test queries
- **`direct_data_gen.py`** - Direct database insertion script
- **`debug_test.py`** - Debug database connectivity issues
- **`minimal_test.py`** - Minimal connection test

### Setup Scripts
- **`setup_sample_data.sh`** - Install dependencies for Python generators

### Documentation
- **`SAMPLE_DATA_README.md`** - Comprehensive usage guide
- **`TROUBLESHOOTING.md`** - Common issues and solutions

## 🚀 Quick Start

### Recommended Approach (SQL)
```bash
cd sample-data-generator
psql -h localhost -U postgres -d local-intangles-core -f manual_sample_data.sql
```

### Python Approach
```bash
cd sample-data-generator
./complete_setup.sh
```

### Manual Testing
```bash
cd sample-data-generator
python test_db_connection.py
```

## 📊 Generated Data Structure

All scripts create a table `sample-location-data` with:
- 15,000 sample records
- 3 cities (Mumbai, Delhi, Bangalore)
- 15 devices (5 per city)
- 72-hour time range
- Realistic geospatial and sensor data

## 🔗 Integration

The generated data works seamlessly with the main Airflow DAG in the parent directory. The table structure and query format match exactly what the pipeline expects.

## 💡 Usage

1. **Generate sample data** using any method above
2. **Go back to parent directory**: `cd ..`
3. **Update `.env`** with your database credentials  
4. **Start Airflow**: `./setup.sh`
5. **Test pipeline** at http://localhost:8080