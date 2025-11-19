# Sample Geospatial Data Generator

This script generates 1 million realistic geospatial time-series records for testing your TimescaleDB to S3 pipeline.

## What the Script Creates

### Table Structure: `sample-location-data`
- **TimescaleDB hypertable** with 1-day chunks
- **Compression** enabled for data older than 7 days
- **Geospatial indexes** for efficient location queries
- **Device tracking** across multiple Indian cities

### Sample Data Features
- ✅ **1M records** across ~750 simulated IoT devices
- 🏙️ **10 major Indian cities** (Mumbai, Delhi, Bangalore, etc.)
- 📍 **Realistic GPS coordinates** with natural movement patterns
- 🕐 **30-day time range** with varying data density
- 🌡️ **Sensor data**: temperature, humidity, pressure, battery level
- 🚗 **Vehicle data**: speed, heading, altitude, accuracy

## Quick Setup & Run

### 1. Install Dependencies
```bash
# Run the setup script
./setup_sample_data.sh
```

### 2. Generate Sample Data
```bash
# Generate 1M records (default)
python generate_sample_data.py --user your_postgres_user --password your_password

# Or generate smaller dataset for testing
python generate_sample_data.py --user your_postgres_user --password your_password --records 100000

# Custom database connection
python generate_sample_data.py \\
  --host localhost \\
  --database local-intangles-core \\
  --user your_postgres_user \\
  --password your_password \\
  --records 1000000
```

### 3. Test the Data
After generation, you'll get a `sample_queries.sql` file with test queries:

```sql
-- Time-bucketed aggregation (matches your Airflow DAG query)
SELECT 
    time_bucket('1 hour', timestamp_column) as hour_bucket,
    device_id,
    AVG(latitude) as avg_latitude,
    AVG(longitude) as avg_longitude,
    AVG(speed) as avg_speed,
    AVG(temperature) as avg_temperature,
    COUNT(*) as record_count
FROM "sample-location-data" 
WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
GROUP BY hour_bucket, device_id
ORDER BY hour_bucket DESC, device_id;
```

## Performance Expectations

### Generation Time
- **100K records**: ~2-3 minutes
- **1M records**: ~15-20 minutes
- **Memory usage**: ~500MB-1GB during generation

### Database Size
- **1M records**: ~300-400MB uncompressed
- **Compressed chunks**: ~60-80% space savings after 7 days
- **Indexes**: Additional ~50-100MB

## Integration with Airflow DAG

The generated data works seamlessly with your Airflow pipeline:

1. **Table name**: `sample-location-data` (already configured in DAG)
2. **Query structure**: Matches the updated DAG query
3. **Data format**: Compatible with CSV → Parquet → S3 pipeline
4. **Time range**: Always has recent data for testing

## Data Distribution

### Cities & Devices
- **Mumbai**: 50-100 devices
- **Delhi**: 50-100 devices  
- **Bangalore**: 50-100 devices
- **Chennai, Hyderabad, Pune, Kolkata, Ahmedabad, Jaipur, Surat**: 50-100 devices each

### Realistic Patterns
- **Movement**: Devices move naturally within city boundaries
- **Frequency**: More recent data has higher density
- **Sensors**: Values vary realistically (temperature 15-45°C, speed 0-80 km/h)
- **Battery**: Simulates IoT device power consumption

## Monitoring & Validation

### Check Record Count
```sql
SELECT COUNT(*) FROM "sample-location-data";
```

### Verify Time Distribution
```sql
SELECT 
    DATE(timestamp_column) as date,
    COUNT(*) as records
FROM "sample-location-data" 
GROUP BY DATE(timestamp_column) 
ORDER BY date DESC;
```

### Check Compression Status
```sql
SELECT chunk_name, range_start, range_end, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sample-location-data'
ORDER BY range_start DESC;
```

## Troubleshooting

### Common Issues

1. **TimescaleDB Extension Missing**
   ```sql
   CREATE EXTENSION IF NOT EXISTS timescaledb;
   ```

2. **PostGIS Extension for Geospatial Queries**
   ```sql
   CREATE EXTENSION IF NOT EXISTS postgis;
   CREATE EXTENSION IF NOT EXISTS earthdistance;
   ```

3. **Permission Issues**
   - Ensure your user has CREATE TABLE privileges
   - Verify TimescaleDB functions are accessible

4. **Memory Issues**
   - Reduce `--records` parameter for testing
   - Increase `--batch-size` if database can handle larger batches

### Performance Tuning

1. **Faster Insertion**
   ```bash
   # Increase batch size
   python generate_sample_data.py --batch-size 50000 --user your_user --password your_password
   ```

2. **Skip Table Creation** (if table exists)
   ```bash
   python generate_sample_data.py --skip-table-creation --user your_user --password your_password
   ```

## Next Steps

After generating the data:

1. **Update your `.env` file** with correct database credentials
2. **Test your Airflow DAG** with the sample data
3. **Verify S3 uploads** work with the generated Parquet files
4. **Monitor compression** after 7 days to see space savings

The sample data provides a realistic testing environment for your complete ETL pipeline! 🚀