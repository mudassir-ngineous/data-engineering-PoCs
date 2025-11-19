# Airflow TimescaleDB to S3 Pipeline

This project sets up Apache Airflow locally using Docker to run a daily ETL pipeline that:
1. Extracts compressed chunk data from TimescaleDB at 12:00 PM IST
2. Converts the data to CSV format
3. Converts CSV to Parquet format with compression
4. Uploads the Parquet file to S3 with date partitioning

## Prerequisites

- Docker Desktop installed and running
- PostgreSQL/TimescaleDB instance running locally
- AWS CLI configured with profile 'qa-in' (or update AWS_PROFILE in .env)
- AWS account with S3 bucket access
- conda environment (optional but recommended)

## Setup Instructions

### 1. Configure Environment Variables

Copy the example environment file and update with your actual values:

```bash
cp .env.example .env
```

Edit `.env` file with your actual credentials:

```bash
# AWS Configuration (uses local AWS profile)
AWS_PROFILE=qa-in
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=your-s3-bucket-name

# TimescaleDB Connection
TIMESCALE_HOST=host.docker.internal  # For local PostgreSQL
TIMESCALE_DB=your_database_name
TIMESCALE_USER=your_postgres_user
TIMESCALE_PASSWORD=your_postgres_password

# Airflow Database Connection
AIRFLOW_DB_HOST=host.docker.internal
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_USER=your_postgres_user
AIRFLOW_DB_PASSWORD=your_postgres_password
```

### 2. Setup Database

Create the Airflow database in your PostgreSQL instance:

```bash
psql -U your_postgres_user -h localhost -c "CREATE DATABASE airflow;"
```

Or run the provided SQL script:

```bash
psql -U your_postgres_user -h localhost -f db_setup.sql
```

### 3. Run Setup Script

Make the setup script executable and run it:

```bash
chmod +x setup.sh
./setup.sh
```

This script will:
- Check prerequisites
- Initialize Airflow database
- Start Airflow webserver and scheduler
- Create admin user (username: admin, password: admin)

### 4. Access Airflow UI

Open your browser and go to: http://localhost:8080

Login credentials:
- Username: `admin`
- Password: `admin`

### 5. Generate Sample Data

Before testing the pipeline, create sample data for testing:

```bash
# Go to sample data generator folder
cd sample-data-generator

# Quick setup with SQL (recommended)
psql -h localhost -U postgres -d local-intangles-core -f manual_sample_data.sql

# Or use automated Python script
./complete_setup.sh

# Return to main directory
cd ..
```

This creates a `sample-location-data` table with 15,000 test records.

### 6. Test the Pipeline

1. Go to Airflow UI at http://localhost:8080
2. Find the DAG named `timescale_to_s3_pipeline`  
3. Turn on the DAG toggle
4. Click "Trigger DAG" to test manually
5. Monitor the task execution in the Graph or Tree view

The DAG is already configured to work with the sample data structure.

## Sample Data Generator

The `sample-data-generator/` folder contains comprehensive tools for generating test data:

- **📊 15,000 sample records** across 3 Indian cities
- **🏙️ Realistic geospatial data** (Mumbai, Delhi, Bangalore)
- **📱 IoT device simulation** with sensor readings
- **⏰ 72-hour time range** for comprehensive testing

See `sample-data-generator/README.md` for detailed usage instructions.

## DAG Details

### Schedule
- **Trigger Time**: 12:00 PM IST (6:30 AM UTC)
- **Frequency**: Daily  
- **Timezone**: Asia/Kolkata

### Tasks
1. **extract_timescale_data**: Connects to TimescaleDB and extracts compressed chunk data
2. **convert_to_parquet**: Converts CSV data to Parquet format with Snappy compression
3. **upload_to_s3**: Uploads Parquet file to S3 with date partitioning
4. **notify_completion**: Logs completion details

### Output Structure
Files are uploaded to S3 with the following structure:
```
s3://your-bucket/timescale_data/year=2025/month=11/day=18/timescale_data_2025-11-18.parquet
```

## Management Commands

### Start Services
```bash
docker-compose up -d
```

### Stop Services
```bash
docker-compose down
```

### View Logs
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Restart Services
```bash
docker-compose restart
```

### Clean Reset (removes all data)
```bash
docker-compose down -v
docker system prune -f
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check if PostgreSQL is running: `pg_ctl status`
   - Verify connection details in `.env` file
   - Ensure `host.docker.internal` resolves correctly

2. **DAG Import Errors**
   - Check Airflow logs: `docker-compose logs airflow-scheduler`
   - Verify all required packages are installed
   - Check syntax in DAG file

3. **S3 Upload Failed**
   - Verify AWS credentials are correct
   - Check S3 bucket permissions
   - Ensure bucket exists and is accessible

4. **TimescaleDB Query Failed**
   - Verify table and column names in the query
   - Check TimescaleDB connection and permissions
   - Test query manually in psql

### Performance Tuning

1. **For Large Datasets**:
   - Adjust `row_group_size` in Parquet conversion
   - Use chunk processing for very large datasets
   - Consider parallel processing

2. **Memory Optimization**:
   - Increase Docker memory allocation
   - Use streaming for large file processing
   - Monitor container resource usage

## Security Notes

- Store credentials in environment variables, never in code
- Use IAM roles for S3 access in production
- Enable SSL for database connections in production
- Regularly rotate AWS access keys
- Use Airflow Connections for sensitive data in production

## Production Deployment

For production deployment, consider:
- Using external PostgreSQL/RDS for Airflow metadata
- Setting up Redis for Celery executor
- Using Kubernetes for orchestration
- Implementing proper monitoring and alerting
- Setting up backup and disaster recovery