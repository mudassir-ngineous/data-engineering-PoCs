from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import os
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Dict, Any

# Set up logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 18),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def extract_timescale_data(**context) -> str:
    """
    Extract compressed chunk data from TimescaleDB and save to CSV
    Returns: Path to the CSV file
    """
    logger.info("Starting TimescaleDB data extraction")
    
    # Get connection details from environment variables
    timescale_config = {
        'host': os.getenv('TIMESCALE_HOST', 'localhost'),
        'port': int(os.getenv('TIMESCALE_PORT', 5432)),
        'database': os.getenv('TIMESCALE_DB'),
        'user': os.getenv('TIMESCALE_USER'),
        'password': os.getenv('TIMESCALE_PASSWORD')
    }
    
    # Create connection string
    conn_string = f"postgresql://{timescale_config['user']}:{timescale_config['password']}@{timescale_config['host']}:{timescale_config['port']}/{timescale_config['database']}"
    
    # Query for sample-location-data table with actual columns
    query = """
    SELECT 
        time_bucket('1 hour', timestamp_column) as hour_bucket,
        device_id,
        AVG(latitude) as avg_latitude,
        AVG(longitude) as avg_longitude,
        AVG(speed) as avg_speed,
        AVG(temperature) as avg_temperature,
        AVG(humidity) as avg_humidity,
        AVG(battery_level) as avg_battery_level,
        COUNT(*) as record_count,
        MAX(timestamp_column) as max_timestamp,
        MIN(timestamp_column) as min_timestamp,
        city,
        country
    FROM "sample-location-data" 
    WHERE timestamp_column >= NOW() - INTERVAL '24 hours'
    GROUP BY hour_bucket, device_id, city, country
    ORDER BY hour_bucket DESC, device_id;
    """

    try:
        # Read data using pandas
        df = pd.read_sql(query, conn_string)
        logger.info(f"Extracted {len(df)} records from TimescaleDB")

        # Create temporary CSV file
        csv_file = f"/tmp/timescale_data_{context['ds']}.csv"
        df.to_csv(csv_file, index=False)

        logger.info(f"Data saved to CSV: {csv_file}")

        # Push the file path to XCom for the next task
        return csv_file

    except Exception as e:
        logger.error(f"Error extracting data from TimescaleDB: {str(e)}")
        raise

def convert_csv_to_parquet(**context) -> str:
    """
    Convert CSV file to Parquet format
    Returns: Path to the Parquet file
    """
    logger.info("Starting CSV to Parquet conversion")
    
    # Get CSV file path from previous task
    csv_file = context['task_instance'].xcom_pull(task_ids='extract_timescale_data')
    
    if not csv_file or not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        logger.info(f"Read {len(df)} records from CSV file")
        
        # Convert to Arrow Table for better performance
        table = pa.Table.from_pandas(df)
        
        # Create parquet file path
        parquet_file = csv_file.replace('.csv', '.parquet')
        
        # Write to Parquet format with compression
        pq.write_table(
            table, 
            parquet_file,
            compression='snappy',
            use_dictionary=True,
            row_group_size=10000
        )
        
        logger.info(f"Parquet file created: {parquet_file}")
        
        # Clean up CSV file
        os.remove(csv_file)
        logger.info(f"Cleaned up CSV file: {csv_file}")
        
        return parquet_file
        
    except Exception as e:
        logger.error(f"Error converting CSV to Parquet: {str(e)}")
        raise

def upload_to_s3(**context) -> Dict[str, Any]:
    """
    Upload Parquet file to S3 bucket
    Returns: Dictionary with S3 upload details
    """
    logger.info("Starting S3 upload")
    
    # Get Parquet file path from previous task
    parquet_file = context['task_instance'].xcom_pull(task_ids='convert_to_parquet')
    
    if not parquet_file or not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
    
    # Get S3 configuration from environment variables
    bucket_name = os.getenv('AWS_S3_BUCKET')
    aws_profile = os.getenv('AWS_PROFILE', 'qa-in')
    
    if not bucket_name:
        raise ValueError("AWS_S3_BUCKET environment variable not set")
    
    try:
        # Initialize boto3 session with specific profile
        session = boto3.Session(profile_name=aws_profile)
        s3_client = session.client('s3')
        
        # Generate S3 key with date partition
        execution_date = context['ds']
        file_name = os.path.basename(parquet_file)
        s3_key = f"timescale_data/year={execution_date[:4]}/month={execution_date[5:7]}/day={execution_date[8:10]}/{file_name}"
        
        # Upload file to S3
        s3_client.upload_file(parquet_file, bucket_name, s3_key)
        
        # Get file size
        file_size = os.path.getsize(parquet_file)
        
        logger.info(f"File uploaded successfully to S3: s3://{bucket_name}/{s3_key}")
        
        # Clean up local parquet file
        os.remove(parquet_file)
        logger.info(f"Cleaned up Parquet file: {parquet_file}")
        
        upload_details = {
            'bucket': bucket_name,
            'key': s3_key,
            'file_size_bytes': file_size,
            'upload_time': datetime.now().isoformat()
        }
        
        return upload_details
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

def notify_completion(**context):
    """
    Send notification about successful completion
    """
    upload_details = context['task_instance'].xcom_pull(task_ids='upload_to_s3')
    
    logger.info("=== DAG Execution Completed Successfully ===")
    logger.info(f"Execution Date: {context['ds']}")
    logger.info(f"S3 Location: s3://{upload_details['bucket']}/{upload_details['key']}")
    logger.info(f"File Size: {upload_details['file_size_bytes']} bytes")
    logger.info(f"Upload Time: {upload_details['upload_time']}")
    logger.info("=============================================")

# Create the DAG
dag = DAG(
    'timescale_to_s3_pipeline',
    default_args=default_args,
    description='Extract TimescaleDB compressed data, convert to Parquet, and upload to S3',
    schedule_interval='30 18 * * *',  # Runs at 00:00 IST (6:30 PM UTC)
    catchup=False,
    max_active_runs=1,
    tags=['timescale', 'etl', 's3', 'parquet']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_timescale_data',
    python_callable=extract_timescale_data,
    dag=dag
)

convert_task = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_csv_to_parquet,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag
)

notify_task = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    dag=dag
)

# Set task dependencies
extract_task >> convert_task >> upload_task >> notify_task