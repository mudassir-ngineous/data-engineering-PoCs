#!/bin/bash

# Airflow Setup Script for macOS with Docker

set -e

echo "🚀 Starting Airflow Setup..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ .env file not found. Please configure your environment variables first."
    echo "📝 Copy .env.example to .env and update the values."
    exit 1
fi

# Load environment variables
source .env

# Check AWS profile exists
if ! aws configure list-profiles | grep -q "^${AWS_PROFILE:-qa-in}$"; then
    echo "❌ AWS profile '${AWS_PROFILE:-qa-in}' not found. Please configure it first:"
    echo "   aws configure --profile ${AWS_PROFILE:-qa-in}"
    exit 1
fi

echo "📋 Checking environment variables..."
required_vars=("AWS_PROFILE" "AWS_S3_BUCKET" "TIMESCALE_HOST" "TIMESCALE_DB" "TIMESCALE_USER" "TIMESCALE_PASSWORD" "AIRFLOW_DB_HOST" "AIRFLOW_DB_NAME" "AIRFLOW_DB_USER" "AIRFLOW_DB_PASSWORD")

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Environment variable $var is not set in .env file"
        exit 1
    fi
done

echo "✅ Environment variables configured"

# Test AWS profile access
echo "🔑 Testing AWS profile access..."
if aws s3 ls --profile ${AWS_PROFILE:-qa-in} > /dev/null 2>&1; then
    echo "✅ AWS profile '${AWS_PROFILE:-qa-in}' is working"
else
    echo "❌ AWS profile '${AWS_PROFILE:-qa-in}' access failed. Please check your credentials."
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p logs/scheduler
mkdir -p plugins
mkdir -p config

# Initialize Airflow database
echo "🗄️  Initializing Airflow database..."
docker-compose up airflow-init

# Start Airflow services
echo "🚀 Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "⏳ Waiting for services to start..."
sleep 30

# Check if services are healthy
echo "🔍 Checking service health..."
if docker-compose ps | grep -q "Up"; then
    echo "✅ Airflow services are running successfully!"
    echo ""
    echo "🎉 Setup Complete!"
    echo "📊 Airflow UI: http://localhost:8080"
    echo "🔐 Username: admin"
    echo "🔐 Password: admin"
    echo ""
    echo "📝 Next steps:"
    echo "1. Update the SQL query in dags/timescale_to_s3_pipeline.py"
    echo "2. Configure your actual table names and columns"
    echo "3. Test the DAG in Airflow UI"
else
    echo "❌ Some services failed to start. Check logs with: docker-compose logs"
    exit 1
fi