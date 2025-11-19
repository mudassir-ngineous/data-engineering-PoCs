#!/bin/bash

# Sample Data Generation Setup Script
set -e

echo "🔧 Setting up sample data generation environment..."

# Check if conda is available
if command -v conda &> /dev/null; then
    echo "📦 Installing required packages with conda..."
    conda install -y numpy pandas psycopg2 faker tqdm
else
    echo "📦 Installing required packages with pip..."
    pip install numpy pandas psycopg2-binary faker tqdm
fi

# Make the sample data script executable
chmod +x generate_sample_data.py

echo "✅ Setup completed!"
echo ""
echo "🚀 Quick start options:"
echo ""
echo "1. Generate 1M records (default):"
echo "   python generate_sample_data.py --user YOUR_DB_USER --password YOUR_DB_PASSWORD"
echo ""
echo "2. Generate 100K records for testing:"
echo "   python generate_sample_data.py --user YOUR_DB_USER --password YOUR_DB_PASSWORD --records 100000"
echo ""
echo "3. Use different database:"
echo "   python generate_sample_data.py --host localhost --database your-db --user YOUR_DB_USER --password YOUR_DB_PASSWORD"
echo ""
echo "📝 Note: The script will create a table called 'sample-location-data' with TimescaleDB features"