#!/bin/bash
set -e

echo "Running ETL pipeline..."
python3 etl_pipeline.py
echo "Pipeline finished successfully."
