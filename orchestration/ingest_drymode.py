import argparse
import logging
import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# Ensure the logs directory exists
log_dir = os.path.join(os.path.dirname(__file__), "ingestion_log", "logs")
os.makedirs(log_dir, exist_ok=True)

# Set up logging
log_file = os.path.join(log_dir, "ingestion.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Parse CLI args
parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="Run the script without inserting data")
args = parser.parse_args()

# Connect to DB
engine = create_engine("postgresql://postgres:postgres@localhost:5432/mlops")

# Example dataset
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
df = pd.read_parquet(url)

# Show preview and schema
logging.info(f"Read {len(df)} rows")
logging.info(f"Schema preview:\n{df.dtypes}")

if args.dry_run:
    logging.info("Dry run: skipping database insertion.")
else:
    df.to_sql("green_tripdata_jan2023", con=engine, if_exists="replace", index=False)
    logging.info("✅ Data inserted into PostgreSQL.")
