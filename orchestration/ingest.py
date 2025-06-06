import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# DB Connection
# ----------------------------
DB_URL = "postgresql://postgres:postgres@localhost:5432/mlops"
engine = create_engine(DB_URL)

# ----------------------------
# Dry run flag
# ----------------------------
DRY_RUN = False  # Set to False to actually insert into the DB

# ----------------------------
# Create schema if not exists
# ----------------------------
def create_schema(schema_name):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        logging.info(f"Ensured schema '{schema_name}' exists")

# ----------------------------
# File ingestion
# ----------------------------
def ingest_file_to_db(url, table_name, schema="raw"):
    try:
        logging.info(f"Downloading {url}")
        df = pd.read_parquet(url)

        logging.info(f"{table_name} — Shape: {df.shape}")
        logging.info(f"{table_name} — Columns: {df.dtypes.to_dict()}")
        logging.info(f"{table_name} — Sample: {df.head(1).to_dict(orient='records')}")

        if not DRY_RUN:
            logging.info(f"Writing to table '{schema}.{table_name}'")
            df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)
            logging.info(f"Successfully ingested {table_name}")
        else:
            logging.info(f"DRY RUN — Skipping DB insert for {table_name}")

    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {str(e)}")

# ----------------------------
# Main execution
# ----------------------------
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)

    schema = "raw"
    create_schema(schema)

    parquet_files = {
        "green_tripdata_jan": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "green_tripdata_feb": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "green_tripdata_mar": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
    }

    for table_name, url in parquet_files.items():
        ingest_file_to_db(url, table_name, schema=schema)

    logging.info(" Ingestion complete")




# import os
# import logging
# import pandas as pd
# from sqlalchemy import create_engine, text

# # ----------------------------
# # Logging setup
# # ----------------------------
# logging.basicConfig(
#     filename="logs/ingestion.log",
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # ----------------------------
# # DB Connection
# # ----------------------------
# DB_URL = "postgresql://postgres:postgres@localhost:5432/mlops"
# engine = create_engine(DB_URL)

# # ----------------------------
# # Create schema if not exists
# # ----------------------------
# def create_schema(schema_name):
#     with engine.connect() as conn:
#         conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
#         logging.info(f"Ensured schema '{schema_name}' exists")

# # ----------------------------
# # File ingestion
# # ----------------------------
# def ingest_file_to_db(url, table_name, schema="raw"):
#     try:
#         logging.info(f"⬇ Downloading {url}")
#         df = pd.read_parquet(url)

#         logging.info(f" Writing to table '{schema}.{table_name}'")
#         df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)

#         logging.info(f" Successfully ingested {table_name}")
#     except Exception as e:
#         logging.error(f" Error ingesting {table_name}: {str(e)}")

# # ----------------------------
# # Main execution
# # ----------------------------
# if __name__ == "__main__":
#     os.makedirs("logs", exist_ok=True)

#     schema = "raw"
#     create_schema(schema)

#     parquet_files = {
#         "green_tripdata_jan": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet",
#         "green_tripdata_feb": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-02.parquet",
#         "green_tripdata_mar": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-03.parquet",
#     }

#     for table_name, url in parquet_files.items():
#         ingest_file_to_db(url, table_name, schema=schema)
