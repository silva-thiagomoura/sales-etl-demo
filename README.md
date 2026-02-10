Sales ETL Demo

This repository contains a demo ETL project for sales data using Python, PySpark, and PostgreSQL, designed to demonstrate a full end-to-end ETL pipeline. It includes generating mock data, cleaning, transforming, encrypting sensitive information, loading to a database, logging, and saving partitioned Parquet files.

The project is designed to run in Jupyter Notebook for demonstration purposes but can also be executed as a standalone Python script.

Features:

- Generate mock sales data in CSV format.
- Extract, transform, and clean data using PySpark.
- Encrypt sensitive fields (e.g., customer IDs).
- Incremental load to PostgreSQL.
- Log ETL runs to a database table.
- Save processed data to partitioned Parquet files.
- Modular code structure for reusable ETL functions.

Project Structure:

- data/ # Raw and intermediate data
- jars/ # Any required JAR files for Spark/Postgres
- logs/ # ETL run logs (if saving locally)
- notebooks/ # Supporting notebooks (optional)
- src/ # Python modules with ETL logic
-- ├─ extract.py # Functions to read raw data
-- ├─ transform.py # Data cleaning and transformations
-- ├─ load.py # Incremental load & Parquet saving
-- ├─ encryption.py # Column encryption functions
-- ├─ config.py # Configuration loader from YAML
-- └─ logger.py # ETL run logging functions
- Sales_ETL_End_to_End_Demo-Copy1.ipynb # Jupyter notebook demo
- config.yaml # Project configuration file
- docker-compose.yml # Optional Docker setup (Postgres container)
- requirements.txt # Python dependencies
- src.zip # Optional zipped source folder for reuse
- main.py # Python script to run ETL outside Jupyter


Running the Project
1. Jupyter Notebook (Recommended for Demo)

** Open Sales_ETL_End_to_End_Demo-Copy1.ipynb and run the notebook cells sequentially. **

The notebook will:

- Generate mock sales data (if missing).
- Read CSV files and transform the data.
- Encrypt sensitive columns.
- Load data incrementally to PostgreSQL.
- Log ETL runs in the database.
- Save final data in partitioned Parquet format.
- Display the final tables and partitions for verification.
- Using the notebook is all you need to demonstrate the ETL workflow.

2. Python Script (VS Code or Terminal)
Run the ETL pipeline as a script:

**** python main.py ****

Configuration:

All paths, database connections, and ETL parameters are defined in config.yaml.

Example entries:
paths:
  raw_data_dir: data/raw
  parquet_output_dir: data/output/sales_parquet
db:
  url: jdbc:postgresql://localhost:5432/sales_db
  user: sales_user
  password_env: DB_PASSWORD
  driver: org.postgresql.Driver

- Adjust paths, database URL, and credentials as needed.
- Passwords can be stored as environment variables (DB_PASSWORD) for security.

Data Flow Overview:

- Extract: Read CSV sales data from data/raw.
- Transform: Clean and normalize data.
- Encrypt: Apply encryption to sensitive columns.
- Load: Incremental load to PostgreSQL + update watermark.
- Log: Save ETL run details to a log table.
- Save Parquet: Store partitioned Parquet files by sale_date.

Notes:

- The project is modular: each step is implemented in src/ as reusable functions.
- The notebook demo is for presentation.
- The Python script (main.py) can be used for automation or production runs.

Docker is optional but included for easy Postgres setup (docker-compose.yml).

Docker:

To run PostgreSQL in Docker:

docker-compose up -d

he service will create a Postgres container with the database and user defined in docker-compose.yml.

Future Improvements:

- Implement retry and error handling for database writes.
- Add dynamic column encryption based on config.
- Enable Parquet compression and multi-column partitioning.
- Add ETL monitoring dashboard using Parquet and DB logs.

Author:
Thiago Moura – Data Engineer
