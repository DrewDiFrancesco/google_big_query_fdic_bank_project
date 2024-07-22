# Data Engineering Project: FDIC Banks Data

## Project Overview

This project involves extracting data from Google BigQuery, transforming it, and loading it into a PostgreSQL database. The project focuses on creating three types of data tables:
1. Kimball-style dimension and fact tables.
2. A single comprehensive table.
3. Relational modeling tables.

The primary tools and technologies used in this project include PySpark for data processing, PostgreSQL for data storage, and Python for scripting and orchestration.

## Project Structure

- `etl_helpers.py`: Helper functions for ETL processes, including saving and reading data to/from PostgreSQL.
- `schemas.py`: Defines the schemas for the FDIC Banks data in both PySpark and PostgreSQL formats.
- `config.py`: Configuration management for the project, including paths and connection details.
- `spark_session.py`: Manages the Spark session.

## Setup and Requirements

### Prerequisites

- Python 3.8+
- Apache Spark
- PostgreSQL
- Google Cloud BigQuery


