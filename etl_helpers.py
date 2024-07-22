#!/usr/bin/env python3
import os
from pyspark.sql.functions import *
import schemas
import psycopg2

def read_data(path_to_files="", file_name="", spark=None):
    """
    This function reads and returns saved spark dataframes

    Args:
        path_to_files (string): The directory location of where the dataframe you want to read is saved
        file_name (string): The name of the saved dataframe
        spark: The variable that represents the spark session

    Returns:
        df: returns a spark dataframe
    """

    data_path = f"{path_to_files}/{file_name}".rstrip('/')
    df = spark.read.parquet(data_path)
    return df

def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def write_data(df, path_to_files="", file_name="", mode="overwrite", partition_columns=None, output_type='parquet',spark_schema=None):
    """
    This function saves a spark dataframe to the given path directory, partitioning the data if given columns to partition on
    
    Args:
        df (spark df): The spark dataframe you want to save
        path_to_files (string): The directory location where you want to save the data
        file_name (string): The name you want the dataframe to be saved as
        mode (string): Can be "overwrite" or "append", depending on whether you wish to save a completely new dataframe or add to an existing one
        partition_columns (list): A list of the columns you wish to partition on
        output_type (string): Either parquet or csv depending on what kind of file you want to save the data as
    """
    file_name = file_name.rstrip('/')
    # data_path = f"{path_to_files}/{file_name}".rstrip('/')
    data_path = os.path.join(path_to_files, file_name)
    print(f"Saving to: {data_path}")

    if output_type == 'parquet':
        schema = getattr(schemas, f'{file_name}_spark')
        if partition_columns:
            df.write.option('schema', schema.json()).partitionBy(*partition_columns).mode(mode).parquet(data_path)
        else:
            df.write.option('schema', schema.json()).mode(mode).parquet(data_path)
            
    elif output_type == 'csv':
        data_path = os.path.join(path_to_files, file_name + '.csv')
        schema = getattr(schemas, f'{file_name}_pandas')
        create_directory_if_not_exists(path_to_files)
        # df.toPandas().astype(schema).to_csv(data_path,index=False)
        df.astype(schema).to_csv(data_path,index=False)
        # df.write.csv(data_path)

def finalize_spark_df_schema(schema, df):
    df_columns = set(df.columns)
    schema_columns = set(field.name for field in schema.fields)
    missing_columns = list(schema_columns - df.columns)
    for column in missing_columns:
        df = df.withColumn(column, lit(None))
    
    columns_from_schema = [
        (col(field.name).cast(field.dataType).alias(field.name))
        for field in schema]
    df = df.select(columns_from_schema)

    return df


def generate_create_table_query(table_name, schema):
    """
    Generate a CREATE TABLE SQL query string from a schema dictionary.

    Parameters:
    table_name (str): The name of the table to create.
    schema (dict): A dictionary where keys are column names and values are their PostgreSQL data types.

    Returns:
    str: A CREATE TABLE SQL query string.
    """
    columns = []
    for column_name, data_type in schema.items():
        columns.append(f"{column_name} {data_type}")

    columns_string = ",\n    ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_string}\n);"
    return create_table_query

def save_spark_df_to_psql_table(args,df,mode='overwrite',psql_schema=None):

    table_name = args['table_name']
    db_format = 'jdbc'
    db_host = args['psql_config']['db_host']
    db_port = args['psql_config']['db_port']
    db_name = args['psql_config']['db_name']
    db_password = args['psql_config']['db_password']
    db_url = f'jdbc:postgresql://{db_host}:{db_port}/{db_name}'
    db_user = args['psql_config']['db_user']
    db_driver = 'org.postgresql.Driver'

    create_table_if_not_exists(table_name, args['psql_config'],psql_schema=psql_schema)


    df.write.format(db_format).options(url=db_url,driver=db_driver,dbtable = table_name,user=db_user
                                       ,password=db_password).mode(mode).save()

def read_psql_table_to_spark_df(args, table_name, spark_session):
    """
    Read data from a PostgreSQL table into a Spark DataFrame.

    Parameters:
    args (dict): Dictionary containing PostgreSQL connection configurations.
    table_name (str): The name of the PostgreSQL table to read from.

    Returns:
    Spark DataFrame: Data read from the PostgreSQL table.
    """
    db_format = 'jdbc'
    db_host = args['psql_config']['db_host']
    db_port = args['psql_config']['db_port']
    db_name = args['psql_config']['db_name']
    db_url = f'jdbc:postgresql://{db_host}:{db_port}/{db_name}'
    db_user = args['psql_config']['db_user']
    db_password = args['psql_config']['db_password']
    db_driver = 'org.postgresql.Driver'

    # Read from PostgreSQL table
    df = spark_session.read.format(db_format) \
        .option('url', db_url) \
        .option('driver', db_driver) \
        .option('dbtable', table_name) \
        .option('user', db_user) \
        .option('password', db_password) \
        .load()

    return df

def connect_to_postgres(config):
    """
    Connect to a PostgreSQL database and return the connection and cursor.

    Parameters:
    config (dict): Configuration dictionary containing database connection parameters.

    Returns:
    conn, cursor: The connection and cursor to the database.
    """
    try:
        # Establish the connection
        conn = psycopg2.connect(
            host=config['db_host'],
            database=config['db_name'],
            user=config['db_user'],
            password=config['db_password'],
            port=config.get('db_port', 5432)
        )

        # Create a cursor object
        cursor = conn.cursor()

        print("Connection to PostgreSQL established successfully.")

        return conn, cursor
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None, None


def close_postgres_connection(conn, cursor):
    """
    Close the cursor and connection to the PostgreSQL database.

    Parameters:
    conn: The database connection.
    cursor: The database cursor.
    """
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
    print("PostgreSQL connection closed.")

def create_table_if_not_exists(table_name, args, psql_schema = None):
    """
    Create a table in PostgreSQL if it doesn't exist.

    Parameters:
    table_name (str): Name of the table to create.
    args (dict): Configuration dictionary containing database connection parameters.
    """
    conn = None
    cursor = None
    try:
        # Establish the connection
        conn = psycopg2.connect(
            host=args['db_host'],
            database=args['db_name'],
            user=args['db_user'],
            password=args['db_password'],
            port=args.get('db_port', 5432)
        )

        # Create a cursor object
        cursor = conn.cursor()
        if psql_schema: schema = psql_schema
        else: schema = getattr(schemas,f'{table_name}_psql')
        sql = generate_create_table_query(table_name,schema)

        # Execute the SQL statement
        cursor.execute(sql)

        # Commit the changes
        conn.commit()

        print(f"Table '{table_name}' created successfully (if it didn't already exist).")

    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

