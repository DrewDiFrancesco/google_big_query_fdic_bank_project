import pandas as pd
import os
from pyspark.sql import *
from pyspark.sql import functions as spark_functions
import etl_helpers as etl_helpers
import config as config
from conf_variables import default_args
from spark_session import SparkManager
import importlib
import configparser
from functools import reduce
import psycopg2
from psycopg2 import sql
from google.cloud import bigquery
# from schemas import fdic_banks_spark, fdic_banks_psql
import schemas as schemas
from argparse import ArgumentParser

def fetch_data(service_key_path):
    table_name = 'fdic_banks'

    # Set the path to the service account key file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key_path

    # Initialize a BigQuery client using application default credentials
    client = bigquery.Client()

    # Construct a reference to the dataset and table
    dataset_ref = client.dataset(table_name, project='bigquery-public-data')
    table_ref = dataset_ref.table('institutions')

    # API request - fetch the table
    table = client.get_table(table_ref)

    # Preview the first few rows of the table
    rows = client.list_rows(table, max_results=30000)

    # Convert to a pandas DataFrame
    columns = [field.name for field in table.schema]
    df = pd.DataFrame([dict(row) for row in rows], columns=columns)

    return df


def parse_args():
    parser = ArgumentParser(description='Getting data from google big query using a service key path')

    parser.add_argument('--data_path',type=str,help='data path')
    parser.add_argument('--s3_bucket',type=str,help='s3 bucket')
    parser.add_argument('--psql_config_path',type=str,help='psql config path')
    parser.add_argument('--service_key_path',type=str,help='service key path')
    parser.add_argument('--table_name',type=str,help='table name')
    parser.add_argument('--jar_file_path',type=str,help='jar file path')

    args, _ = parser.parse_known_args()

    return vars(args)


# Main function
def main(override_args=None, spark=None, config_manager=None):
    print(f"HERE is the override args: {override_args}")

    if override_args:
        default_args.update(override_args)

    print(f"HERE is the new default args: {default_args}")
    if config_manager is None:
        config_manager = config.Config(default_args)

    running_locally = config_manager.args['running_locally']

    if spark is None:
        print(f"Getting Spark")
        spark = SparkManager(config_manager.args).get_spark()
        print("Done getting spark")

    if config_manager.args['s3_bucket'] != '':
        print(f"Updating data_path because s3_bucket is not an empty string...")
        config_manager.args['data_path'] = config_manager.args['s3_bucket'] + '/data'

    service_key_path = config_manager.args['service_key_path']
    table_name = config_manager.args['table_name']

    pandas_df = fetch_data(service_key_path)

    # Convert pandas DataFrame to Spark DataFrame using the provided schema
    spark_df = spark.createDataFrame(pandas_df, schema=getattr(schemas, f'{table_name}_spark'))

    etl_helpers.save_spark_df_to_psql_table(config_manager.args,spark_df)
    # df = etl_helpers.read_psql_table_to_spark_df(config_manager.args, table_name, spark)

if __name__ == '__main__':
    # root_path = os.path.dirname(os.path.abspath(__file__))
    # drews_conf = {
    #     'data_path': '/Users/drewdifrancesco/Desktop/data',
    #     'root_path': root_path,
    #     's3_bucket': '',
    #     'psql_config_path': '/Users/drewdifrancesco/Desktop/repos/configs/psql_config',
    #     'service_key_path': '/Users/drewdifrancesco/Desktop/repos/configs/google_big_query/service_key.json',
    #     'table_name': 'fdic_banks',
    #     'jar_file_path': '/Users/drewdifrancesco/Desktop/jars/postgresql-42.7.3.jar'
    # }

    # main(override_args=drews_conf)

    args = parse_args()
    main(override_args = args)