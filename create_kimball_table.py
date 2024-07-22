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

def run_spark_query_and_save_to_psql(spark,sql,schema,args,table_name):
    df = spark.sql(sql)
    columns = df.columns
    new_schema = {}
    args['table_name'] = table_name
    for name, data_type in schema.items():
        if name in columns:
            new_schema[name] = data_type
    etl_helpers.save_spark_df_to_psql_table(args, df,psql_schema=new_schema)


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

    table_name = config_manager.args['table_name']

    # Convert pandas DataFrame to Spark DataFrame using the provided schema
    spark_df = etl_helpers.read_psql_table_to_spark_df(config_manager.args, table_name, spark)
    spark_df.createOrReplaceTempView('fdic_banks_spark')

    base_table_psql_schema = getattr(schemas,f'{table_name}_psql')

    sql = """
        SELECT DISTINCT
            fdic_certificate_number,
            institution_name,
            address,
            city,
            state_name,
            zip_code,
            bank_charter_class,
            occ_charter,
            chartering_agency,
            federal_charter,
            fdic_field_office
        FROM fdic_banks_spark;
    """

    run_spark_query_and_save_to_psql(spark, sql,
        base_table_psql_schema, config_manager.args,'bank_dimension_table')

    sql = """
        SELECT DISTINCT
            state_name,
            state_fips_code
        FROM fdic_banks_spark;
    """

    run_spark_query_and_save_to_psql(spark, sql,
                                     base_table_psql_schema, config_manager.args, 'state_dimension_table')

    sql = """ 
        SELECT DISTINCT city, state_name
        FROM fdic_banks_spark;
    """

    run_spark_query_and_save_to_psql(spark, sql,
                                     base_table_psql_schema, config_manager.args, 'city_dimension_table')

    sql = """
        SELECT DISTINCT chartering_agency
        FROM fdic_banks_spark;
    """

    run_spark_query_and_save_to_psql(spark, sql,
                                     base_table_psql_schema, config_manager.args, 'chartering_agency_dimension_table')

    sql = """
        SELECT
            fdic_certificate_number,
            total_assets,
            total_deposits,
            equity_capital,
            net_income,
            return_on_assets,
            return_on_equity,
            state_fips_code,
            city,
            chartering_agency,
            effective_date
        FROM fdic_banks_spark;
    """

    run_spark_query_and_save_to_psql(spark, sql,
                                     base_table_psql_schema, config_manager.args, 'fact_table')

if __name__ == '__main__':
    root_path = os.path.dirname(os.path.abspath(__file__))
    drews_conf = {
        'data_path': '/Users/drewdifrancesco/Desktop/data',
        'root_path': root_path,
        's3_bucket': '',
        'psql_config_path': '/Users/drewdifrancesco/Desktop/repos/configs/psql_config',
        'service_key_path': '/Users/drewdifrancesco/Desktop/repos/configs/google_big_query/service_key.json',
        'table_name': 'fdic_banks',
        'jar_file_path': '/Users/drewdifrancesco/Desktop/jars/postgresql-42.7.3.jar'
    }

    main(override_args=drews_conf)

    # args = parse_args()
    # main(override_args = args)