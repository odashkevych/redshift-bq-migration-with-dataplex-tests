import argparse
import json
from datetime import datetime

import jaydebeapi

INSERT_TIME_COLUMN = "insert_time"
JDBC_DRIVER_PATH = "lib/redshift-jdbc42-2.1.0.3.jar"
export_datetime = datetime.utcnow().isoformat(timespec="seconds")


def load_config(env):
    with open(f'.secrets/credentials-{env}.json') as json_file:
        return json.load(json_file)


def get_redshift_columns(table_name, config):
    # Replace these variables with your Redshift connection details
    host = config["EDH_HOST"]
    port = config["EDH_PORT"]
    dbname = config["EDH_DATABASE"]
    user = config["EDH_USERNAME"]
    password = config["EDH_PASSWORD"]

    # JDBC connection string
    url = f"jdbc:redshift://{host}:{port}/{dbname}"

    # JDBC driver and connection details
    driver = "com.amazon.redshift.jdbc42.Driver"
    jars = [JDBC_DRIVER_PATH]
    connection_args = {
        "host": url,
        "port": port,
        "user": user,
        "password": password,
    }

    # Establish JDBC connection
    connection = jaydebeapi.connect(driver, url, connection_args, jars)

    # Create a cursor
    cursor = connection.cursor()
    schema_name = config["EDH_SCHEMA_NAME"]
    # Example SQL query
    sql_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE " \
                f"table_schema = '{schema_name}' " \
                f"AND table_name  = '{table_name}'"

    # Execute the query
    cursor.execute(sql_query)

    # Fetch and print the results
    result_response = cursor.fetchall()
    result = [{"name": record[0], "type": record[1]} for record in result_response]
    # Close cursor and connection
    cursor.close()
    connection.close()
    return result


def get_bq_sql(columns_list, table_name, config):
    bq_schema_name = config['BQ_DATASET']
    bq_project = config['GCP_PROJECT']

    cols_concat = " || ', ' || ".join(get_bq_cast_sql(column) for column in columns_list)
    equal_checksum_column = f"TO_HEX(MD5({cols_concat})) as bq_md5_checksum, checksum"
    return f"WITH bq_checksum AS (SELECT {equal_checksum_column} FROM `{bq_project}.{bq_schema_name}.{table_name}`) SELECT checksum, bq_md5_checksum, (bq_md5_checksum = checksum) AS are_equal FROM bq_checksum"


def get_bq_cast_sql(column):
    if column['type'] == 'boolean':
        return f"CASE WHEN {column['name']} THEN 'true' ELSE 'false' END"
    elif 'timestamp' in column['type']:
        return f"COALESCE(CAST({column['name']} AS STRING FORMAT 'YYYY-MM-DD HH24:MI:SS.FF6'), '')"
    elif 'numeric' in column['type']:
        return f"COALESCE(CAST({column['name']} AS STRING FORMAT 'FM999999999D99999'), '')"
    else:
        return f"COALESCE(CAST({column['name']} AS STRING), '')"


def get_redshift_select_sql(columns_list, table_name, config):
    database = config['EDH_DATABASE']
    schema_name = config['EDH_SCHEMA_NAME']

    cols_concat = " || '', '' || ".join(get_rs_cast_sql(column) for column in columns_list)
    checksum_subquery = f"MD5({cols_concat}) AS checksum"

    # Create the dynamic SQL query
    column_names = ', '.join(column['name'] for column in columns_list)
    return f"SELECT {column_names}, TO_TIMESTAMP(''{export_datetime}'', ''YYYY-MM-DD\"T\"HH24:MI:SS'') as export_datetime, {checksum_subquery} " \
           f"FROM {database}.{schema_name}.{table_name}"


def get_rs_cast_sql(column):
    if column['type'] == 'boolean':
        return f"CASE WHEN {column['name']} THEN ''true'' ELSE ''false'' END"
    elif column['type'] == 'numeric':
        return f"COALESCE(TO_CHAR({column['name']}, ''FM999999999D99999''), '''')"
    elif 'timestamp' in column['type']:
        return f"COALESCE(TO_CHAR({column['name']}, ''YYYY-MM-DD HH24:MI:SS.US''), '''')"
    else:
        return f"COALESCE(CAST({column['name']} AS VARCHAR), '''')"


def create_select_statements(table_name, config):
    columns_list = get_redshift_columns(table_name, config)
    redshift_sql = get_redshift_select_sql(columns_list, table_name, config)
    bq_sql = get_bq_sql(columns_list, table_name, config)
    return {"bq": bq_sql, "redshift": redshift_sql}


def generate_unload_query(select_sql, table_name, config):
    return f"unload ('{select_sql}') to " \
           f"'s3://{config['UNLOAD_BUCKET_NAME']}/unload/{table_name}/{export_datetime}/{table_name}_' " \
           f"iam_role '{config['EDH_IAM_ROLE']}' " \
           f"FORMAT PARQUET ALLOWOVERWRITE parallel off maxfilesize 100 mb;"


def generate_unload_rows_count_query(table_name, config):
    return f"SELECT sum(line_count) FROM stl_unload_log WHERE path like " \
           f"'s3://{config['UNLOAD_BUCKET_NAME']}/unload/{table_name}/{export_datetime}/{table_name}_'"


def main(table_name, env):
    config = load_config(env)
    print(f"Entity {table_name}\n---------")
    rs_select_statement = create_select_statements(table_name, config)
    unload_query = generate_unload_query(rs_select_statement["redshift"], table_name, config)
    unload_rows_sum_query = generate_unload_rows_count_query(table_name, config)
    print("Unload: " + unload_query + "\n---------")
    print("Unload count: " + unload_rows_sum_query + "\n---------")
    bq_select_statement = rs_select_statement["bq"]
    print("BQ validation statement: " + bq_select_statement + "\n---------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RedShift parameters')
    parser.add_argument('--table_name', default='product_dim', type=str, help='EDH table name')
    parser.add_argument('--env', default='prod', type=str, choices=['qa', 'prod'], help='Data environment')

    args = parser.parse_args()
    main(args.table_name, args.env)
