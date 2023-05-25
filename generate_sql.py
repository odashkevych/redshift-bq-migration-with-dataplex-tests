import argparse
import json
import time

import boto3

INSERT_TIME_COLUMN = "insert_time"

with open('.secrets/credentials.json') as json_file:
    config = json.load(json_file)


def get_redshift_columns(table_name):
    # Specify your cluster details
    cluster_id = config['CLUSTER_ID']
    database = config['DATABASE']
    schema_name = config['SCHEMA_NAME']
    db_user = config['DB_USER']

    session = boto3.Session(
        aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
    )
    # Create a Redshift Data API client
    client = session.client('redshift-data', region_name='eu-north-1')
    # Get the table schema
    query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
            AND table_name   = '{table_name}'
        """
    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=query
    )

    # Wait for the query to finish executing
    while True:
        status_response = client.describe_statement(Id=response['Id'])
        status = status_response['Status']
        if status in ['FAILED', 'FINISHED']:
            break
        time.sleep(1)  # wait before checking status again

    if status == 'FAILED':
        print("Failed to execute query:", status_response['Error'])
        return

    # Fetch the result of the query
    result_response = client.get_statement_result(Id=response['Id'])
    return [{"name": record[0]["stringValue"], "type": record[1]["stringValue"]} for record in
            result_response['Records']]


def get_bq_sql(columns_list, table_name, timestamp_column):
    bq_schema_name = config['BQ_DATASET']
    bq_project = config['GCP_PROJECT']

    cols_concat = " || ', ' || ".join(
        f"CASE WHEN {column['name']} THEN 'true' ELSE 'false' END" if column['type'] == 'boolean'
        else f"COALESCE(CAST({column['name']} AS STRING), '')"
        for column in columns_list)
    equal_checksum_column = f"TO_HEX(MD5({cols_concat})) = checksum as equal_checksum"
    return f"SELECT {equal_checksum_column} FROM `{bq_project}.{bq_schema_name}.{table_name}` " \
           f"WHERE {timestamp_column} = '%(export_datetime)s'"


def get_redshift_select_sql(columns_list, table_name, timestamp_column):
    database = config['DATABASE']
    schema_name = config['SCHEMA_NAME']

    cols_concat = " || '', '' || ".join(
        f"CASE WHEN {column['name']} THEN ''true'' ELSE ''false'' END" if column['type'] == 'boolean'
        else f"COALESCE(CAST({column['name']} AS VARCHAR), '''')"
        for column in columns_list)
    checksum_subquery = f"MD5({cols_concat}) AS checksum"

    # Create the dynamic SQL query
    column_names = ', '.join(column['name'] for column in columns_list)
    return f"SELECT {column_names}, TO_TIMESTAMP(''%(export_datetime)s'', ''YYYY-MM-DD\"T\"HH24:MI:SS'') as export_datetime, {checksum_subquery} " \
           f"FROM {database}.{schema_name}.{table_name} " \
           f"WHERE {timestamp_column} > ''%(insert_time)s''"


def create_select_statements(table_name, timestamp_column):
    columns_list = get_redshift_columns(table_name)
    contains_timestamp_column = any(entry.get('name') == timestamp_column for entry in columns_list)
    if contains_timestamp_column:
        redshift_sql = get_redshift_select_sql(columns_list, table_name, timestamp_column)
        bq_sql = get_bq_sql(columns_list, table_name, timestamp_column)
        return {"bq": bq_sql, "redshift": redshift_sql}
    else:
        print(f"{timestamp_column} is missing at the table columns list!")
        return False


def create_adding_insert_ts_statement(table_name):
    database = config['DATABASE']
    schema_name = config['SCHEMA_NAME']
    return f"ALTER TABLE {database}.{schema_name}.{table_name} ADD COLUMN insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"


def generate_unload_query(select_sql, table_name):
    return f"unload ('{select_sql}') to " \
           f"'s3://{config['UNLOAD_BUCKET_NAME']}/unload/{table_name}/%(export_datetime)s/{table_name}_' " \
           f"iam_role DEFAULT " \
           f"FORMAT PARQUET ALLOWOVERWRITE parallel off maxfilesize 100 mb;"


def main(entity_name, timestamp_column):
    print(f"Entity {entity_name} with timestamp column {timestamp_column}" + "\n---------")
    rs_select_statement = create_select_statements(entity_name, timestamp_column)
    if timestamp_column == INSERT_TIME_COLUMN and rs_select_statement is False:
        print(
            "Alter RedShift table with the next statement and run this script again: " + create_adding_insert_ts_statement(
                entity_name))
    else:
        unload_query = generate_unload_query(rs_select_statement["redshift"], entity_name)
        print("Unload: " + unload_query + "\n---------")
        bq_select_statement = rs_select_statement["bq"]
        print("BQ validation statement: " + bq_select_statement + "\n---------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RedShift parameters')
    parser.add_argument('--entity_name', default='event', type=str, help=' RedShift table name')
    parser.add_argument('--timestamp_column', default=INSERT_TIME_COLUMN, type=str,
                        help=' RedShift table timestamp column to be used for incremental unloads')

    args = parser.parse_args()
    main(args.entity_name, args.timestamp_column)
