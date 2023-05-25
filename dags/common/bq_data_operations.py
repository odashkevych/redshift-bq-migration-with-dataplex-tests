import logging

import pandas_gbq
from pandas import DataFrame

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


def query_bq_df(sql: str) -> DataFrame:
    return pandas_gbq.read_gbq(sql, dialect='standard').dropna()


def query_bq_single_value(sql: str) -> str:
    df = query_bq_df(sql)
    sql_data = []
    for index, row in df.iterrows():
        single_row = []
        for column in list(df):
            single_row.append(str(row[column]))
        sql_data.append(single_row)
    if not sql_data or sql_data[0][0] == 'NaT':
        return ''
    else:
        return sql_data[0][0]


def get_latest_load_ts(**context: dict) -> str:
    full_table_path = get_full_table_id(context['project_id'], context['dataset_id'], context['table_id'])
    column_name = context['column_name']
    sql = f"SELECT COALESCE(MAX({column_name}), TIMESTAMP('1970-01-01 00:00:00 UTC')) FROM `{full_table_path}`"
    log.info(f"SQL: {sql}")
    load_ts = query_bq_single_value(sql)
    if not load_ts:
        log.warning("Latest load_ts in %s table wasn't found", context['table_id'])
    else:
        log.info("Latest load_ts in %s table: %s", context['table_id'], load_ts)
    return load_ts


def get_full_table_id(project_id, dataset_id, table_id):
    return f"{project_id}.{dataset_id}.{table_id}"
