import logging
import time
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryCreateDataTransferOperator, \
    BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import \
    CloudDataTransferServiceS3ToGCSOperator
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
from airflow.utils.dates import days_ago

from common import bq_data_operations
from common import file_operations

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

entity_name = "<ENTITY_NAME>"
config = file_operations.load_schema_from_json(f'redshift_migration_{entity_name}/{entity_name}-entity-config.json')
gcp_config = config['gcp']
target_bq_table_sink = bq_data_operations.get_full_table_id(gcp_config['project'], gcp_config['dataset_id'],
                                                            gcp_config['table_id'])
dataset_region_id = gcp_config['dataset_region_id']
aws_config = config['aws']
ts_incremental_column_name = config['ts_incremental_column_name']
run_dq_tests: bool = config['run_dq_tests']

with DAG(
        dag_id=f'redshift-to-bq-{entity_name}-migration',
        start_date=days_ago(1),
        default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
        description=f'Redshift {entity_name} table DAG',
        schedule_interval=None,
        catchup=False,
        tags=['redshift-data-migration', 'beta-6.0'],
) as dag:
    generate_export_datetime = PythonOperator(
        task_id='generate_export_datetime',
        provide_context=True,
        python_callable=lambda: datetime.utcnow().isoformat(timespec="seconds"),
        dag=dag)

    export_datetime = "{{ task_instance.xcom_pull('generate_export_datetime') }}"

    bq_create_table = BigQueryExecuteQueryOperator(
        task_id='bq_create_table',
        sql=f"/sql/bq/{entity_name}_schema.sql",
        use_legacy_sql=False,
        dag=dag)

    get_previous_insert_time = PythonOperator(
        task_id='get_previous_insert_time',
        provide_context=True,
        python_callable=bq_data_operations.get_latest_load_ts,
        op_kwargs=
        {
            'project_id': gcp_config['project'],
            'dataset_id': gcp_config['dataset_id'],
            'table_id': gcp_config['table_id'],
            'column_name': ts_incremental_column_name,
        },
        dag=dag)
    previous_insert_time = "{{ti.xcom_pull(task_ids='get_previous_insert_time')}}"

    check_if_table_has_new_records = RedshiftDataOperator(
        task_id='check_if_table_has_new_records',
        aws_conn_id='aws_default',
        db_user='awsuser',
        sql=file_operations.read_sql_file(
            f'redshift_migration_{entity_name}/sql/redshift/new_records_exist_after_ts.sql')
            % {'column_name': ts_incremental_column_name, 'insert_time': previous_insert_time,
               'table_id': aws_config['table_id']},
        database='dev',
        cluster_identifier='<RS_CLUSTER_ID>',
        return_sql_result=True,
        dag=dag
    )


    def validate_table_has_new_records_decide_which_path(**kwargs):
        response = kwargs['ti'].xcom_pull(task_ids='check_if_table_has_new_records')
        records = response.get('Records', [])

        if records and isinstance(records[0], list) and isinstance(records[0][0], dict):
            return records[0][0].get('booleanValue', False)
        return False


    validate_table_has_new_records = ShortCircuitOperator(
        task_id='validate_table_has_new_records',
        python_callable=validate_table_has_new_records_decide_which_path
    )

    unload_to_s3 = RedshiftDataOperator(
        task_id='unload_to_s3',
        aws_conn_id='aws_default',
        db_user='awsuser',
        sql=file_operations.read_sql_file(f'redshift_migration_{entity_name}/sql/redshift/unload_{entity_name}.sql')
            % {'table_id': aws_config['table_id'], 'insert_time': previous_insert_time,
               'export_datetime': export_datetime},
        database='dev',
        cluster_identifier='<RS_CLUSTER_ID>',
        dag=dag
    )


    def get_s3_unload_files_wildcard(**kwargs):
        return f"s3://{aws_config['bucket']}/{aws_config['path']}{export_datetime}/{aws_config['file_prefix']}*{aws_config['file_format']}"


    s3_key_sensor = S3KeySensor(
        task_id='s3_key_sensor',
        bucket_key=get_s3_unload_files_wildcard(),
        wildcard_match=True,
        timeout=18 * 60 * 60,
        poke_interval=120
    )

    # create the Resource in Secret Manager. at the format .aws/secret-manager-credentials.example.json
    create_s3_transfer_job = CloudDataTransferServiceS3ToGCSOperator(
        task_id='create_s3_transfer_job',
        s3_bucket=aws_config['bucket'],
        gcs_bucket=gcp_config['bucket'],
        s3_path=aws_config['path'] + export_datetime,
        gcs_path=gcp_config['path'] + export_datetime,
        project_id=gcp_config['project'],
        aws_conn_id="aws_default",
        schedule=None,
        transfer_options=dict(
            deleteObjectsFromSourceAfterTransfer=True,
            overwriteObjectsAlreadyExistingInSink=True
        ),
        description=f"S3 {entity_name} transfer for {export_datetime}"
    )

    create_bq_transfer = BigQueryCreateDataTransferOperator(
        task_id='create_bq_transfer',
        transfer_config={
            "destination_dataset_id": gcp_config["dataset_id"],
            "display_name": f"BQ {entity_name} import for {export_datetime}",
            "data_source_id": "google_cloud_storage",
            "schedule_options": {"disable_auto_scheduling": True},
            "params": {
                "max_bad_records": "0",
                "skip_leading_rows": "0",
                "write_disposition": "APPEND",
                "data_path_template": f"gs://{gcp_config['bucket']}/{gcp_config['path']}{export_datetime}/{gcp_config['file_prefix']}*{gcp_config['file_format']}",
                "destination_table_name_template": gcp_config["table_id"],
                "file_format": "PARQUET"
            },
        }

    )

    transfer_config_id_ = "{{ task_instance.xcom_pull(task_ids='create_bq_transfer', key='transfer_config_id') }}"

    run_bq_transfer_job = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id='run_bq_transfer_job',
        location=dataset_region_id,
        transfer_config_id=transfer_config_id_,
        project_id=gcp_config['project'],
        requested_run_time={"seconds": int(time.time() + 60)},
    )

    bq_transfer_job_run_id = "{{ task_instance.xcom_pull('run_bq_transfer_job', key='run_id') }}"

    bq_transfer_job_succeeded = BigQueryDataTransferServiceTransferRunSensor(
        task_id='bq_transfer_job_succeeded',
        location=dataset_region_id,
        run_id=bq_transfer_job_run_id,
        transfer_config_id=transfer_config_id_,
        expected_statuses='SUCCEEDED'
    )


    def handle_failure(**kwargs):
        # Here, you can put the logic for what should happen if the BigQuery data transfer job fails.
        log.error(kwargs)
        pass


    count_files_total_rows = PythonOperator(
        task_id='count_files_total_rows',
        python_callable=file_operations.calculate_total_rows,
        op_kwargs={
            'bucket_name': f"{gcp_config['bucket']}",
            'prefix': f"{gcp_config['path']}{export_datetime}/{gcp_config['file_prefix']}"
        },
        execution_timeout=timedelta(minutes=10),  # Increase timeout to 10 minutes
    )

    get_bq_total_rows = PythonOperator(
        task_id='get_bq_total_rows',
        provide_context=True,
        python_callable=bq_data_operations.query_bq_single_value,
        op_kwargs=
        {
            'sql': file_operations.read_sql_file(
                f'redshift_migration_{entity_name}/sql/bq/get_amount_of_inserted_rows.sql') % {
                       'table_id': target_bq_table_sink,
                       'export_datetime': export_datetime
                   }
        },
        dag=dag)


    def validate_amount_is_eq(**kwargs):
        ti = kwargs['ti']
        bq_num = ti.xcom_pull(task_ids='get_bq_total_rows')
        files_num = ti.xcom_pull(task_ids='count_files_total_rows')
        log.info(f"BQ inserted rows: {bq_num}, Parquet files total rows: {files_num}")
        return str(bq_num) == str(files_num)


    validate_rows_number_equal = ShortCircuitOperator(
        task_id='validate_rows_number_equal',
        python_callable=validate_amount_is_eq
    )

    compare_redshift_checksum_with_bq = PythonOperator(
        task_id='compare_redshift_checksum_with_bq',
        provide_context=True,
        python_callable=bq_data_operations.query_bq_single_value,
        op_kwargs=
        {
            'sql': file_operations.read_sql_file(
                f'redshift_migration_{entity_name}/sql/bq/validate_{entity_name}_bq_checksum.sql')
                   % {'table_id': target_bq_table_sink, 'export_datetime': export_datetime},
        },
        dag=dag)

    validate_checksum = ShortCircuitOperator(
        task_id='validate_checksum',
        provide_context=True,
        python_callable=lambda **kwargs: kwargs['ti'].xcom_pull(task_ids='compare_redshift_checksum_with_bq') == 'True',
    )

    trigger_data_quality_dag = TriggerDagRunOperator(
        task_id='trigger_data_quality_dag',
        trigger_dag_id=f'{entity_name}-data-quality-check',
        wait_for_completion=True,
        dag=dag
    )

    generate_export_datetime >> bq_create_table >> get_previous_insert_time >> check_if_table_has_new_records >> \
    validate_table_has_new_records >> unload_to_s3 >> s3_key_sensor >> create_s3_transfer_job >> create_bq_transfer >> \
    run_bq_transfer_job >> bq_transfer_job_succeeded >> [count_files_total_rows, get_bq_total_rows] >> \
    validate_rows_number_equal >> compare_redshift_checksum_with_bq >> validate_checksum

    if run_dq_tests:
        validate_checksum.set_downstream(trigger_data_quality_dag)
