import logging
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateTaskOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from common.dataplex import get_dataplex_task, get_dataplex_job_state, submit_dataplex_task

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME = "dataplex-clouddq-artifacts"


def __get_task_api_body(_gcp_config: dict, entity_name: str):
    dataplex_region = _gcp_config['dataplex']['region']
    service_acc = _gcp_config['dataplex']['service_acc']

    spark_file_full_path = f"gs://{PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{dataplex_region}/clouddq_pyspark_driver.py"
    # Public Cloud Storage bucket containing the driver code for executing data quality job.
    # There is one bucket per GCP region.
    clouddq_executable_file_path = f"gs://{PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{dataplex_region}/clouddq-executable.zip"
    # The Cloud Storage path containing the prebuilt data quality executable artifact hashsum.
    # There is one bucket per GCP region.
    clouddq_executable_hashsum_file_path = f"gs://{PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{dataplex_region}/clouddq-executable.zip.hashsum"

    # The gcs bucket to store data quality YAML configurations input to the data quality task.
    # You can have a single yaml file in .yml or yaml format or a .zip archive containing multiple YAML files.
    yaml_bucket_name = _gcp_config['dataplex']['yaml_bucket_name']
    environment = _gcp_config['environment']
    configs_path = f"gs://{yaml_bucket_name}/{entity_name}-{environment}.yaml"

    # The Google Cloud Project where the BQ jobs will be created
    gcp_project_id = _gcp_config['project']
    # The BigQuery dataset used for storing the intermediate data quality summary results
    # and the BigQuery views associated with each rule binding
    gcp_bq_dataset_id = f"{_gcp_config['dataset_id']}_dq_results"
    # The BigQuery table where the final results of the data quality checks are stored.
    target_bq_table = _gcp_config['table_id']
    gcp_bq_region = _gcp_config['dataset_region_id']  # GCP BQ region where the data is stored
    full_target_table_name = f"{gcp_project_id}.{gcp_bq_dataset_id}.{target_bq_table}"

    return {
        "spark": {
            "python_script_file": spark_file_full_path,
            "file_uris": [clouddq_executable_file_path,
                          clouddq_executable_hashsum_file_path,
                          configs_path
                          ],
            "infrastructure_spec": {
                "vpc_network": {
                    "sub_network": f"dataplex-{dataplex_region}-subnet"
                }
            }
        },
        "execution_spec": {
            "service_account": service_acc,
            "args": {
                "TASK_ARGS": f"clouddq-executable.zip, \
               ALL, \
               {configs_path}, \
              --gcp_project_id=\"{gcp_project_id}\", \
              --gcp_region_id=\"{gcp_bq_region}\", \
              --gcp_bq_dataset_id=\"{gcp_bq_dataset_id}\", \
              --target_bigquery_summary_table=\"{full_target_table_name}\""
            }
        },
        "trigger_spec": {
            "type_": "ON_DEMAND"
        },
        "description": "CloudDQ Airflow Task"
    }


# Dag is returned by a factory method
def dq_tasks(dag: DAG, entity_name: str, gcp_config: dict):
    dataplex_task_id = f"{entity_name}-dq-check"  # The unique identifier for the task
    # The Google Cloud Project, region and lake where the Dataplex task will be created
    dataplex_project_id = gcp_config['dataplex']['project']
    dataplex_region = gcp_config['dataplex']['region']
    dataplex_lake_id = gcp_config['dataplex']['lake_id']

    # this will check for the existing dataplex task
    get_dataplex_dq_task = BranchPythonOperator(
        task_id="get_dataplex_dq_task",
        python_callable=lambda: get_dataplex_task(dataplex_project_id, dataplex_region, dataplex_lake_id,
                                                  dataplex_task_id),
        provide_context=True,
        dag=dag,
    )

    dataplex_task_exists = BashOperator(
        task_id="task_exist",
        bash_command="echo 'Task Already Exists'",
        dag=dag,
    )
    dataplex_task_not_exists = BashOperator(
        task_id="task_not_exist",
        bash_command="echo 'Task not Present'",
        dag=dag,
    )
    dataplex_task_error = BashOperator(
        task_id="task_error",
        bash_command="echo 'Error in fetching dataplex task details'",
        dag=dag,
    )

    # this will check for the existing dataplex task
    create_dataplex_task_job = PythonOperator(
        task_id="create_dataplex_task_job",
        python_callable=lambda: submit_dataplex_task(dataplex_project_id, dataplex_region, dataplex_lake_id,
                                                     dataplex_task_id),
        provide_context=True
    )

    # this will create a new dataplex task with a given task id
    create_dataplex_task = DataplexCreateTaskOperator(
        project_id=dataplex_project_id,
        region=dataplex_region,
        lake_id=dataplex_lake_id,
        body=__get_task_api_body(gcp_config, entity_name),
        dataplex_task_id=dataplex_task_id,
        task_id="create_dataplex_task",
        trigger_rule="none_failed_min_one_success",
    )

    # this will get the status of dataplex task job
    dataplex_task_state = BranchPythonOperator(
        task_id="dataplex_task_state",
        python_callable=lambda: get_dataplex_job_state(dataplex_project_id, dataplex_region, dataplex_lake_id,
                                                       dataplex_task_id),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        provide_context=True,

    )

    dataplex_task_success = BashOperator(
        task_id="SUCCEEDED",
        bash_command="echo 'Job Completed Successfully'",
        dag=dag,
    )
    dataplex_task_failed = BashOperator(
        task_id="FAILED",
        bash_command="echo 'Job Failed'",
        dag=dag,
    )

    get_dataplex_dq_task >> [dataplex_task_exists, dataplex_task_not_exists, dataplex_task_error]

    dataplex_task_exists >> create_dataplex_task_job >> dataplex_task_state
    dataplex_task_not_exists >> create_dataplex_task >> dataplex_task_state

    dataplex_task_state >> [dataplex_task_success, dataplex_task_failed]


def create_dag(entity_name: str, gcp_config: dict, start_date: datetime.date = None, dag_name: str = None) -> DAG:
    if dag_name is None:
        dag_name = f"{entity_name}-dq-check"
    dag = DAG(
        dag_id=dag_name,
        start_date=start_date,
        default_args={'retries': 1, 'retry_delay': timedelta(minutes=5), 'email': 'odash@softserveinc.com'},
        description=f'{entity_name} data quality checks DAG',
        schedule=None,
        catchup=False,
        tags=['dataplex-tests', 'beta-8.1', entity_name],
    )

    dq_tasks(dag, entity_name, gcp_config)

    return dag
