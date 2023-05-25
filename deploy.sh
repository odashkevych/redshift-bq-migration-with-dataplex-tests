dags_folder_name="redshift_migration_$1"
dags_folder=$(gcloud composer environments describe <YOUR_GCP_COMPOSER_ENV_ID> --location europe-central2 --format='value(config.dagGcsPrefix)')
airflow_folder="${dags_folder%/*}"

echo "Deploying common to the DAGs folder $dags_folder"
gsutil -o "GSUtil:parallel_process_count=1" -m rsync -r -d "dags/common" "${dags_folder}/common"
echo "Deploying $dags_folder_name to the DAGs folder $dags_folder"
gsutil -o "GSUtil:parallel_process_count=1" -m rsync -r -d "dags/${dags_folder_name}" "${dags_folder}/${dags_folder_name}"