rules_bucket='gs://dataplex-dq-rules-dev'
echo "Deploying rules to the rules bucket $rules_bucket"
gsutil -o "GSUtil:parallel_process_count=1" -m rsync -r -d "$1" "${rules_bucket}"
