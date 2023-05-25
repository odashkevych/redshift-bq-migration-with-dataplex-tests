#!/bin/bash

project_id=<YOUR_GCP_PROJECT_ID>

# List all transfer configurations and get their names
configs=$(gcloud transfer jobs list --project=${project_id} --format="value(name)")
echo "Removing data transfer jobs"
# Loop over the configurations and delete each one
for config in $configs
do
  gcloud transfer jobs delete $config --project=${project_id} -q
done
echo "Removing BQ data transfer jobs"