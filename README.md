# RedShift migration to BQ

## DAG to migrate Redshift data to BQ in batch and incremental modes

### Architecture diagram

![RedShift data transfer - PoC.jpg](doc%2FRedShift%20data%20transfer%20-%20PoC.jpg)

### Architecture concerns

1. RedShift unload task should not affect usual BI operations. Based on the table size think about increasing cluster
   threads & RAM, enabling parallel unload option and file max size partitioning.
2. Parquet rows size equality to BQ inserted rows validation downloads Parquet files one by one to calculate total rows
   for exported files in a DAG run. Consider adjusting files size and Composer cluster HDD to be able to handle all DAGs
   simultaneously if they do validation at the same time. It's `minimum required space = (number of DAGs) * file_size`.
   To see the logic of getting file rows amount — look
   here [files.py](dags%2Fredshift_migration_ENTITY_NAME%2Fcommon%2Ffiles.py)#`calculate_total_rows`
3. Airflow
   task [redshift_ENTITY_NAME_migration_dag.py](dags%2Fredshift_migration_ENTITY_NAME%2Fredshift_ENTITY_NAME_migration_dag.py)#`count_files_total_rows`
   may take too long due to the amount and size of Parquet files which are downloaded to the Composer HDD and deleted
   sequentially. Consider expanding its `execution_timeout` parameter.
4. [common](dags%2Fcommon) package is general for all DAGs Changing it we should keep in mind it affects all of them and
   do in a backward compatible manner with regression testing. If we want to avoid it, custom DAG-specific changes
   should be applying within its directory as the separate sub-package.

## Environment

### AWS

#### <a name="aws-iam">IAM</a>

1. Create IAM
   user [redshift-sa](https://us-east-1.console.aws.amazon.com/iamv2/home?region=eu-north-1#/users/details/redshift-sa?section=permissions)
   with the
   policy [RedShift-Unload-To-S3](https://us-east-1.console.aws.amazon.com/iamv2/home?region=eu-north-1#/policies/details/arn%3Aaws%3Aiam%3A%3A<ROLE_ID>%3Apolicy%2FRedShift-Unload-To-S3?section=policy_permissions)
   which is created from the [RedShift-Unload-To-S3-policy.json](infra%2FRedShift-Unload-To-S3-policy.json)
2. Generate Access keys as the security credentials for yourself (name it by author name at the AWS console) and save to
   the [.secrets/credentials.json](.secrets%2Fcredentials.json).

#### RedShift

Resume existing RedShift Cluster from the snapshot

```shell
aws redshift resume-cluster --cluster-identifier <RS_CLUSTER_ID>

```

### GCP

Prepare VPC network and subnet, Cloud Composer, BQ Datasets and Secret Manager resource to run DAGs

```shell
gcloud compute networks create composer-vpc --subnet-mode=custom
tput bel
```

```shell
gcloud compute networks subnets create europe-central-2 --network=composer-vpc --region=europe-central2 --range=10.168.0.0/20
tput bel
```

Create BQ dataset

```shell
gcloud bigquery datasets create redshift_raw --project=<YOUR_GCP_PROJECT_ID> --location=europe-north1

```

#### Composer 2

Create Composer 2 environment, update it with the packages from the [gcp-requirements.txt](dags%2Fgcp-requirements.txt)
and set AWS connection to real values from [.secrets/credentials.json](.secrets%2Fcredentials.json) file.

```shell
gcloud composer environments create <YOUR_GCP_COMPOSER_ENV_ID> \
  --location=europe-central2 \
  --environment-size=SMALL \
  --network=projects/<YOUR_GCP_PROJECT_ID>/global/networks/composer-vpc \
  --subnetwork=projects/<YOUR_GCP_PROJECT_ID>/regions/europe-central2/subnetworks/europe-central-2 \
  --service-account=391173521045-compute@developer.gserviceaccount.com \
  --airflow-configs=core-secrets_backend=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend \
  --image-version=composer-2.2.1-airflow-2.4.3
gcloud composer environments update <YOUR_GCP_COMPOSER_ENV_ID> \
  --location europe-central2 \
  --update-pypi-packages-from-file dags/gcp-requirements.txt \
   --update-airflow-configs=core-lazy_load_plugins=False,webserver-reload_on_plugin_change=True
tput bel
 ```

Generate a GCP role to be used for BQ Data transfer

```shell
PROJECT=$(jq -r '.GCP_PROJECT' .secrets/credentials.json)
gcloud iam roles create bigQueryDataTransferRole \
  --project "<YOUR_GCP_PROJECT_ID>" \
  --title "BigQuery Data Transfer Role" \
  --description "Custom role for BigQuery Data Transfer Operator" \
  --permissions bigquery.transfers.update,bigquery.transfers.get,bigquery.datasets.get \
  --stage GA
gcloud projects add-iam-policy-binding "<YOUR_GCP_PROJECT_ID>" \
    --member user:391173521045-compute@developer.gserviceaccount.com \
    --role projects/<YOUR_GCP_PROJECT_ID>/roles/bigQueryDataTransferRole
gcloud projects add-iam-policy-binding "<YOUR_GCP_PROJECT_ID>" \
    --member user:391173521045-compute@developer.gserviceaccount.com \
    --role roles/secretmanager.secretAccessor

```

Add AWS connection

```shell
gcloud composer environments run <YOUR_GCP_COMPOSER_ENV_ID> --location=europe-central2 connections delete -- aws_default
AWS_ACCESS_KEY_ID=$(jq -r '.AWS_ACCESS_KEY_ID' .secrets/credentials.json)
AWS_SECRET_ACCESS_KEY=$(jq -r '.AWS_SECRET_ACCESS_KEY' .secrets/credentials.json)
REGION_NAME=$(jq -r '.REGION' .secrets/credentials.json)

gcloud composer environments run <YOUR_GCP_COMPOSER_ENV_ID> --location=europe-central2 \
  connections add -- aws_default --conn-type "aws" \
  --conn-login $AWS_ACCESS_KEY_ID \
  --conn-password $AWS_SECRET_ACCESS_KEY \
  --conn-extra="{\"region_name\":\"$REGION_NAME\"}"

```

Make user and Airflow UI Admin

```shell
gcloud composer environments run <YOUR_GCP_COMPOSER_ENV_ID> --location europe-central2 users add-role -- -e <PRINCIPAL_EMAIL> -r Admin

```

## Development workflow

### Local environment setup


#### Git SSH Auth

Create SSH public and private keys [How-To](https://docs.oracle.com/en/cloud/paas/big-data-cloud/csbdi/generating-secure-shell-ssh-public-private-key-pair.html). Then add public key to [AzDO org](https://dev.azure.com/oklev/_usersSettings/keys). Use password for that and save it to your password manager — you'll need it later.

Then set SSH config for AzDO domain. Add these lines to ~/.ssh/config **before** any wildcard entry:

```
Host ssh.dev.azure.com
  IdentityFile ~/.ssh/your_private_key_file_name
  IdentitiesOnly yes
  HostkeyAlgorithms +ssh-rsa
  PubkeyAcceptedKeyTypes=ssh-rsa 
```

Run `ssh -v -T git@ssh.dev.azure.com` and enter the password you've used for SSH key creation for the prompt. 

#### Python virtual environment

Create virtual env

```shell
python3.9 -m venv .migration-venv
```

To activate the virtual environment, you'll need to source the activate script located in the bin directory within the
environment. The exact command differs based on your operating system:

On Linux/macOS:

```shell
source .migration-venv/bin/activate
```

On Windows (Command Prompt):

```shell
.migration-venv\Scripts\activate.bat
```

On Windows (PowerShell):

```shell
.\.migration-venv\Scripts\Activate.ps1
```

#### Install pip packages

```shell
pip3 install -r dags/local-requirements.txt
```

#### Access to the Cloud

1. Install gcloud and aws CLIs
2. Login to Web and generate access tokens for your IAM Principal
3. Update [.secrets/credentials.json](.secrets%2Fcredentials.json) file according
   the [.secrets/credentials.example.json](.secrets%2Fcredentials.example.json) structure. Get values
   “AWS_ACCESS_KEY_ID” (visible), "AWS_SECRET_ACCESS_KEY" (to be generated by you)
   from [here](#aws-iam)

### Create Redshift table migration DAG

1. Figure out RedShift table name used for migration and the timestamp column to be used for incremental data fetching.
2. Copy existing DAG folder [redshift_migration_ENTITY_NAME](dags%2Fredshift_migration_ENTITY_NAME) as the new one and
   rename it in
   format _redshift_migration_entity_name_
3. Change `ts-incremental-column-name` at
   the [ENTITY_NAME-entity-config.json](dags%2Fredshift_migration_ENTITY_NAME%2FENTITY_NAME-entity-config.json) to the
   custom one from the table _(optional)_
4. Run `python3 generate_sql.py --entity_name "category"` with your parameters of from
   step 1 and follow output instructions. `--timestamp_column "ts-incremental-column-name"` is optional parameter for
   your custom column name. If alter table DDL statement was generated for `insert_time` usage - apply it
   for Redshift table.
5. Copy-paste unload SQL to
   your [unload_ENTITY_NAME.sql](dags%2Fredshift_migration_ENTITY_NAME%2Fsql%2Fredshift%2Funload_ENTITY_NAME.sql) and BQ
   validation SQL
   to [validate_ENTITY_NAME_bq_checksum.sql](dags%2Fredshift_migration_ENTITY_NAME%2Fsql%2Fbq%2Fvalidate_ENTITY_NAME_bq_checksum.sql)
6. Prepare schema for BQ at the
   new [ENTITY_NAME_schema.sql](dags%2Fredshift_migration_ENTITY_NAME%2Fsql%2Fbq%2FENTITY_NAME_schema.sql) identical to
   the RedShift one. _Reference:_ https://cloud.google.com/bigquery/docs/migration/redshift-sql
7. Replace ENTITY_NAME to yours at scripts, filenames and config values to the real one

#### DQ Tests

Compile 

```shell
cd tests && python compile.py event.yaml event-compiled.yaml
```

### Deploy DAG

#### Build common plugin

Build [common](dags%2Fcommon) is included at the [deploy.sh](deploy.sh)

#### Users

Call [deploy.sh](deploy.sh) script with your ENTITY_NAME as the first parameter.

```shell
./deploy.sh users
```

#### Event

```shell
./deploy.sh event
```