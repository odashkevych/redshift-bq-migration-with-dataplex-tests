import logging

from airflow.utils.dates import days_ago

from common import file_operations
from common.data_quality import create_dag

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

entity_name = "event"
config = file_operations.load_schema_from_json(f'redshift_migration_{entity_name}/{entity_name}-entity-config.json')
gcp_config = config['gcp']

dag = create_dag(entity_name, gcp_config, days_ago(1))
