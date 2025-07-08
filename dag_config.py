# dag_config.py
from datetime import datetime, timedelta

# Constantes
LOCAL_DIR = "/opt/airflow/data"
LOG_DIR = "/opt/airflow/logs"
BUCKET_NAME = "bronze"
BUCKET_PATH = "datafile"
SSH_CONN_ID = "ssh_connection"
MINIO_CONN_ID = "minio_connect"
DB_CONN_ID = "transfer_db"

# Nombre de threads par worker
MAX_WORKERS = 2

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}