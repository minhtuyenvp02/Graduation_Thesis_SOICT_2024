import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor

sys.path.append("/opt/airflow/scripts/")
from kafka_topic_creation import create_kafka_topic

sys.path.append("/opt/airflow/scripts/spark")

KAFKA_PRODUCER_SERVERS = Variable.get("KAFKA_PRODUCER_SERVERS")
KAFKA_CONSUMER_SERVERS = Variable.get("KAFKA_CONSUMER_SERVERS")
PATH_LOCATION_CSV = Variable.get("PATH_LOCATION_CSV")
PATH_DPC_BASE_NUM_CSV = Variable.get("PATH_DPC_BASE_NUM_CSV")
SPARK_CLUSTER = Variable.get("SPARK_CLUSTER")
S3_ENDPOINT = Variable.get("S3_ENDPOINT")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
TOPICS = Variable.get("TOPIC").split(',')
TRIP_PRODUCER_IMAGE = Variable.get("TRIP_PRODUCER_IMAGE")
DATA_DIR = Variable.get("DATA_DIR")
MESSAGE_SEND_SPEED = Variable.get("MESSAGE_SEND_SPEED")
start_date = datetime(2024, 5, 30)

default_args = {
    "owner": "airflow",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": start_date,
}
with DAG(
    default_args=default_args,
    dag_id="trip_tracking_daily",
    schedule_interval='@daily',
    tags=["trip_tracking_daily", "medallion_architecture"],
    catchup=False,
) as dag:

    gold_fact_yellow_tracking = BashOperator(
        task_id="gold_update_yellow_tracking_daily",
        bash_command=f'''
                        spark-submit /opt/airflow/scripts/spark/gold_fact_yellow_tracking.py
                            --spark_cluster {SPARK_CLUSTER} \
                            --bucket_name {S3_BUCKET_NAME} \
                            --s3_endpoint {S3_ENDPOINT} \
                            --s3_access_key {S3_ACCESS_KEY} \
                            --s3_secret_key {S3_SECRET_KEY} \
                        '''
    )
    gold_fact_fhvhv_tracking = BashOperator(
        task_id="gold_update_fhvhv_tracking_daily",
        bash_command=f'''
                           spark-submit /opt/airflow/scripts/spark/gold_fact_fhvhv_tracking.py
                               --spark_cluster {SPARK_CLUSTER} \
                               --bucket_name {S3_BUCKET_NAME} \
                               --s3_endpoint {S3_ENDPOINT} \
                               --s3_access_key {S3_ACCESS_KEY} \
                               --s3_secret_key {S3_SECRET_KEY} \
                           '''
    )
    gold_fact_fhvhv_tracking
    gold_fact_yellow_tracking