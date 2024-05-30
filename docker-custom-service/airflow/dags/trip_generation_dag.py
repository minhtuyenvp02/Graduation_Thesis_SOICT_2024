import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.edgemodifier import Label

sys.path.append("/opt/airflow/scripts/")
from kafka_topic_creation import create_kafka_topic
from airflow.utils.trigger_rule import TriggerRule

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
start_date = datetime(2024, 6, 29)

default_args = {
    "owner": "airflow",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}

with DAG(
        dag_id="trip_streaming_kafka",
        start_date=start_date,
        schedule="@once",
        description="Streaming trip record to kafka topic",
        default_args=default_args,
        tags=["trip-generator", "producer"],
        catchup=False
) as dag:
    create_kafka_topic = PythonOperator(
        task_id="create_kafka_topic",
        python_callable=create_kafka_topic,
        op_kwargs={
            "kafka_servers": KAFKA_PRODUCER_SERVERS,
            "topics": TOPICS,
            "n_partitions": 3,
            "n_nodes": 2
        }
    )
    trip_generator = KubernetesPodOperator(
        namespace="airflow",
        task_id="trip_producer",
        image=TRIP_PRODUCER_IMAGE + ":main",
        cmds=["python3", 'trip_streaming_script.py'],
        arguments=[
            '--kafka_servers', KAFKA_PRODUCER_SERVERS,
            '--data_dir', DATA_DIR,
            '--send_speed', str(MESSAGE_SEND_SPEED),
            '--minio_endpoint', S3_ENDPOINT
        ],
        get_logs=True,
        in_cluster=True,
        image_pull_policy='Always'
    )
    send_error_email = EmailOperator(
        task_id="send_error_email",
        to='minhtuyenpa@gmail.com',
        subject="Producer Notification",
        retries=0,
        trigger_rule=TriggerRule.ONE_FAILED,
        html_content='ERROR!!!,\n\nThere was an error in Trip Producer task.',
        dag=dag,
    )

    stream_data_to_bronze = BashOperator(
        task_id="streaming_raw_data_to_bronze",
        bash_command=f'''
            spark-submit /opt/airflow/scripts/spark/stream_to_bronze.py
                --spark_cluster {SPARK_CLUSTER} \
                --kafka_servers {KAFKA_CONSUMER_SERVERS} \
                --bucket_name {S3_BUCKET_NAME} \
                --path_location_csv {PATH_LOCATION_CSV} \
                --path_dpc_base_num_csv {PATH_DPC_BASE_NUM_CSV} \
                --s3_endpoint {S3_ENDPOINT} \
                --s3_access_key {S3_ACCESS_KEY} \
                --s3_secret_key {S3_SECRET_KEY} \
            ''',
        retries=2
    )

    create_kafka_topic >> trip_generator >> Label("On error") >> send_error_email
    stream_data_to_bronze