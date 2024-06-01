import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.edgemodifier import Label
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

sys.path.append("/opt/airflow/scripts/")
from kafka_topic_creation import create_kafka_topic
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.hooks.http import HttpHook

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
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"


def create_spark_connection():
    conn = Connection(
        conn_id='spark_default',
        conn_type='Spark',
        host='spark://spark-master-svc.spark.svc.cluster.local:7077',
        login='admin',
        password='admin',
        port='7077'
    )  # create a connection object
    session = settings.Session()  # get the session
    session.add(conn)
    session.commit()


create_spark_connection()


def alert_slack_channel(context):
    """ Alert to slack channel on failed dag
    :param context: airflow context object
    """
    if not SLACK_WEBHOOK_URL:
        # Do nothing if slack webhook not set up
        return

    last_task = context.get('task_instance')
    dag_name = last_task.dag_id
    execution_date = context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    failed_tis = [ti.task_id for ti in task_instances if ti.state == 'failed']
    title = f':red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks'
    msg_parts = {
        'Execution date': execution_date,
        'Failed Tasks': ', '.join(failed_tis),
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()

    http_hook = HttpHook(method='POST')
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({'text': msg})
    response = http_hook.run(endpoint=SLACK_WEBHOOK_URL, data=payload, headers=headers)
    if response.status_code != 200:
        raise ValueError(
            f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")


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
        on_failure_callback=alert_slack_channel,
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
        },
        on_failure_callback=alert_slack_channel
    )
    trip_generator = KubernetesPodOperator(
        namespace="airflow",
        task_id="trip_producer",
        image=TRIP_PRODUCER_IMAGE + ":main",
        cmds=["python3", 'trip-data-generation/trip_streaming_script.py'],
        arguments=[
            '--kafka_servers', KAFKA_PRODUCER_SERVERS,
            '--data_dir', DATA_DIR,
            '--send_speed', str(MESSAGE_SEND_SPEED),
            '--minio_endpoint', S3_ENDPOINT
        ],
        get_logs=True,
        in_cluster=True,
        image_pull_policy='Always',
        on_failure_callback=alert_slack_channel,
    )
    # stream_data_to_bronze = BashOperator(
    #     task_id="streaming_raw_data_to_bronze",
    #     bash_command=f'''
    #         spark-submit /opt/airflow/scripts/spark/stream_to_bronze.py \
    #             --spark_cluster {SPARK_CLUSTER} \
    #             --kafka_servers {KAFKA_CONSUMER_SERVERS} \
    #             --bucket_name {S3_BUCKET_NAME} \
    #             --path_location_csv {PATH_LOCATION_CSV} \
    #             --path_dpc_base_num_csv {PATH_DPC_BASE_NUM_CSV} \
    #             --s3_endpoint {S3_ENDPOINT} \
    #             --s3_access_key {S3_ACCESS_KEY} \
    #             --s3_secret_key {S3_SECRET_KEY} \
    #         ''',
    #     retries=2,
    #     on_failure_callback=alert_slack_channel
    # )
    stream_data_to_bronze = SparkSubmitOperator(
        task_id="stream_data_to_bronze",
        application="/opt/airflow/scripts/spark/stream_to_bronze.py",
        application_args=[
            "--spark_cluster", SPARK_CLUSTER,
            "--kafka_servers", KAFKA_CONSUMER_SERVERS,
            "--bucket_name", S3_BUCKET_NAME,
            "--path_location_csv", PATH_LOCATION_CSV,
            "--path_dpc_base_num_csv", PATH_DPC_BASE_NUM_CSV,
            "--s3_endpoint", S3_ENDPOINT,
            "--s3_access_key", S3_ACCESS_KEY,
            "--s3_secret_key", S3_SECRET_KEY
        ],
        conn_id=spark_default,
        on_failure_callback=alert_slack_channel
    )

    create_kafka_topic >> trip_generator
    create_kafka_topic >> stream_data_to_bronze
