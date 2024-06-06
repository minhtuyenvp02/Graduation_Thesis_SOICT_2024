import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.edgemodifier import Label
from airflow.decorators import task_group
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.trigger_rule import TriggerRule
sys.path.append("/opt/airflow/scripts/spark")
sys.path.append("/opt/airflow/scripts/")
from kafka_topic_creation import create_kafka_topic

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
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEB_HOOK")

def alert_slack_channel(context: dict):
    """ Alert to slack channel on failed dag

    :param context: airflow context object
    """
    if not SLACK_WEBHOOK_URL:
        # Do nothing if slack webhook not set up
        return

    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    error_message = context.get('exception') or context.get('reason')
    execution_date = context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis = [file_and_link_template.format(log_url=ti.log_url, name=ti.task_id)
                  for ti in task_instances
                  if ti.state == 'failed']
    title = f':red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks'
    msg_parts = {
        'Execution date': execution_date,
        'Failed Tasks': ', '.join(failed_tis),
        'Error': error_message
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()

    SlackWebhookHook(
        slack_webhook_conn_id='slack_default'
    ).send_text(msg)


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
        catchup=False,
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


    @task_group(default_args={'retries': 1})
    def kafka_streaming():
        yellow_trip_generator = KubernetesPodOperator(
            namespace="airflow",
            task_id="yellow_trip_producer",
            image=TRIP_PRODUCER_IMAGE + ":main",
            cmds=["python3", 'yellow_trip_streaming_script.py'],
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

        fhvhv_trip_generator = KubernetesPodOperator(
            namespace="airflow",
            task_id="fhvhv_trip_producer",
            image=TRIP_PRODUCER_IMAGE + ":main",
            cmds=["python3", 'fhvhv_trip_streaming_script.py'],
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
        yellow_trip_generator
        fhvhv_trip_generator


    stream_data_to_bronze = SparkKubernetesOperator(
        task_id='stream_data_to_bronze',
        namespace='spark',
        application_file='/kubernetes/spark-pi.yaml',
        kubernetes_conn_id='kubernetes_default',
        on_failure_callback=alert_slack_channel,
        do_xcom_push=True,
    )
    # stream_data_to_bronze = SparkSubmitOperator(
    #     task_id="stream_data_to_bronze",
    #     packages='org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
    #     application="/opt/airflow/scripts/spark/stream_to_bronze.py",
    #     application_args=[
    #         "--spark_cluster", SPARK_CLUSTER,
    #         "--kafka_servers", KAFKA_CONSUMER_SERVERS,
    #         "--bucket_name", S3_BUCKET_NAME,
    #         "--path_location_csv", PATH_LOCATION_CSV,
    #         "--path_dpc_base_num_csv", PATH_DPC_BASE_NUM_CSV,
    #         "--s3_endpoint", S3_ENDPOINT,
    #         "--s3_access_key", S3_ACCESS_KEY,
    #         "--s3_secret_key", S3_SECRET_KEY
    #     ],
    #     conf={'spark.driver.host': '10.112.1.9',
    #           "spark.kubernetes.driver.service.deleteOnTermination": "true"},
    #     total_executor_cores=1,
    #     executor_cores=1,
    #     executor_memory='2g',
    #     num_executors=2,
    #     driver_memory='2g',
    #     conn_id='spark_default',
    #     verbose=True,
    #     on_failure_callback=alert_slack_channel
    # )


    create_kafka_topic >> Label("Topics created") >> kafka_streaming()
    create_kafka_topic >> Label("Consume data") >> stream_data_to_bronze
