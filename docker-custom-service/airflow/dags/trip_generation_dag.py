import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task_group
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import V1ResourceRequirements
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
start_date = datetime(2024, 6, 9)
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEB_HOOK")

kafka_resource_requirements = V1ResourceRequirements(
    requests={'memory': '400Mi', 'cpu': '500m'},
    limits={'memory': '1000Mi', 'cpu': '1000m'}
)
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
    create_topic = PythonOperator(
        task_id="create_topic",
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
        # kafka_yellow_trip_producer = KubernetesPodOperator(
        #     namespace="airflow",
        #     task_id="kafka_yellow_trip_producer",
        #     image=TRIP_PRODUCER_IMAGE + ":main",
        #     cmds=["python3", 'yellow_trip_streaming_script.py'],
        #     arguments=[
        #         '--kafka_servers', KAFKA_PRODUCER_SERVERS,
        #         '--data_dir', DATA_DIR,
        #         '--send_speed', str(MESSAGE_SEND_SPEED),
        #         '--minio_endpoint', S3_ENDPOINT
        #     ],
        #     get_logs=True,
        #     in_cluster=True,
        #     image_pull_policy='Always',
        #     on_failure_callback=alert_slack_channel,
        #     on_finish_action="delete_pod"
        # )

        kafka_fhvhv_trip_producer = KubernetesPodOperator(
            namespace="airflow",
            task_id="kafka_fhvhv_trip_producer",
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
            container_resources=kafka_resource_requirements,
            on_failure_callback=alert_slack_channel,
            on_finish_action="delete_pod"
        )
        kafka_fhvhv_trip_producer
        # kafka_yellow_trip_producer


    @task_group(default_args={'retries': 3})
    def trip_consuming():
        stream_fhvhv_to_bronze = SparkKubernetesOperator(
            task_id='stream_fhvhv_to_bronze',
            namespace='spark',
            application_file='/kubernetes/bronze_fhvhv_streaming.yaml',
            kubernetes_conn_id='kubernetes_default',
            on_failure_callback=alert_slack_channel,
            image_pull_policy='Always',
            do_xcom_push=False,
            on_finish_action="delete_pod",
            delete_on_termination=True
        )
        # stream_yellow_to_bronze = SparkKubernetesOperator(
        #     task_id='stream_yellow_to_bronze',
        #     namespace='spark',
        #     application_file='/kubernetes/bronze_yellow_streaming.yaml',
        #     kubernetes_conn_id='kubernetes_default',
        #     on_failure_callback=alert_slack_channel,
        #     image_pull_policy='Always',
        #     is_delete_operator_pod=True,
        #     delete_on_termination=True
        # )
        # stream_yellow_to_bronze
        stream_fhvhv_to_bronze

    csv_to_bronze = SparkKubernetesOperator(
       task_id='csv_to_bronze',
       namespace='spark',
       retries=2,
       application_file='/kubernetes/csv_to_bronze.yaml',
       kubernetes_conn_id='kubernetes_default',
       on_failure_callback=alert_slack_channel,
       image_pull_policy='Always',
       on_finish_action="delete_pod",
       delete_on_termination=True
    )

    csv_to_bronze
    create_topic >> Label("Stream data") >> kafka_streaming()
    create_topic >> Label("Consume data") >> trip_consuming()
