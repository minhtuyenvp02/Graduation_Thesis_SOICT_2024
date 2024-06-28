import sys
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from kubernetes.client import V1ResourceRequirements
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
start_date = datetime(2024, 6, 26)
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEB_HOOK")


silver_transform_resource_requirements = V1ResourceRequirements(
    requests={'memory': '256Mi', 'cpu': '200m'},
    limits={'memory': '512Mi', 'cpu': '500m'}
)


def alert_slack_channel(context: dict):
    if not SLACK_WEBHOOK_URL:
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
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": start_date,
}
with DAG(
        default_args=default_args,
        dag_id="data_quality_pipeline",
        schedule_interval='0 */2 * * *',
        tags=["lakehouse processing", "medallion architecture"],
        catchup=False,
        on_failure_callback=alert_slack_channel,
) as dag:
    @task_group(default_args={'retries': 3})
    def silver_transform():
        # silver_yellow_transform = SparkKubernetesOperator(
        #     task_id='silver_yellow_transform',
        #     namespace='spark',
        #     application_file='/kubernetes/silver_yellow_transform.yaml',
        #     kubernetes_conn_id='kubernetes_default',
        #     on_failure_callback=alert_slack_channel,
        #     image_pull_policy='Always',
        #     do_xcom_push=True,
        #     is_delete_operator_pod=True,
        #     delete_on_termination=True
        # )
        silver_fhvhv_transform = SparkKubernetesOperator(
            task_id='silver_fhvhv_transform',
            namespace='spark',
            application_file='/kubernetes/silver_fhvhv_transform.yaml',
            kubernetes_conn_id='kubernetes_default',
            on_failure_callback=alert_slack_channel,
            container_resources=silver_transform_resource_requirements,
            image_pull_policy='Always',
            do_xcom_push=False,
            on_finish_action="delete_pod",
            delete_on_termination=True
        )
        # silver_yellow_transform
        silver_fhvhv_transform


    @task_group(default_args={'retries': 2})
    def gold_load():
        gold_load_fhvhv_fact = SparkKubernetesOperator(
            task_id='gold_load_fhvhv_fact',
            namespace='spark',
            application_file='/kubernetes/gold_load_fhvhv_fact.yaml',
            kubernetes_conn_id='kubernetes_default',
            on_failure_callback=alert_slack_channel,
            image_pull_policy='Always',
            container_resources=silver_transform_resource_requirements,
            do_xcom_push=False,
            on_finish_action="delete_pod",
            delete_on_termination=True
        )
        # gold_load_yellow_fact = SparkKubernetesOperator(
        #     task_id='silver_fhvhv_transform',
        #     namespace='spark',
        #     application_file='/kubernetes/gold_load_yellow_fact.yaml',
        #     kubernetes_conn_id='kubernetes_default',
        #     on_failure_callback=alert_slack_channel,
        #     image_pull_policy='Always',
        #     do_xcom_push=True,
        #     is_delete_operator_pod=True,
        #     delete_on_termination=True
        # )
        gold_load_fhvhv_fact
        # gold_load_yellow_fact


    silver_transform() >> Label("No failed") >> gold_load()
