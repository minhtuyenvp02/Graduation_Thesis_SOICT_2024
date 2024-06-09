import sys
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.decorators import task_group
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
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
start_date = datetime(2024, 6, 9)
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
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": start_date,
}
with DAG(
        default_args=default_args,
        dag_id="data_quality_pipeline",
        schedule_interval='0 0 * * *',
        tags=["slowly change dimension update"],
        catchup=False,
        on_failure_callback=alert_slack_channel,
) as dag:
    @task_group(default_args={'retries': 2})
    def gold_load():
        gold_scd1_update = SparkKubernetesOperator(
            task_id='gold_scd1_update',
            namespace='spark',
            application_file='/kubernetes/gold_update_scd1.yaml',
            kubernetes_conn_id='kubernetes_default',
            on_failure_callback=alert_slack_channel,
            image_pull_policy='Always',
            do_xcom_push=True,
            is_delete_operator_pod=True,
            delete_on_termination=True
        )
        gold_scd2_update = SparkKubernetesOperator(
            task_id='gold_scd2_update',
            namespace='spark',
            application_file='/kubernetes/gold_update_scd2.yaml',
            kubernetes_conn_id='kubernetes_default',
            on_failure_callback=alert_slack_channel,
            image_pull_policy='Always',
            do_xcom_push=True,
            is_delete_operator_pod=True,
            delete_on_termination=True
        )
        gold_scd2_update
        gold_scd1_update
    gold_load()

