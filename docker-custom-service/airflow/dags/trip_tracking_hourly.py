import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
sys.path.append("/opt/airflow/scripts/spark")
from gold_fact_fhvhv_tracking import main

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
start_date = datetime(2024, 7, 2)
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
        dag_id="trip_tracking_daily",
        schedule_interval='0 */5 * * *',
        tags=["trip_tracking_daily"],
        on_failure_callback=alert_slack_channel,
        catchup=False,
) as dag:
    
    gold_fact_fhvhv_tracking_daily = SparkKubernetesOperator(
        task_id='gold_fact_fhvhv_tracking_daily',
        namespace='spark',
        application_file='/kubernetes/gold_fact_fhvhv_tracking_daily.yaml',
        kubernetes_conn_id='kubernetes_default',
        on_failure_callback=alert_slack_channel,
        image_pull_policy='Always',
        # on_finish_action="delete_pod",
        # delete_on_termination=True
    )
    # gold_fact_yellow_tracking_daily = SparkKubernetesOperator(
    #     task_id='gold_fact_yellow_tracking_daily',
    #     namespace='spark',
    #     application_file='/kubernetes/gold_fact_yellow_tracking_daily.yaml',
    #     kubernetes_conn_id='kubernetes_default',
    #     on_failure_callback=alert_slack_channel,
    #     image_pull_policy='Always',
    #     is_delete_operator_pod=True,
    #     delete_on_termination=True
    # )

    # gold_fact_yellow_tracking_daily
    gold_fact_fhvhv_tracking_daily