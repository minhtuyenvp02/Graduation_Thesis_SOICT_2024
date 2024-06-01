import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable, Connection
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

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
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"


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
        webhook_token=SLACK_WEBHOOK_URL,
        message=msg,
    ).execute()


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
        on_failure_callback=alert_slack_channel,
        catchup=False,
) as dag:
    gold_fact_yellow_tracking = BashOperator(
        task_id="gold_update_yellow_tracking_daily",
        bash_command=f'''
                        spark-submit /opt/airflow/scripts/spark/gold_fact_yellow_tracking.py \
                            --spark_cluster {SPARK_CLUSTER} \
                            --bucket_name {S3_BUCKET_NAME} \
                            --s3_endpoint {S3_ENDPOINT} \
                            --s3_access_key {S3_ACCESS_KEY} \
                            --s3_secret_key {S3_SECRET_KEY} \
                        ''',
        on_failure_callback=alert_slack_channel,
    )
    gold_fact_fhvhv_tracking = BashOperator(
        task_id="gold_update_fhvhv_tracking_daily",
        bash_command=f'''
                           spark-submit /opt/airflow/scripts/spark/gold_fact_fhvhv_tracking.py \
                               --spark_cluster {SPARK_CLUSTER} \
                               --bucket_name {S3_BUCKET_NAME} \
                               --s3_endpoint {S3_ENDPOINT} \
                               --s3_access_key {S3_ACCESS_KEY} \
                               --s3_secret_key {S3_SECRET_KEY} \
                           ''',
        on_failure_callback=alert_slack_channel,
    )
    gold_fact_fhvhv_tracking
    gold_fact_yellow_tracking
