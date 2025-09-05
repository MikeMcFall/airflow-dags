from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'bills',
    'retries': 0
}

with DAG(
    dag_id='process_boa_unlimited',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    env = Variable.get("airflow_env", default_var="dev")
    script = "/opt/github/process-pdf-bills/scripts/shell/load_pdf_to_hdfs.sh "
    year = "{{ logical_date.strftime('%Y') }} "
    month = "{{ logical_date.strftime('%m') }} "

    load = BashOperator(
        task_id='load_pdf_to_HDFS',
        bash_command="bash " + script + year + month + "boa_unlimited" + env
    )

    load