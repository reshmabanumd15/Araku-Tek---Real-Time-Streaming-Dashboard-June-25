from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "rt-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="rt_streaming_dashboard",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # trigger manually or via CI/CD
    catchup=False,
    default_args=default_args,
    tags=["kafka","spark","redshift"],
) as dag:

    create_topics = BashOperator(
        task_id="create_topics",
        bash_command="python /opt/airflow/dags/src/kafka/create_topics.py --bootstrap kafka:9092 --config /opt/airflow/dags/configs/topics.json"
    )

    spark_job = BashOperator(
        task_id="spark_streaming_job",
        bash_command="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/airflow/dags/src/spark/streaming_job.py"
    )

    health_check = BashOperator(
        task_id="health_check",
        bash_command="echo 'TODO: add health probes (lag, throughput, errors)'"
    )

    create_topics >> spark_job >> health_check
