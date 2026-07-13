"""
lakehouse_gold_refresh
-----------------------
Orchestrates the batch side of the lakehouse. The streaming jobs
(bronze ingest, silver transform) run continuously as long-lived Spark
Structured Streaming apps outside of Airflow. This DAG owns everything
that's naturally batch-shaped:

    1. run Gold aggregation Spark job (Silver -> Gold)
    2. run the data-quality gate against Silver before Gold is trusted
    3. refresh the Snowflake external tables over Gold
    4. run dbt (staging -> marts)
    5. run dbt tests
    6. Slack-alert on any failure

Schedule: every 30 minutes. Retries are conservative (2x, 5 min apart)
since Spark job failures are usually resource/transient, not logic bugs
by the time this reaches prod.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=25),
}

SPARK_SUBMIT = (
    "spark-submit --master spark://spark-master:7077 "
    "--packages io.delta:delta-spark_2.12:3.1.0 "
)


def _run_dq_gate(**context):
    """Loads Silver via Spark and asserts the DQ suite passes before Gold runs."""
    from pyspark.sql import SparkSession
    import sys
    sys.path.append("/opt/airflow/great_expectations")
    from dq_checks import run_silver_orders_suite, assert_suite_passes

    spark = (
        SparkSession.builder.appName("dq-gate")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    df = spark.read.format("delta").load("s3a://lakehouse/silver/orders")
    results = run_silver_orders_suite(df)
    assert_suite_passes(results)
    spark.stop()


with DAG(
    dag_id="lakehouse_gold_refresh",
    description="Batch refresh of Gold aggregates + Snowflake serving layer",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "spark", "dbt", "snowflake"],
    on_failure_callback=send_slack_notification(
        slack_conn_id="slack_default",
        text="🔴 lakehouse_gold_refresh failed: {{ ti.task_id }} in {{ dag.dag_id }}",
        channel="#data-eng-alerts",
    ),
) as dag:

    dq_gate = PythonOperator(
        task_id="data_quality_gate_silver",
        python_callable=_run_dq_gate,
    )

    gold_aggregate = BashOperator(
        task_id="spark_gold_aggregate",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/spark/jobs/03_gold_aggregate.py",
    )

    refresh_external_tables = BashOperator(
        task_id="refresh_gold_external_tables",
        bash_command=(
            "snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER "
            "-q \"ALTER EXTERNAL TABLE raw_gold.eta_model_features REFRESH; "
            "ALTER EXTERNAL TABLE raw_gold.restaurant_ops_metrics REFRESH;\""
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
    )

    dq_gate >> gold_aggregate >> refresh_external_tables >> dbt_run >> dbt_test
