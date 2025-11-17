import datetime
import subprocess
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

@dag(
    dag_id="etl_dbt_pipeline",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "snowflake", "etl"],
)
def dbt_snowflake_pipeline():

    @task
    def run_dbt_staging():
        """
        Staging réteg feltöltése dbt-vel (STAGING.EVENTS_FLAT).
        """
        cmd = [
            "dbt",
            "run",
            "--project-dir",
            "/dbt_project",
            "--profiles-dir",
            "/home/airflow/.dbt",
            "--select",
            "tag:staging",
        ]
        subprocess.run(cmd, check=True)

    run_dbt_staging()

dbt_snowflake_pipeline()
