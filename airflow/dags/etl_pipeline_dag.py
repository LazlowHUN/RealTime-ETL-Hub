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

    # -------------------------------
    # STAGING MODELS
    # -------------------------------

    @task
    def run_dbt_staging():
        """
        Runs dbt models tagged as `staging` to build the staging layer
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
    
    @task
    def test_dbt_staging():
        """
        Executes dbt tests for staging models (data quality checks).
        """
        cmd = [
            "dbt",
            "test",
            "--project-dir",
            "/dbt_project",
            "--profiles-dir",
            "/home/airflow/.dbt",
            "--select",
            "tag:staging",
        ]
        subprocess.run(cmd, check=True)

    # -------------------------------
    # MARTS MODELS
    # -------------------------------

    @task
    def run_dbt_marts():
        """
        Runs all dbt models tagged as `marts` to build the data mart layer
        """
        cmd = [
            "dbt",
            "run",
            "--project-dir",
            "/dbt_project",
            "--profiles-dir",
            "/home/airflow/.dbt",
            "--select",
            "tag:marts",
        ]
        subprocess.run(cmd, check=True)

    @task
    def test_dbt_marts():
        """
        Executes dbt tests for marts models (business logic quality checks).
        """
        cmd = [
            "dbt",
            "test",
            "--project-dir",
            "/dbt_project",
            "--profiles-dir",
            "/home/airflow/.dbt",
            "--select",
            "tag:marts",
        ]
        subprocess.run(cmd, check=True)

    # ---------------------------------------
    # PIPELINE DEPENDENCY STRUCTURE (DAG)
    # ---------------------------------------

    s_run = run_dbt_staging()
    s_test = test_dbt_staging()
    m_run = run_dbt_marts()
    m_test = test_dbt_marts()

    s_run >> s_test >> m_run >> m_test

dbt_snowflake_pipeline()
