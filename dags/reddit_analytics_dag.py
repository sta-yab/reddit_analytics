import os
from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

from reddit_analytics.utilities import const

# Arctic Shift lags ~36h, so the run for day D processes data of day D - 2.
SOURCE_LAG_DAYS = 2


def _target_date(ds: str) -> str:
    """Single source of truth: extract and transform must use the same date."""
    return (date.fromisoformat(ds) - timedelta(days=SOURCE_LAG_DAYS)).isoformat()

def _use_reddit_credentials():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GOOGLE_APPLICATION_CREDENTIALS_REDDIT"]


@dag(
    dag_id="reddit_analytics",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=2,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["reddit", "ingest", "transform"],
)
def reddit_analytics():

    # Step 1: API -> raw JSON (one mapped task per subreddit)
    @task(max_active_tis_per_dag=1)  # one request stream at a time, API rate limit
    def extract(subreddit: str, ds=None) -> str:
        from reddit_analytics.extract.get_reddit_data import run_extract
        return run_extract(subreddit=subreddit, date=_target_date(ds))

    # Step 1b: local raw JSON -> GCS
    @task(max_active_tis_per_dag=1)
    def upload(ds=None) -> list[str]:
        from reddit_analytics.load.upload_to_gcs import upload_raw_files
        return upload_raw_files(_target_date(ds))

    # Step 2: raw JSON in GCS -> PySpark -> BigQuery (curated)
    # one Spark job at a time across runs: it is memory-heavy
    @task(max_active_tis_per_dag=1, execution_timeout=timedelta(minutes=30))
    def transform(ds=None) -> dict:
        _use_reddit_credentials()
        # lazy import: keeps pyspark out of the DAG parser
        from reddit_analytics.transform.clean_posts import run_transform
        return run_transform(_target_date(ds))

    # transform starts only after every extract task succeeded
    extract.expand(subreddit=const.TOPICS) >> upload() >> transform()


reddit_analytics()
