# Reddit Analytics Pipeline

Daily batch pipeline: Reddit posts (Arctic Shift API) -> GCS raw layer -> PySpark -> BigQuery, orchestrated by Airflow.

## Architecture

```
Arctic Shift API
      |  extract (one Airflow mapped task per subreddit)
      v
local raw JSON
      |  upload
      v
GCS raw layer: raw/reddit/YYYY/MM/DD/{subreddit}.json
      |  transform (PySpark, local mode)
      v
BigQuery dataset `curated`
  - clean_posts
  - subreddit_daily
  - post_popularity
  - author_daily
```

Airflow DAG `reddit_analytics`: `extract.expand(subreddit) >> upload >> transform`.

## Design notes

- **Idempotent writes:** the run date is an argument (never a wildcard over the bucket); re-running a date overwrites only that `post_date` partition.
- **Source lag:** Arctic Shift lags ~36h, so the run for day D processes day D-2 (one function, `_target_date`).
- **Airflow-friendly code:** no Spark session or reads at import time; Spark is imported lazily inside the task.
- **Rate limits:** `max_active_tis_per_dag=1` on extract/upload/transform (API limit, memory-heavy Spark job).
- **Cleaning:** schema-enforced multiLine JSON read, trimmed strings, mixed `created_utc` formats handled, deterministic deduplication by post id.

## Stack

Python, PySpark, GCS, BigQuery, Airflow 3.0.2 (CeleryExecutor, Docker Compose).

## Configuration

All config comes from environment variables, see `.env.airflow.example`:

| Variable | Purpose |
|---|---|
| `GCS_RAW_BUCKET` | bucket with raw JSON |
| `GCS_TMP_BUCKET` | temp bucket for the BigQuery connector |
| `BQ_PROJECT` | GCP project ID |
| `REDDIT_USER_AGENT` | User-Agent for API requests |
| `GOOGLE_APPLICATION_CREDENTIALS_REDDIT` | path to the service account key (kept outside the repo) |

## Jars

Not stored in git. Put these into `jars/`:

- spark-bigquery-connector
- GCS connector (Hadoop)

## Run the transform manually

```bash
python -m reddit_analytics.transform.clean_posts 2026-10-11 --dry-run
python -m reddit_analytics.transform.clean_posts 2026-10-11
```
