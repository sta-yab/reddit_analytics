"""Transform step: GCS raw JSON (one day) -> clean posts + marts -> BigQuery.

Designed to be called from an Airflow PythonOperator:
    run_transform("2026-10-11")

Rules that keep it Airflow-friendly:
  * no code at import time (no SparkSession, no reads) - the DAG parser imports safely
  * every step is a function: DataFrame in -> DataFrame out
  * the date is an argument, never a wildcard over the whole bucket
  * writes are idempotent: re-running a date overwrites only that date's partition
"""
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from reddit_analytics.transform.schemas import POST_SCHEMA
from reddit_analytics.transform.session import get_spark
from reddit_analytics.utilities import const

log = logging.getLogger(__name__)

# Authors that would dominate author metrics without being real users
EXCLUDED_AUTHORS = ["[deleted]", "AutoModerator"]


# --------------------------------------------------------------------------
# Read
# --------------------------------------------------------------------------
def read_raw(
    spark: SparkSession, run_date: str, subreddit: str | None = None
) -> DataFrame:
    y, m, d = run_date.split("-")
    name = subreddit or "*"
    path = f"gs://{const.GCS_RAW_BUCKET}/raw/reddit/{y}/{m}/{d}/{name}.json"
    log.info("Reading %s", path)
    return (
        spark.read.schema(POST_SCHEMA)
        .option("multiLine", True)
        .json(path)
        .withColumn("_source_path", F.col("_metadata.file_path"))
    )


# --------------------------------------------------------------------------
# Clean
# --------------------------------------------------------------------------
def clean(df: DataFrame, run_date: str) -> DataFrame:
    """Trim strings, derive subreddit/created_at/post_date, drop unusable rows."""
    source_subreddit = F.regexp_extract(F.col("_source_path"), r"/([^/]+)\.json$", 1)

    # created_utc: either 'yyyy-MM-dd HH:mm' string or Unix seconds
    created_text = F.trim(F.col("created_utc"))
    created_number = F.expr("try_cast(created_utc AS DOUBLE)")
    created_at = F.when(
    created_text.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$"),
    F.expr("try_to_timestamp(trim(created_utc))"),
).otherwise(F.to_timestamp(F.from_unixtime(created_number)))

    return (
        df.withColumn("load_date", F.to_date(F.lit(run_date)))
        .withColumn("id", F.trim(F.col("id")))
        .withColumn("author", F.trim(F.col("author")))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn(
            "subreddit",
            F.coalesce(
                F.when(source_subreddit != "", source_subreddit),
                F.col("subreddit"),
            ),
        )
        .withColumn("created_at", created_at)
        .withColumn("post_date", F.to_date("created_at"))
        .filter(F.col("id").isNotNull() & (F.col("id") != ""))
        .filter(F.col("created_at").isNotNull())
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per post id. Deterministic: highest score, then source path."""
    w = Window.partitionBy("id").orderBy(
        F.col("score").desc_nulls_last(), F.col("_source_path")
    )
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


# --------------------------------------------------------------------------
# Marts (calculations from the notebook)
# --------------------------------------------------------------------------
def build_subreddit_daily(df: DataFrame) -> DataFrame:
    """subreddit | post_date | total_posts | avg_score | avg_upvote_ratio
    | avg_comments | top_post_id"""
    agg = df.groupBy("subreddit", "post_date").agg(
        F.count("*").alias("total_posts"),
        F.avg("score").alias("avg_score"),
        F.avg("upvote_ratio").alias("avg_upvote_ratio"),
        F.avg("num_comments").alias("avg_comments"),
    )
    w = Window.partitionBy("subreddit", "post_date").orderBy(
        F.col("score").desc_nulls_last(), F.col("id")
    )
    top = (
        df.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select("subreddit", "post_date", F.col("id").alias("top_post_id"))
    )
    return agg.join(top, on=["subreddit", "post_date"], how="left")


def build_post_popularity(df: DataFrame) -> DataFrame:
    """id | title | subreddit | post_date | score | num_comments | upvote_ratio
    | comments_popularity | score_rank_in_subreddit"""
    w = Window.partitionBy("subreddit", "post_date").orderBy(
        F.col("score").desc_nulls_last()
    )
    popularity = (
        F.when(F.col("num_comments") == 0, "no_comments")
        .when(F.col("num_comments") <= const.COMMENTS_P75, "low")
        .when(F.col("num_comments") <= const.COMMENTS_P90, "medium")
        .when(F.col("num_comments") > const.COMMENTS_P90, "high")
        .otherwise("unknown")
    )
    return (
        df.withColumn("comments_popularity", popularity)
        .withColumn("score_rank_in_subreddit", F.rank().over(w))
        .select(
            "id", "title", "subreddit", "post_date", "score",
            "num_comments", "upvote_ratio",
            "comments_popularity", "score_rank_in_subreddit",
        )
    )


def build_author_daily(df: DataFrame) -> DataFrame:
    """author | post_date | total_posts | total_score | avg_score | avg_comments
    | subreddits_count | most_active_subreddit"""
    base = df.filter(
        F.col("author").isNotNull() & ~F.col("author").isin(EXCLUDED_AUTHORS)
    )
    stats = base.groupBy("author", "post_date").agg(
        F.count("id").alias("total_posts"),
        F.sum("score").alias("total_score"),
        F.avg("score").alias("avg_score"),
        F.avg("num_comments").alias("avg_comments"),
        F.countDistinct("subreddit").alias("subreddits_count"),
    )
    per_sub = base.groupBy("author", "post_date", "subreddit").agg(
        F.count("id").alias("posts_in_subreddit")
    )
    # row_number + tie-breaker: exactly one row per author (rank() would
    # duplicate authors on ties)
    w = Window.partitionBy("author", "post_date").orderBy(
        F.col("posts_in_subreddit").desc(), F.col("subreddit")
    )
    most_active = (
        per_sub.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select(
            "author", "post_date",
            F.col("subreddit").alias("most_active_subreddit"),
        )
    )
    return stats.join(most_active, on=["author", "post_date"], how="left")


# --------------------------------------------------------------------------
# Write
# --------------------------------------------------------------------------
def write_bq(df: DataFrame, table: str, run_date: str) -> None:
    """Idempotent: overwrites only the post_date partition == run_date."""
    target = f"{const.BQ_PROJECT}.{const.BQ_DATASET_CURATED}.{table}"
    log.info("Writing %s (partition %s)", target, run_date)
    (
        df.write.format("bigquery")
        .option("table", target)
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", const.GCS_TMP_BUCKET)
        .option("partitionField", "post_date")
        .option("partitionType", "DAY")
        .option("datePartition", run_date.replace("-", ""))
        .mode("overwrite")
        .save()
    )


# --------------------------------------------------------------------------
# Entry point (what the Airflow task calls)
# --------------------------------------------------------------------------
def run_transform(
    run_date: str, subreddit: str | None = None, dry_run: bool = False
) -> dict:
    spark = get_spark()
    try:
        raw = read_raw(spark, run_date, subreddit)
        posts = (
            deduplicate(clean(raw, run_date))
            .filter(F.col("post_date") == F.to_date(F.lit(run_date)))
            .cache()
        )

        row_count = posts.count()
        if row_count == 0:
            raise ValueError(f"No posts found for {run_date}")

        outputs = {
            "clean_posts": posts,
            "subreddit_daily": build_subreddit_daily(posts),
            "post_popularity": build_post_popularity(posts),
            "author_daily": build_author_daily(posts),
        }
        for table, frame in outputs.items():
            if dry_run:
                print(f"=== {table} ({frame.count()} rows) ===")
                frame.show(5, truncate=40)
            else:
                write_bq(frame, table, run_date)

        return {"run_date": run_date, "posts": row_count, "tables": list(outputs)}
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("run_date", help="YYYY-MM-DD")
    parser.add_argument("--subreddit", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    print(run_transform(args.run_date, args.subreddit, args.dry_run))
    