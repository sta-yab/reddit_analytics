# reddit_analytics/transform/session.py
import os
from pathlib import Path

from pyspark.sql import SparkSession

JARS_DIR = Path(__file__).resolve().parent.parent / "jars"


def get_spark(app_name: str = "reddit") -> SparkSession:
    jars = ",".join(str(p) for p in sorted(JARS_DIR.glob("*.jar")))
    return (
        SparkSession.builder
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app_name)
        .config("spark.jars", jars)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # GCS (як було)
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
        .getOrCreate()
    )