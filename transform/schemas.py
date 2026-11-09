from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)

POST_SCHEMA = StructType([
    StructField("author", StringType(), True),
    StructField("created_utc", StringType(), True),
    StructField("id", StringType(), True),
    StructField("link_flair_text", StringType(), True),
    StructField("num_comments", LongType(), True),
    StructField("permalink", StringType(), True),
    StructField("score", LongType(), True),
    StructField("selftext", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("title", StringType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("url", StringType(), True),
])