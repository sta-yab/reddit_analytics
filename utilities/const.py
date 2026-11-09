import os


TOPICS = ['dataengineering',
          'python',
          'pandas',
          'cloud',
          'googlecloud',
          'bigquery',
          'spark',
          'PySpark']

HEADERS = {"User-Agent": os.environ["REDDIT_USER_AGENT"]}
URL = "https://arctic-shift.photon-reddit.com/api/posts/search"

GCS_RAW_BUCKET = os.environ["GCS_RAW_BUCKET"]
GCS_TMP_BUCKET = os.environ["GCS_TMP_BUCKET"]
BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET_CURATED = "curated"
COMMENTS_P75 = 2
COMMENTS_P90 = 9
