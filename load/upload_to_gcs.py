# reddit_analytics/load/upload_to_gcs.py
import logging
import os
from pathlib import Path

from google.cloud import storage

from reddit_analytics.utilities import const

log = logging.getLogger(__name__)

# <project>/data, regardless of where the script is launched from
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _client() -> storage.Client:
    """Explicit reddit key if set (Airflow); otherwise default credentials (local)."""
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_REDDIT")
    if key_path:
        return storage.Client.from_service_account_json(key_path)
    return storage.Client()


def upload_raw_files(run_date: str) -> list[str]:
    """Upload one day of raw JSON to GCS, keeping the same relative path:

    <data root>/raw/reddit/YYYY/MM/DD/{subreddit}.json
        -> gs://<bucket>/raw/reddit/YYYY/MM/DD/{subreddit}.json

    Returns the uploaded blob paths (small, safe for XCom).
    """
    data_root = Path(os.environ.get("REDDIT_DATA_DIR", DEFAULT_DATA_DIR))
    y, m, d = run_date.split("-")
    day_dir = data_root / "raw" / "reddit" / y / m / d

    files = sorted(day_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No raw files for {run_date} in {day_dir}")

    bucket = _client().bucket(const.GCS_RAW_BUCKET)
    uploaded = []
    for local_file in files:
        blob_path = local_file.relative_to(data_root).as_posix()
        bucket.blob(blob_path).upload_from_filename(str(local_file))
        log.info("Uploaded %s -> gs://%s/%s", local_file, const.GCS_RAW_BUCKET, blob_path)
        uploaded.append(blob_path)
    return uploaded


if __name__ == "__main__":
    # Local run (from the folder that contains reddit_analytics/):
    #   python -m reddit_analytics.load.upload_to_gcs 2026-10-11
    import sys

    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    print(upload_raw_files(sys.argv[1]))
    