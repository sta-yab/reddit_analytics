"""Reddit (Arctic Shift) extract: one subreddit + one day -> one JSON array file.

Layout: <BASE_DIR>/raw/reddit/YYYY/MM/DD/<subreddit>.json

Run locally (from the directory that contains the `reddit_analytics` package):
    python -m reddit_analytics.extract.get_reddit_data --start 2026-08-01 --end 2026-08-31
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from reddit_analytics.utilities import const
from reddit_analytics.utilities.logger import setup_logger

log = setup_logger("reddit_pipeline")

# Airflow: REDDIT_DATA_DIR is set in docker-compose.
# Local: falls back to <reddit_analytics>/data, independent of the current working directory.
BASE_DIR = Path(
    os.getenv("REDDIT_DATA_DIR", Path(__file__).resolve().parents[1] / "data")
)

PAGE_SIZE = 100
REQUEST_TIMEOUT = 30          # seconds
MAX_RETRIES = 5
PAUSE_BETWEEN_PAGES = 1       # seconds, be polite to the API
SELFTEXT_MAX_CHARS = None     # raw layer keeps the full text; truncate later in PySpark


# --------------------------------------------------------------------------- #
# API
# --------------------------------------------------------------------------- #
def fetch_posts(
    subreddit: str,
    limit: int = PAGE_SIZE,
    after: str | None = None,
    before: str | None = None,
    sort: str = "desc",
    title: str | None = None,
    query: str | None = None,
    author: str | None = None,
) -> list[dict]:
    params = {"subreddit": subreddit, "limit": limit, "sort": sort}
    optional = {"after": after, "before": before, "title": title,
                "query": query, "author": author}
    params.update({k: v for k, v in optional.items() if v})

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                const.URL,
                headers=const.HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            # Retry on rate limit and server errors, fail fast on other 4xx
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(
                    f"HTTP {response.status_code}", response=response
                )
            response.raise_for_status()
            return response.json()["data"]
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            log.warning(f"Request failed ({e}); retry {attempt}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
    return []  # unreachable, keeps type checkers happy


def fetch_all(
    subreddit: str,
    date: str,
    sort: str = "desc",
    title: str | None = None,
    query: str | None = None,
    author: str | None = None,
) -> list[dict]:
    """All posts of `subreddit` created on `date` (UTC, YYYY-MM-DD)."""
    day_start = date
    day_end = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    posts_by_id: dict[str, dict] = {}
    before = day_end
    page = 1

    while True:
        log.info(f"{subreddit} {date} | page {page} | before={before}")
        batch = fetch_posts(
            subreddit=subreddit,
            limit=PAGE_SIZE,
            after=day_start,
            before=before,
            sort=sort,
            title=title,
            query=query,
            author=author,
        )
        if not batch:
            break

        new = 0
        for post in batch:
            if post["id"] not in posts_by_id:
                posts_by_id[post["id"]] = post
                new += 1
        log.info(f"{subreddit} {date} | got {len(batch)}, new {new}, total {len(posts_by_id)}")

        # Last page, or a page with nothing new (protects from an infinite loop)
        if len(batch) < PAGE_SIZE or new == 0:
            break

        # +1s so posts sharing the oldest second are not skipped; duplicates are removed by id
        oldest_ts = min(p["created_utc"] for p in batch)
        before = (
            datetime.fromtimestamp(oldest_ts + 1, tz=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S")
        )
        time.sleep(PAUSE_BETWEEN_PAGES)
        page += 1

    return list(posts_by_id.values())


# --------------------------------------------------------------------------- #
# Transform + save
# --------------------------------------------------------------------------- #
def extract_fields(post: dict) -> dict:
    selftext = post.get("selftext")
    if selftext and SELFTEXT_MAX_CHARS:
        selftext = selftext[:SELFTEXT_MAX_CHARS]

    created = datetime.fromtimestamp(post["created_utc"], tz=timezone.utc)
    return {
        "id": post["id"],
        "title": post.get("title"),
        "author": post.get("author"),
        "subreddit": post.get("subreddit"),
        "score": post.get("score"),
        "upvote_ratio": post.get("upvote_ratio"),
        "num_comments": post.get("num_comments"),
        "created_utc": created.strftime("%Y-%m-%d %H:%M:%S"),
        "url": post.get("url"),
        "permalink": post.get("permalink"),
        "selftext": selftext or None,
        "link_flair_text": post.get("link_flair_text"),
    }


def build_output_path(subreddit: str, date: str) -> Path:
    d = datetime.strptime(date, "%Y-%m-%d")
    return BASE_DIR / "raw" / "reddit" / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{subreddit}.json"


def save_to_file(posts: list[dict], subreddit: str, date: str) -> Path:
    """Write one JSON array per file (Spark reads it with multiLine).

    Written to a temp file and renamed, so a failed task never leaves a half-written file
    and reruns simply overwrite the partition (idempotent). An empty day becomes `[]`.
    """
    output_path = build_output_path(subreddit, date)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = output_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False)
    tmp_path.replace(output_path)
    return output_path


# --------------------------------------------------------------------------- #
# Entry points
# --------------------------------------------------------------------------- #
def run_extract(subreddit: str, date: str) -> str:
    """One subreddit + one day. This is what an Airflow task calls.

    Errors are NOT swallowed: Airflow must see the failure to retry.
    """
    posts = fetch_all(subreddit=subreddit, date=date)
    clean = [extract_fields(p) for p in posts]
    path = save_to_file(clean, subreddit, date)
    log.info(f"{subreddit} {date}: {len(clean)} posts -> {path}")
    return str(path)


def date_generator(start_date: str, end_date: str) -> list[str]:
    """Date strings from start_date to end_date, both included."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end - start).days + 1)]


def generate_data(dates: list[str], topics: list[str]) -> None:
    """Local backfill. Continues after a failure, but reports all failures at the end."""
    failed: list[tuple[str, str]] = []
    for date in dates:
        for subreddit in topics:
            try:
                run_extract(subreddit, date)
            except Exception as e:
                log.error(f"Failed {subreddit} for {date}: {e}")
                failed.append((subreddit, date))

    if failed:
        raise RuntimeError(f"{len(failed)} extract(s) failed: {failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Reddit posts locally")
    parser.add_argument("--start", default="2026-08-01", help="YYYY-MM-DD")
    parser.add_argument("--end", default="2026-08-31", help="YYYY-MM-DD")
    args = parser.parse_args()

    generate_data(dates=date_generator(args.start, args.end), topics=const.TOPICS)