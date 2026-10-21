import gzip
import json
import logging
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"

# File logger for local error/debug tracking (separate from Prefect's UI logs).
# Explicit handler because Prefect configures root logging, making basicConfig a no-op.
log = logging.getLogger("gharchive")
log.setLevel(logging.INFO)
_fh = logging.FileHandler("gharchive_flow.log")
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(_fh)


@task(retries=3, retry_delay_seconds=5)
def count_events_for_hour(hour):
    """Stream one hourly GH Archive file and return {event_type: count}."""
    counts = Counter()
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
            r.raise_for_status()  # turn a 404/500 into an exception so Prefect retries
            # Decompress while streaming, so the full file never sits in memory
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for line in gz:  # each line is one JSON event
                    try:
                        counts[json.loads(line)["type"]] += 1
                    except (json.JSONDecodeError, KeyError):
                        log.warning(f"hour {hour}: skipped malformed line")
    except requests.RequestException as e:
        log.error(f"hour {hour}: download failed: {e}")
        raise  # re-raise so Prefect counts it as a failure and retries
    print(f"hour {hour}: {counts.total()} events")
    log.info(f"hour {hour}: {dict(counts)}")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    pass


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
