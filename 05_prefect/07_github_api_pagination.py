"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import os
from pathlib import Path
import duckdb
import httpx
from prefect import flow, task


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10

# duckdb file lives next to this script, no matter where it's run from
DB_FILE = Path(__file__).parent / "actions.duckdb"
TABLE = "commits"


# turn one nested commit dict from the API into a flat dict (one table row)
def flatten(c):
    return {
        "sha": c["sha"],
        "author_name": c["commit"]["author"]["name"],
        "author_login": (c.get("author") or {}).get("login"),   # None if no linked GitHub account
        "committed_at": c["commit"]["author"]["date"],          # ISO string; DuckDB casts it to TIMESTAMPTZ on insert
        "message": c["commit"]["message"].splitlines()[0],      # first line only
        "url": c["html_url"],
    }


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # fetch PAGES pages of commits by following GitHub's "next" links, then load them into duckdb.
    # pagination docs: https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api
    # review runs at https://sds-prefect.pods.uvarc.io/v2/runs

    # follow the "next" links page by page, collecting every commit in one list
    all_commits = []
    url = URL
    params = {"per_page": PER_PAGE}   # only needed on the first request
    page = 1
    while url and page <= PAGES:
        response = httpx.get(url, headers=HEADERS, params=params)
        response.raise_for_status()   # raise on 4xx/5xx (e.g. bad token, rate limit)
        commits = response.json()     # JSON text -> list of commit dicts
        all_commits.extend(commits)
        print(f"page {page}: {len(commits)} commits ({len(all_commits)} total)")

        url = response.links.get("next", {}).get("url")   # None on the last page
        params = None   # the next URL already has per_page and page in it
        page += 1

    print(f"fetched {len(all_commits)} commits")

    # flatten every commit into a row dict, ready for DuckDB
    rows = [flatten(c) for c in all_commits]

    # rebuild the table on every run (no duplicates on reruns/retries), then insert all rows.
    # the with-block closes the connection automatically, even on errors
    with duckdb.connect(DB_FILE) as con:
        con.execute(f"""
            CREATE OR REPLACE TABLE {TABLE} (
                sha VARCHAR PRIMARY KEY, author_name VARCHAR, author_login VARCHAR,
                committed_at TIMESTAMPTZ, message VARCHAR, url VARCHAR
            )
        """)
        # $name placeholders are filled from each row dict's matching key
        con.executemany(
            f"INSERT INTO {TABLE} VALUES ($sha, $author_name, $author_login, $committed_at, $message, $url)",
            rows,
        )
        count = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
        print(f"{count} rows in {DB_FILE.name}:{TABLE}")
        # fail the task (and trigger a retry) if we didn't land exactly 10 x 100 rows
        if count != PAGES * PER_PAGE:
            raise ValueError(f"expected {PAGES * PER_PAGE} rows, got {count}")


# name the flow something you can identify as yourself
@flow(name="wsutton-flow")
def main_flow():
    ingest_data()


# @flow only *defines* main_flow; this block actually calls it when the
# file is run directly (python 07_github_api_pagination.py), but not on import
if __name__ == "__main__":
    main_flow()
