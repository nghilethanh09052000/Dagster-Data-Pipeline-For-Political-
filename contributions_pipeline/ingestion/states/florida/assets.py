import csv
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import dagster as dg
import requests
from dagster import AssetExecutionContext, get_dagster_logger
from dagster_dbt import build_dbt_asset_selection
from psycopg import sql

from contributions_pipeline.lib.file import safe_readline_csv_like_file
from contributions_pipeline.lib.ingest import (
    get_sql_count_query,
    insert_parsed_file_to_landing_table,
)
from contributions_pipeline.resources import PostgresResource
from contributions_pipeline.transformation.assets import dbt_transformations

# Base directory for Florida contributions data
FL_BASE_PATH = "./states/florida/contribs"

FL_BASE_URL = "https://dos.elections.myflorida.com/campaign-finance/contributions/"
FL_CONTRIB_API_URL = "https://dos.elections.myflorida.com/cgi-bin/contrib.exe"

# Headers and user agent based on successful curl request
FL_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)
FL_HEADERS = {
    "accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "accept-language": "en-US,en;q=0.9,es;q=0.8",
    "cache-control": "max-age=0",
    "content-type": "application/x-www-form-urlencoded",
    "dnt": "1",
    "origin": "https://dos.elections.myflorida.com",
    "referer": "https://dos.elections.myflorida.com/campaign-finance/contributions/",
    "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": FL_USER_AGENT,
}

FL_FIRST_AVAILABLE_YEAR = datetime(1995, 1, 1)

FL_START_YEAR = 1995
FL_CURRENT_YEAR = datetime.now().year

fl_monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date=FL_FIRST_AVAILABLE_YEAR,
    fmt="%Y-%m",
    timezone="America/New_York",
    end_offset=1,
)


def _fl_get_cookie():
    """
    Retrieve session cookies required for Florida contributions POST requests.
    """
    logger = get_dagster_logger(name="_fl_get_cookie")
    logger.info(f"Fetching FL cookies from {FL_BASE_URL}")

    res = requests.get(FL_BASE_URL, headers=FL_HEADERS)
    res.raise_for_status()

    logger.info(f"Retrieved FL cookies: {len(res.cookies.get_dict())} items")

    return res.cookies.get_dict()


@dg.asset(partitions_def=fl_monthly_partitions, pool="fl_api")
def fl_fetch_contributions(context: dg.AssetExecutionContext):
    """
    Fetch contributions data from florida elections website
    """

    cookies = _fl_get_cookie()

    base_path = Path(FL_BASE_PATH)
    base_path.mkdir(parents=True, exist_ok=True)

    time_window = context.partition_time_window
    real_end_date = time_window.end - timedelta(days=1)

    from_mdy = time_window.start.strftime("%m/%d/%Y")
    thru_mdy = real_end_date.strftime("%m/%d/%Y")

    from_ymd = time_window.start.strftime("%Y%m%d")
    thru_ymd = real_end_date.strftime("%Y%m%d")

    file_path = base_path / f"flc_{from_ymd}-{thru_ymd}.tsv"

    context.log.info(f"Posting FL contributions request for {from_mdy} to {thru_mdy}")

    try:
        # Use data-raw format that matched the successful curl request
        payload = (
            f"election=All&search_on=1&CanFName=&CanLName=&CanNameSrch=2&"
            f"office=All&cdistrict=&cgroup=&party=All&ComName=&ComNameSrch=2&"
            f"committee=All&cfname=&clname=&namesearch=2&ccity=&cstate=&czipcode=&"
            f"coccupation=&cdollar_minimum=&cdollar_maximum=&"
            f"csort1=DAT&csort2=CAN&cdatefrom={from_mdy}&cdateto={thru_mdy}&"
            f"queryformat=2&Submit=Submit"
        )

        context.log.info(f"Using payload: {payload}")

        res = requests.post(
            FL_CONTRIB_API_URL,
            headers=FL_HEADERS,
            cookies=cookies,
            data=payload,
            timeout=600,
        )
        if not res.ok:
            context.log.warning(
                f"Failed to get data from {from_mdy} to {thru_mdy},"
                f" status code: {res.status_code}, body: {res.text}"
            )
            raise RuntimeError(
                f"Failed to get data from {from_mdy} to {thru_mdy},"
                f" status code: {res.status_code}, body: {res.text}"
            )

        # Check if we received TSV data (should start with header line)
        content = res.content.decode("utf-8", errors="replace")
        if content and not content.startswith("<!DOCTYPE html"):
            context.log.info(f"Fetched data, writing to {file_path}")
            file_path.write_bytes(res.content)
        else:
            context.log.error(
                f"Received HTML instead of TSV data for {from_mdy} to {thru_mdy}"
            )
            context.log.debug(f"Response preview: {content[:500]}...")

        # Small delay after successful fetch
        time.sleep(random.uniform(1.0, 2.0))

    except Exception as e:
        context.log.error(f"Error fetching data for {from_mdy} to {thru_mdy}: {e}")
        raise e


@dg.asset(
    deps=[fl_fetch_contributions],
    partitions_def=fl_monthly_partitions,
)
def fl_insert_contributions(
    context: AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    """
    Insert Florida contributions to the right landing table.
    """

    time_window = context.partition_time_window
    real_end_date = time_window.end - timedelta(days=1)

    context.log.info(
        f"Starting to insert Florida contributions {time_window.start}-{real_end_date}"
    )

    from_ymd = time_window.start.strftime("%Y%m%d")
    thru_ymd = real_end_date.strftime("%Y%m%d")

    base_path = Path(FL_BASE_PATH)
    file_path = base_path / f"flc_{from_ymd}-{thru_ymd}.tsv"

    with pg.pool.connection() as conn, conn.cursor() as cur:
        truncate_query = sql.SQL(
            "DELETE FROM fl_contributions_landing WHERE"
            ' "Date" >= {start_date}'
            ' AND "Date" <= {end_date}'
        ).format(
            start_date=time_window.start.strftime("%m/%d/%Y"),
            end_date=real_end_date.strftime("%m/%d/%Y"),
        )

        context.log.info(f"Truncating data using `{truncate_query}`")
        cur.execute(truncate_query)

        context.log.info(f"Inserting file {file_path}")
        lines = safe_readline_csv_like_file(str(file_path.absolute()))
        reader = csv.reader(lines, delimiter="\t", quotechar=None)

        # Skip header
        next(reader, None)

        insert_parsed_file_to_landing_table(
            pg_cursor=cur,
            csv_reader=reader,
            table_name="fl_contributions_landing",
            table_columns_name=[
                "Candidate/Committee",
                "Date",
                "Amount",
                "Type",
                "Contributor_Name",
                "Address",
                "City_State_Zip",
                "Occupation",
                "InKind_Desc",
            ],
            row_validation_callback=lambda row: len(row) == 9,
        )

        # Count rows
        count_result = cur.execute(
            get_sql_count_query(table_name="fl_contributions_landing")
        ).fetchone()
        count = count_result[0] if count_result is not None else 0

    context.log.info(f"Completed insertion for {time_window}; rows: {count}")
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": "fl_contributions_landing",
            "dagster/row_count": count,
        },
    )


# Export all assets
fl_assets = [
    fl_fetch_contributions,
    fl_insert_contributions,
]

fl_dbt_selection = build_dbt_asset_selection(
    [dbt_transformations], dbt_select="tag:florida"
)
fl_asset_selection = dg.AssetSelection.assets(*fl_assets)

# Combine both selection
fl_refresh_assets_selection = fl_asset_selection | fl_dbt_selection
