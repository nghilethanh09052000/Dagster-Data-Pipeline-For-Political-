import json

import dagster as dg
from dagster_slack import SlackResource
from psycopg import sql

from contributions_pipeline.resources import PostgresResource


@dg.asset(
    description=(
        "Calculate benchmark match rate by state comparing ground truth "
        "against individual_contributions"
    ),
)
def benchmark_match_rate_by_state(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Compare ground_truth_contributions_manually_gathered_sample
    against individual_contributions and calculate match rate by state.

    Returns:
        MaterializeResult with metadata containing match rate statistics by state
    """
    logger = dg.get_dagster_logger()

    query = sql.SQL(
        """
        SELECT
            gt.state,
            COUNT(DISTINCT gt.name_concat) as total_ground_truth,
            COUNT(DISTINCT CASE WHEN ic.name_concat IS NOT NULL
                THEN gt.name_concat END) as matched_count,
            ROUND(
                COUNT(DISTINCT CASE WHEN ic.name_concat IS NOT NULL
                    THEN gt.name_concat END)::numeric /
                NULLIF(COUNT(DISTINCT gt.name_concat), 0)::numeric * 100,
                2
            ) as match_rate_percent
        FROM {ground_truth_table} gt
        LEFT JOIN {individual_contributions_table} ic
            ON gt.name_concat = ic.name_concat
        GROUP BY gt.state
        """
    ).format(
        ground_truth_table=sql.Identifier(
            "ground_truth_contributions_manually_gathered_sample"
        ),
        individual_contributions_table=sql.Identifier("individual_contributions"),
    )

    logger.info("Calculating benchmark match rates by state...")

    results = []
    with (
        pg.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            results.append(
                {
                    "state": row[0],
                    "total_ground_truth": row[1],
                    "matched_count": row[2],
                    "match_rate_percent": row[3],
                }
            )

    logger.info(f"Found {len(results)} states with benchmark data")
    for result in results:
        logger.info(
            f"State: {result['state']}, "
            f"Total: {result['total_ground_truth']}, "
            f"Matched: {result['matched_count']}, "
            f"Rate: {result['match_rate_percent']}%"
        )

    return dg.MaterializeResult(
        metadata={
            "states_count": len(results),
            "total_ground_truth": sum(r["total_ground_truth"] for r in results),
            "total_matched": sum(r["matched_count"] for r in results),
            "results_json": json.dumps(results),
        }
    )


@dg.asset(
    pool="pg",
    description="Calculate benchmark match rate by state for unique people",
)
def benchmark_match_rate_by_state_unique_people(
    pg: dg.ResourceParam[PostgresResource],
) -> dg.MaterializeResult:
    """
    Calculate match rate by state for unique people (one person matching is fine).
    This is the same as benchmark_match_rate_by_state but focuses on unique people.
    """
    logger = dg.get_dagster_logger()

    query = sql.SQL(
        """
        WITH unique_ground_truth AS (
            SELECT DISTINCT
                state,
                name_concat
            FROM {ground_truth_table}
        ),
        unique_matches AS (
            SELECT DISTINCT
                ugt.state,
                ugt.name_concat,
                CASE WHEN ic.name_concat IS NOT NULL THEN 1 ELSE 0 END as matched
            FROM unique_ground_truth ugt
            LEFT JOIN {individual_contributions_table} ic
                ON ugt.name_concat = ic.name_concat
        )
        SELECT
            state,
            COUNT(*) as total_unique_people,
            SUM(matched) as matched_unique_people,
            ROUND(
                SUM(matched)::numeric / NULLIF(COUNT(*), 0)::numeric * 100,
                2
            ) as match_rate_percent
        FROM unique_matches
        GROUP BY state
        """
    ).format(
        ground_truth_table=sql.Identifier(
            "ground_truth_contributions_manually_gathered_sample"
        ),
        individual_contributions_table=sql.Identifier("individual_contributions"),
    )

    logger.info("Calculating benchmark match rates by state (unique people)...")

    results = []
    with (
        pg.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            results.append(
                {
                    "state": row[0],
                    "total_unique_people": row[1],
                    "matched_unique_people": row[2],
                    "match_rate_percent": row[3],
                }
            )

    logger.info(f"Found {len(results)} states with unique people benchmark data")
    for result in results:
        logger.info(
            f"State: {result['state']}, "
            f"Unique People: {result['total_unique_people']}, "
            f"Matched: {result['matched_unique_people']}, "
            f"Rate: {result['match_rate_percent']}%"
        )

    return dg.MaterializeResult(
        metadata={
            "states_count": len(results),
            "total_unique_people": sum(r["total_unique_people"] for r in results),
            "total_matched_unique_people": sum(
                r["matched_unique_people"] for r in results
            ),
            "results_json": json.dumps(results),
        }
    )


@dg.asset(
    pool="slack",
    description="Send benchmark match rate report to Slack",
)
def send_benchmark_report_to_slack(
    context: dg.AssetExecutionContext,
    benchmark_match_rate_by_state: dg.MaterializeResult,
    benchmark_match_rate_by_state_unique_people: dg.MaterializeResult,
    slack: dg.ResourceParam[SlackResource],
) -> None:
    """
    Send benchmark match rate report to Slack.
    Uses results from upstream assets to avoid re-querying the database.
    """
    # Get results from upstream assets metadata
    match_rate_metadata = benchmark_match_rate_by_state.metadata
    unique_people_metadata = benchmark_match_rate_by_state_unique_people.metadata

    if not match_rate_metadata or not unique_people_metadata:
        message = "No benchmark data found."
    else:
        match_rate_results_json = match_rate_metadata.get("results_json")
        unique_people_results_json = unique_people_metadata.get("results_json")

        if not match_rate_results_json or not unique_people_results_json:
            message = "No benchmark data found."
        else:
            # Parse JSON results - cast to str for type checker
            match_rate_results = json.loads(str(match_rate_results_json))
            unique_people_results = json.loads(str(unique_people_results_json))

            # Sort by match rate (descending)
            match_rate_results.sort(key=lambda x: x["match_rate_percent"], reverse=True)
            unique_people_results.sort(
                key=lambda x: x["match_rate_percent"], reverse=True
            )

            # Format results as a table
            header = (
                "| State | Total | Matched | Match Rate % |\n"
                "|-------|-------|---------|--------------|\n"
            )
            rows = "\n".join(
                [
                    f"| {r['state']} | {r['total_ground_truth']} | "
                    f"{r['matched_count']} | {r['match_rate_percent']}% |"
                    for r in match_rate_results
                ]
            )
            table_str = header + rows

            # Add unique people summary
            unique_summary = "\n\n*Unique People Match Rates:*\n"
            unique_header = (
                "| State | Unique People | Matched | Match Rate % |\n"
                "|-------|---------------|---------|--------------|\n"
            )
            unique_rows = "\n".join(
                [
                    f"| {r['state']} | {r['total_unique_people']} | "
                    f"{r['matched_unique_people']} | {r['match_rate_percent']}% |"
                    for r in unique_people_results
                ]
            )
            unique_table = unique_header + unique_rows

            # Calculate overall stats
            total_ground_truth = sum(
                r["total_ground_truth"] for r in match_rate_results
            )
            total_matched = sum(r["matched_count"] for r in match_rate_results)
            overall_rate = (
                (total_matched / total_ground_truth * 100)
                if total_ground_truth > 0
                else 0
            )

            message = (
                f"*Benchmark Match Rate Report*\n\n"
                f"Overall: {total_matched}/{total_ground_truth} "
                f"({overall_rate:.2f}%)\n\n"
                f"*Contributions Match Rates:*\n```\n{table_str}\n```\n"
                f"{unique_summary}```\n{unique_table}\n```"
            )

    # Send message to Slack
    response = slack.get_client().chat_postMessage(
        channel="#dagster-reports", text=message
    )
    context.log.info(f"Message sent to Slack: {response['ts']}")


assets = [
    benchmark_match_rate_by_state,
    benchmark_match_rate_by_state_unique_people,
    send_benchmark_report_to_slack,
]
