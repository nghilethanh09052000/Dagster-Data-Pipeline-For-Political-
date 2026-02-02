import dagster as dg
from dagster_slack import SlackResource

from contributions_pipeline.resources import PostgresResource


@dg.asset(pool="pg")
def contribution_source_report(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
) -> list[tuple[str, int]]:
    """
    Generate a report that shows the count of individual contributions by source.
    """
    query = """
    SELECT
        source,
        count(*)
    FROM
        individual_contributions
    GROUP BY
        source
    ORDER BY
        count(*) DESC;
    """
    context.log.info(f"Executing query: {query}")

    results = []
    with pg.pool.connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()

    context.log.info(f"Query results: {results}")
    return results


@dg.asset(pool="slack")
def send_contribution_source_report_to_slack(
    context: dg.AssetExecutionContext,
    contribution_source_report: list[tuple[str, int]],
    slack: dg.ResourceParam[SlackResource],
) -> None:
    """
    Send the contribution source report to Slack.
    """
    if not contribution_source_report:
        message = "No contribution sources found."
    else:
        # Format results as a table
        header = "| Source | Count |\n|--------|-------|\n"
        rows = "\n".join(
            [f"| {row[0] or 'NULL'} | {row[1]} |" for row in contribution_source_report]
        )
        table_str = header + rows
        message = f"*individual_contributions Report*\n```\n{table_str}\n```"

    # Send message to Slack - updated to use correct API
    response = slack.get_client().chat_postMessage(
        channel="#dagster-reports", text=message
    )
    context.log.info(f"Message sent to Slack: {response['ts']}")
