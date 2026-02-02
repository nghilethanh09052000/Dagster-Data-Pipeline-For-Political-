import typing

import dagster as dg
from dagster import DagsterError
from dagster_slack import SlackResource
from psycopg import sql

# Import the PostgresResource from the main resources file
from contributions_pipeline.resources import PostgresResource

# Configuration
MAX_RETRIES = 1
MAX_DELTA_ROWS_AT_ONCE = int(
    typing.cast(str, dg.EnvVar("MAX_DELTA_ROWS_AT_ONCE").get_value(default="1_000_000"))
)
MIN_DELTA_ROWS_DELETED_FOR_ALERT = int(
    typing.cast(
        str, dg.EnvVar("MIN_DELTA_ROWS_DELETED_FOR_ALERT").get_value(default="1")
    )
)
CONFIG = {
    "SYNC_LOCK_TABLE": "sync_lock_state",
    "DELTA_ROWS_TABLE": "app_db_delta_rows",
    "LANDING_TABLE_SCHEMA": "etl",
    "LANDING_TABLE": "app_db_delta_rows_landing",
    "SYNCED_ROWS_TABLE": "app_db_synced_rows",
    "SOURCE_TABLE": "individual_contributions",
    "TARGET_TABLE": "individual_contributions",
    "PIPELINE_NAME": "individual_contributions",
}


def send_slack_alert(
    message: str,
    level: typing.Literal["error", "warning", "success"],
    slack: dg.ResourceParam[SlackResource],
) -> None:
    """
    Send alert to Slack channel with proper formatting and emojis.
    Errors go to #data-engineering, warnings and successes go to #dagster-reports.

    Args:
        message: The message to send
        level: The alert level (error, warning, success, info)
        slack: The Slack resource instance
    """
    logger = dg.get_dagster_logger()

    level_config = {
        "error": {"emoji": "🚨", "prefix": "*ERROR*", "channel": "#data-engineering"},
        "warning": {"emoji": "⚠️", "prefix": "*WARNING*", "channel": "#dagster-reports"},
        "success": {
            "emoji": "✅",
            "prefix": "*SUCCESS*",
            "channel": "#dagster-reports",
        },
    }

    config = level_config.get(level, level_config["error"])

    formatted_message = f"{config['emoji']} {config['prefix']}: {message}"

    try:
        response = slack.get_client().chat_postMessage(
            channel=config["channel"], text=formatted_message
        )
        logger.info(
            f"Slack alert sent successfully to {config['channel']}: \
            {response.get('ts', 'unknown')}"
        )
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e!s}")


@dg.asset(
    description="Check sync lock status to ensure no other sync process is running",
)
def check_sync_lock_status(
    pg: dg.ResourceParam[PostgresResource],
    slack: dg.ResourceParam[SlackResource],
) -> dg.MaterializeResult:
    """
    Check if sync is locked and acquire lock atomically using SELECT FOR UPDATE.
    If locked, raise error to prevent concurrent syncs.
    """
    logger = dg.get_dagster_logger()

    with (
        pg.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        conn.autocommit = False

        try:
            # Use SELECT FOR UPDATE to check lock status and acquire row-level lock

            table_name = sql.Identifier(CONFIG["SYNC_LOCK_TABLE"])

            query = sql.SQL(
                """
                SELECT is_locked
                FROM {table}
                WHERE pipeline_name = %s
                FOR UPDATE
            """
            ).format(table=table_name)

            cursor.execute(query, (CONFIG["PIPELINE_NAME"],))
            result = cursor.fetchone()

            if result and result[0]:
                error_msg = (
                    f"Sync is currently locked for {CONFIG['PIPELINE_NAME']}. "
                    "Another sync process may be running."
                )
                logger.error(error_msg)
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg)

            # Set lock to True atomically
            query = sql.SQL(
                """
                INSERT INTO {table} (pipeline_name, is_locked, locked_at)
                VALUES (%s, TRUE, NOW())
                ON CONFLICT (pipeline_name)
                    DO UPDATE SET
                        is_locked = EXCLUDED.is_locked,
                        locked_at = EXCLUDED.locked_at
                """
            ).format(table=table_name)

            cursor.execute(query, (CONFIG["PIPELINE_NAME"],))

            conn.commit()
            logger.info(f"Sync lock acquired for {CONFIG['PIPELINE_NAME']}")
            return dg.MaterializeResult(metadata={"lock_status": "locked"})

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to acquire sync lock: {e!s}")
            send_slack_alert(f"Failed to acquire sync lock: {e!s}", "error", slack)
            raise e


@dg.asset(
    deps=[check_sync_lock_status],
    description="Truncate delta rows table to prepare for new delta calculation",
)
def truncate_delta_rows(
    pg: dg.ResourceParam[PostgresResource],
    slack: dg.ResourceParam[SlackResource],
) -> dg.MaterializeResult:
    """Truncate the delta rows table to prepare for new delta calculation."""
    logger = dg.get_dagster_logger()

    try:
        with (
            pg.pool.connection() as conn,
            conn.cursor() as cursor,
        ):
            truncate_query = sql.SQL("TRUNCATE TABLE {table}").format(
                table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
            )
            cursor.execute(truncate_query)

            logger.info(f"Truncated {CONFIG['DELTA_ROWS_TABLE']}")

        return dg.MaterializeResult(metadata={"action": "truncated_delta_rows"})

    except Exception as e:
        error_msg = f"Failed to truncate {CONFIG['DELTA_ROWS_TABLE']}: {e!s}"
        logger.error(error_msg)
        send_slack_alert(error_msg, "error", slack)
        raise DagsterError(error_msg) from e


@dg.asset(
    deps=[truncate_delta_rows],
    description="Check if delta rows table is empty after truncation",
)
def check_delta_rows_empty(
    pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
):
    """
    Check if the delta rows table is empty after truncation.
    If not empty, raise error for manual intervention as specified
    in the engineering plan.
    """
    logger = dg.get_dagster_logger()

    try:
        with (
            pg.pool.connection() as conn,
            conn.cursor() as cursor,
        ):
            query = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
            )
            cursor.execute(query)
            result = cursor.fetchone()
            count = result[0] if result else 0

            if count > 0:
                error_msg = (
                    f"Delta rows table {CONFIG['DELTA_ROWS_TABLE']} "
                    f"is not empty ({count} rows) "
                    f"after truncation. Something awfully wrong here. "
                    f"Manual intervention required."
                )
                logger.error(error_msg)
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg)

            logger.info(
                "Delta rows table is empty after truncationwith delta calculation"
            )

        return dg.MaterializeResult(metadata={"delta_rows_status": "empty"})

    except Exception as e:
        error_msg = f"Failed to check delta rows table: {e!s}"
        logger.error(error_msg)
        send_slack_alert(error_msg, "error", slack)
        raise DagsterError(error_msg) from e


@dg.asset(
    deps=[check_delta_rows_empty],
    description="Calculate and insert delta rows that haven't been synced to app db",
)
def calculate_delta_rows(
    pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
) -> dg.MaterializeResult:
    """
    Calculate delta rows by comparing source table with synced rows table.
    Only includes rows that haven't been synced to the app database yet.

    This implements the engineering plan's requirement to get rows that have not been
    inserted to app db by querying the master table and left joining to synced rows.
    """
    logger = dg.get_dagster_logger()

    with (
        pg.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        conn.autocommit = False

        try:
            # Get the latest sync timestamp

            query = sql.SQL(
                """
                SELECT MAX(insert_time)
                FROM {table}
            """
            ).format(table=sql.Identifier(CONFIG["SYNCED_ROWS_TABLE"]))
            cursor.execute(query)
            result = cursor.fetchone()
            max_sync_time = result[0] if result else None

            if max_sync_time is None:
                # First sync - get all rows from source table using INSERT with CTE
                insert_query = sql.SQL(
                    """
                    INSERT INTO {delta_table} (
                        source, committee_id, source_id, name_concat, employer,
                        occupation,
                        amount, contribution_datetime, committee_name,
                        candidate_id,
                        candidate_name, candidate_office, insert_time
                    )
                    WITH source_data AS (
                        SELECT
                            source, committee_id, source_id, name_concat, employer,
                            occupation,
                            amount, contribution_datetime, committee_name,
                            candidate_id,
                            candidate_name, candidate_office, NOW() as insert_time
                        FROM {source_table}
                    )
                    SELECT * FROM source_data
                """
                ).format(
                    delta_table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"]),
                    source_table=sql.Identifier(CONFIG["SOURCE_TABLE"]),
                )

                logger.info("First sync - inserting all rows from source table")
                cursor.execute(insert_query)

            else:
                # Incremental sync - get only new rows using INSERT with CTE
                # This implements the engineering plan's left join approach
                insert_query = sql.SQL(
                    """
                    INSERT INTO {delta_table} (
                        source, committee_id, source_id, name_concat,
                        employer, occupation,
                        amount, contribution_datetime, committee_name,
                        candidate_id,
                        candidate_name, candidate_office, insert_time
                    )
                    WITH new_rows AS (
                        SELECT
                            l.source, l.committee_id, l.source_id,
                            l.name_concat, l.employer,
                            l.occupation,
                            l.amount, l.contribution_datetime,
                            l.committee_name, l.candidate_id,
                            l.candidate_name, l.candidate_office,
                            NOW() as insert_time
                        FROM {source_table} l
                        LEFT JOIN {synced_table} s
                        ON l.source_id = s.source_id
                        WHERE s.source_id IS NULL
                    )
                    SELECT * FROM new_rows
                """
                ).format(
                    delta_table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"]),
                    source_table=sql.Identifier(CONFIG["SOURCE_TABLE"]),
                    synced_table=sql.Identifier(CONFIG["SYNCED_ROWS_TABLE"]),
                )
                logger.info(
                    f"Inserting delta rows into {CONFIG['DELTA_ROWS_TABLE']} "
                    f"from {CONFIG['SOURCE_TABLE']} "
                    f"using incremental sync"
                )
                cursor.execute(insert_query)

            # Get count of inserted rows
            query = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
            )
            cursor.execute(query)
            result = cursor.fetchone()
            row_count = result[0] if result else 0

            conn.commit()
            logger.info(
                f"Calculated {row_count} delta rows from {CONFIG['SOURCE_TABLE']} "
                f"using INSERT with CTE and left join approach"
            )

        except Exception as e:
            conn.rollback()
            error_msg = f"Failed to calculate delta rows: {e!s}"
            logger.error(error_msg)
            send_slack_alert(error_msg, "error", slack)
            raise DagsterError(error_msg) from e

    return dg.MaterializeResult(metadata={"delta_rows_count": row_count})


@dg.asset(deps=[calculate_delta_rows])
def limit_delta_rows(
    pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
):
    """
    This asset will limit the amount of delta rows that's going to be inserted to
    app db. The hard limit is added to make sure that it doesn't take too much
    time in app db inserting to the production table.

    This asset will also alert the amount of rows cut off from the hard limit.
    """

    logger = dg.get_dagster_logger()

    for attempt in range(MAX_RETRIES + 1):
        try:
            with pg.pool.connection() as conn, conn.cursor() as cursor:
                conn.autocommit = False

                delta_rows_count_query = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                    table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
                )
                cursor.execute(delta_rows_count_query)

                count_query_result = cursor.fetchone()
                delta_rows_count = count_query_result[0] if count_query_result else 0

                if delta_rows_count < 1:
                    logger.warning("No rows in the delta table, ignoring...")
                    break

                # If we need to limit rows, delete the overflow in place
                if delta_rows_count > MAX_DELTA_ROWS_AT_ONCE:
                    delete_overflow_query = sql.SQL(
                        """
                        DELETE FROM {delta_table}
                        WHERE ctid IN (
                            SELECT ctid
                            FROM {delta_table}
                            ORDER BY contribution_datetime DESC
                            OFFSET {delta_row_limit}
                        )
                        """
                    ).format(
                        delta_table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"]),
                        delta_row_limit=sql.Literal(MAX_DELTA_ROWS_AT_ONCE),
                    )
                    overflow_count = delta_rows_count - MAX_DELTA_ROWS_AT_ONCE
                    logger.info(
                        f"Deleting {overflow_count} overflow rows from"
                        f" {CONFIG['DELTA_ROWS_TABLE']} table"
                        f" (keeping newest {MAX_DELTA_ROWS_AT_ONCE})"
                    )
                    cursor.execute(delete_overflow_query)

                    # Get the actual number of deleted rows
                    deleted_rows_count = cursor.rowcount
                    logger.info(f"Deleted {deleted_rows_count} rows from delta table")
                else:
                    deleted_rows_count = 0
                    logger.info(
                        f"Delta table has {delta_rows_count} rows, within limit of"
                        f" {MAX_DELTA_ROWS_AT_ONCE}. No rows deleted."
                    )

                conn.commit()

                # Sanity check that the row count is now within limit
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {delta_table}").format(
                        delta_table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
                    )
                )
                final_count_result = cursor.fetchone()

                assert final_count_result, (
                    "Unable to fetch final count from delta rows table"
                )

                final_count = final_count_result[0]

                # final count must be a number
                assert isinstance(final_count, int), (
                    f"Final count is not an integer: {final_count!r}"
                )

                if final_count > MAX_DELTA_ROWS_AT_ONCE:
                    error_msg = (
                        f"Programming error: "
                        f"Failed to limit delta rows to {MAX_DELTA_ROWS_AT_ONCE}."
                        f"Current count is {final_count} after limit_delta_rows."
                        f"Manual intervention required."
                    )
                    logger.error(error_msg)
                    send_slack_alert(error_msg, "error", slack)
                    raise DagsterError(error_msg)

                # Below should be safe, as it *should not* throw any error or exception,
                # thus the process above should not be retried beyond this point
                if deleted_rows_count >= MIN_DELTA_ROWS_DELETED_FOR_ALERT:
                    send_slack_alert(
                        message=f"Delta rows limited to {MAX_DELTA_ROWS_AT_ONCE},"
                        f" leaving {deleted_rows_count} rows behind",
                        level="warning",
                        slack=slack,
                    )

                break

        except Exception as e:
            logger.error(f"Limiting delta rows attempt {attempt + 1} failed: {e!s}")

            conn.rollback()

            if attempt < MAX_RETRIES:
                logger.info(
                    f"Retrying limit operation..."
                    f"(attempt {attempt + 2}/{MAX_RETRIES + 1})"
                )
                continue
            else:
                error_msg = (
                    f"Failed to limit delta rows after"
                    f"{MAX_RETRIES + 1} attempts."
                    f"Last error: {e!s}"
                )
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg) from e


@dg.asset(
    deps=[limit_delta_rows],
    description="Check if landing table is empty before proceeding",
)
def check_landing_table_empty(
    app_pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
) -> dg.MaterializeResult:
    """
    Check if the landing table is empty. If not, raise error for manual intervention.
    """
    logger = dg.get_dagster_logger()

    try:
        with (
            app_pg.pool.connection() as conn,
            conn.cursor() as cursor,
        ):
            query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
                schema=sql.Identifier(CONFIG["LANDING_TABLE_SCHEMA"]),
                table=sql.Identifier(CONFIG["LANDING_TABLE"]),
            )
            cursor.execute(query)
            result = cursor.fetchone()
            count = result[0] if result else 0

            if count > 0:
                error_msg = (
                    f"Landing table {CONFIG['LANDING_TABLE']} "
                    f"is not empty ({count} rows). "
                    "Manual intervention required."
                )
                logger.error(error_msg)
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg)

            logger.info("Landing table is empty - proceeding with data transfer")

        return dg.MaterializeResult(metadata={"landing_status": "empty"})

    except Exception as e:
        error_msg = f"Failed to check landing table: {e!s}"
        logger.error(error_msg)
        send_slack_alert(error_msg, "error", slack)
        raise DagsterError(error_msg) from e


@dg.asset(
    deps=[check_landing_table_empty],
    description="Copy delta rows from pipeline db to app db landing table",
)
def copy_delta_to_landing(
    pg: dg.ResourceParam[PostgresResource],
    app_pg: dg.ResourceParam[PostgresResource],
    slack: dg.ResourceParam[SlackResource],
):
    """
    Copy delta rows from pipeline database to app database landing table.
    Uses efficient bulk transfer with retry logic and proper transaction handling.

    This implements the engineering plan's requirement for copy to/from operations
    with transaction handling and retry logic (2x retries as specified).
    """
    logger = dg.get_dagster_logger()

    for attempt in range(MAX_RETRIES + 1):
        try:
            # Start transaction on app db

            with app_pg.pool.connection() as app_conn, app_conn.cursor() as app_cursor:
                app_conn.autocommit = False

                try:
                    # Get count of rows to be copied
                    with (
                        pg.pool.connection() as pipeline_conn,
                        pipeline_conn.cursor() as pipeline_cursor,
                    ):
                        query = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                            table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
                        )
                        pipeline_cursor.execute(query)
                        result = pipeline_cursor.fetchone()
                        row_count = result[0] if result else 0

                    if row_count > 0:
                        # Use COPY for efficient bulk transfer between databases
                        # This implements the engineering plan's copy to/from approach
                        copy_out_query = sql.SQL(
                            "COPY {} TO STDOUT (FORMAT BINARY)"
                        ).format(sql.Identifier(CONFIG["DELTA_ROWS_TABLE"]))
                        copy_in_query = sql.SQL(
                            "COPY {schema}.{table} FROM STDIN (FORMAT BINARY)"
                        ).format(
                            schema=sql.Identifier(CONFIG["LANDING_TABLE_SCHEMA"]),
                            table=sql.Identifier(CONFIG["LANDING_TABLE"]),
                        )

                        with (
                            pg.pool.connection() as pipeline_conn,
                            pipeline_conn.cursor() as pipeline_cursor,
                            app_pg.pool.connection() as app_conn,
                            app_conn.cursor() as app_cursor,
                        ):
                            with (
                                pipeline_cursor.copy(copy_out_query) as copy_out,
                                app_cursor.copy(copy_in_query) as copy_in,
                            ):
                                for data in copy_out:
                                    copy_in.write(data)

                            app_conn.commit()
                    else:
                        logger.info("No rows to copy - delta table is empty")

                    success_msg = (
                        f"Successfully copied {row_count} rows to "
                        f"landing table using COPY (attempt {attempt + 1}) "
                        "in copy_delta_to_landing asset"
                    )
                    logger.info(success_msg)
                    return dg.MaterializeResult(metadata={"copied_rows": row_count})

                except Exception as e:
                    app_conn.rollback()
                    raise e

        except Exception as e:
            logger.error(f"Copy attempt {attempt + 1} failed: {e!s}")
            if attempt < MAX_RETRIES:
                logger.info(
                    f"Retrying copy operation..."
                    f"(attempt {attempt + 2}/{MAX_RETRIES + 1})"
                )
                continue
            else:
                error_msg = (
                    f"Failed to copy data to landing table after"
                    f"{MAX_RETRIES + 1} attempts."
                    f"Last error: {e!s}"
                )
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg) from e


@dg.asset(
    deps=[copy_delta_to_landing],
    description="Insert landing data into production table",
)
def insert_landing_to_production(
    app_pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
) -> dg.MaterializeResult:
    """
    Insert data from landing table to production table with retry logic.

    Process:
    1. Check if landing table has data to insert
    2. Insert data with retry logic (up to MAX_RETRIES times)
    3. Only truncate landing table if insert succeeds
    4. Return count of inserted rows

    Safety Features:
    - Retries insert operation on failure (2x retries)
    - Only truncates landing table after successful insert
    - Clear error messages and logging
    - Proper transaction handling
    """
    logger = dg.get_dagster_logger()

    # Get row count before proceeding
    with (
        app_pg.pool.connection() as count_conn,
        count_conn.cursor() as count_cursor,
    ):
        count_query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
            schema=sql.Identifier(CONFIG["LANDING_TABLE_SCHEMA"]),
            table=sql.Identifier(CONFIG["LANDING_TABLE"]),
        )
        count_cursor.execute(count_query)
        result = count_cursor.fetchone()
        inserted_count = result[0] if result else 0

    if inserted_count == 0:
        logger.info("No rows to insert from landing to production.")
        return dg.MaterializeResult(metadata={"inserted_rows": 0})

    # First, try to insert data (with retries)
    insert_successful = False
    for attempt in range(MAX_RETRIES + 1):
        try:
            # Insert data from landing to production table

            with (
                app_pg.pool.connection() as conn,
                conn.cursor() as cursor,
            ):
                conn.autocommit = False

                try:
                    cursor.execute("SET statement_timeout = '3h'")

                    # Insert all rows from landing table to production table
                    insert_query = sql.SQL(
                        """
                        INSERT INTO {target_table} (
                            source, committee_id, name_concat, employer,
                            occupation, amount, contribution_datetime, committee_name,
                            candidate_id, candidate_name, candidate_office
                        )
                        SELECT
                            source, committee_id, name_concat, employer,
                            occupation, amount, contribution_datetime, committee_name,
                            candidate_id, candidate_name, candidate_office
                        FROM {landing_table_schema}.{landing_table}
                    """
                    ).format(
                        target_table=sql.Identifier(CONFIG["TARGET_TABLE"]),
                        landing_table_schema=sql.Identifier(
                            CONFIG["LANDING_TABLE_SCHEMA"]
                        ),
                        landing_table=sql.Identifier(CONFIG["LANDING_TABLE"]),
                    )

                    cursor.execute(insert_query)

                    conn.commit()
                    logger.info(
                        f"Successfully inserted {inserted_count} rows from "
                        f"{CONFIG['LANDING_TABLE']} to {CONFIG['TARGET_TABLE']} "
                        f"(attempt {attempt + 1})"
                    )

                    # Mark insert as successful and break out of retry loop
                    insert_successful = True
                    break

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Failed to insert data: {e!s}")
                    raise e

        except Exception as e:
            logger.error(f"Production insert attempt {attempt + 1} failed: {e!s}")
            if attempt < MAX_RETRIES:
                logger.info(
                    f"Retrying production insert..."
                    f"(attempt {attempt + 2}/{MAX_RETRIES + 1})"
                )
                continue
            else:
                error_msg = (
                    f"Failed to insert to production after {MAX_RETRIES + 1} attempts. "
                    f"Last error: {e!s}"
                )
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg) from e

    # Only truncate if insert succeeded - this should NOT be retried
    # If truncate fails, we don't want to retry the insert
    if insert_successful:
        try:
            with (
                app_pg.pool.connection() as cleanup_conn,
                cleanup_conn.cursor() as cleanup_cursor,
            ):
                clean_query = sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                    schema=sql.Identifier(CONFIG["LANDING_TABLE_SCHEMA"]),
                    table=sql.Identifier(CONFIG["LANDING_TABLE"]),
                )
                cleanup_cursor.execute(clean_query)
                cleanup_conn.commit()

            logger.info(f"Truncated {CONFIG['LANDING_TABLE']}")
        except Exception as e:
            # Log truncate failure but don't fail the asset since insert succeeded
            logger.error(f"Failed to truncate landing table: {e!s}")
            send_slack_alert(
                f"Error: Failed to truncate landing table"
                f"after successful insert: {e!s}",
                "error",
                slack,
            )

    return dg.MaterializeResult(metadata={"inserted_rows": inserted_count})


@dg.asset(
    deps=["mark_rows_as_synced"],
    description="Clean up delta rows and release sync lock",
)
def cleanup_and_release_lock(
    pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
):
    """
    Clean up delta rows table and release the sync lock.
    Includes proper transaction handling and error recovery with retries.

    This asset performs the final cleanup step of the sync process. It includes
    retry logic for cleanup operations and only releases the lock after successful
    cleanup to prevent data inconsistencies.

    Process:
    1. Retry cleanup operations (truncate delta rows) up to MAX_RETRIES times
    2. Only release lock after successful cleanup
    3. If cleanup fails after all retries, lock remains active for manual intervention
    4. Send success notification only after complete cleanup

    Safety Features:
    - Retries cleanup operations to handle transient failures (2x retries)
    - Only releases lock after successful cleanup
    - Lock remains active if cleanup fails, preventing concurrent access
    - Clear error messages for manual intervention when needed
    """
    logger = dg.get_dagster_logger()

    for attempt in range(MAX_RETRIES + 1):
        try:
            with (
                pg.pool.connection() as conn,
                conn.cursor() as cursor,
            ):
                conn.autocommit = False

                try:
                    # Truncate delta rows table
                    truncate_query = sql.SQL("TRUNCATE TABLE {table}").format(
                        table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
                    )
                    cursor.execute(truncate_query)

                    # UPDATE sync_lock table to release lock
                    update_lock_query = sql.SQL(
                        """
                        UPDATE {lock_table}
                        SET is_locked = FALSE, locked_at = NULL
                        WHERE pipeline_name = %s
                    """
                    ).format(
                        lock_table=sql.Identifier(CONFIG["SYNC_LOCK_TABLE"]),
                    )
                    cursor.execute(update_lock_query, (CONFIG["PIPELINE_NAME"],))

                    conn.commit()
                    logger.info("Cleanup completed and sync lock released")

                    # Send success notification
                    success_msg = (
                        f"{CONFIG['PIPELINE_NAME']} "
                        "pipeline sync completed successfully"
                    )
                    send_slack_alert(success_msg, "success", slack)

                    return dg.MaterializeResult(metadata={"status": "completed"})

                except Exception as e:
                    conn.rollback()
                    raise e

        except Exception as e:
            logger.error(f"Cleanup attempt {attempt + 1} failed: {e!s}")
            if attempt < MAX_RETRIES:
                logger.info(
                    f"Retrying cleanup operation..."
                    f"(attempt {attempt + 2}/{MAX_RETRIES + 1})"
                )
                continue
            else:
                error_msg = (
                    f"Failed to complete cleanup after {MAX_RETRIES + 1} attempts. "
                    f"Lock will remain active for {CONFIG['PIPELINE_NAME']}. "
                    f"Manual intervention required. Last error: {e!s}"
                )
                send_slack_alert(error_msg, "error", slack)
                raise DagsterError(error_msg) from e


@dg.asset(
    deps=[insert_landing_to_production],
    description="Mark rows as synced in pipeline database",
)
def mark_rows_as_synced(
    pg: dg.ResourceParam[PostgresResource], slack: dg.ResourceParam[SlackResource]
) -> dg.MaterializeResult:
    """
    Insert source_ids from delta rows into synced rows table to mark them as processed.
    This happens AFTER app db processing to ensure the entire pipeline completes
    successfully
    before marking rows as synced. This makes it easier to retry the job in the future
    since synced rows won't be marked until the very end.
    """
    logger = dg.get_dagster_logger()

    with (
        pg.pool.connection() as conn,
        conn.cursor() as cursor,
    ):
        conn.autocommit = False

        try:
            # Insert source_ids from delta rows into synced rows table,
            # update timestamp for duplicates to track last processing time

            insert_query = sql.SQL(
                """
                INSERT INTO {synced_table} (source_id, insert_time, synced_at)
                SELECT source_id, NOW(), NOW()
                FROM {delta_table}
                WHERE source_id IS NOT NULL
                ON CONFLICT (source_id) DO UPDATE SET
                    synced_at = EXCLUDED.synced_at
            """
            ).format(
                synced_table=sql.Identifier(CONFIG["SYNCED_ROWS_TABLE"]),
                delta_table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"]),
            )

            cursor.execute(insert_query)

            # Count rows from delta table
            count_query = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=sql.Identifier(CONFIG["DELTA_ROWS_TABLE"])
            )
            cursor.execute(count_query)
            result = cursor.fetchone()
            marked_count = result[0] if result else 0

            conn.commit()
            logger.info(f"Marked {marked_count} rows as synced")

        except Exception as e:
            conn.rollback()
            error_msg = f"Failed to mark rows as synced: {e!s}"
            logger.error(error_msg)
            send_slack_alert(error_msg, "error", slack)
            raise e

    return dg.MaterializeResult(metadata={"marked_rows": marked_count})


assets = [
    check_sync_lock_status,
    truncate_delta_rows,
    check_delta_rows_empty,
    calculate_delta_rows,
    limit_delta_rows,
    check_landing_table_empty,
    copy_delta_to_landing,
    insert_landing_to_production,
    mark_rows_as_synced,
    cleanup_and_release_lock,
]
