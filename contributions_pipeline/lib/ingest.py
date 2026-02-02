import time
import typing
from _csv import Reader
from collections.abc import Callable

import dagster as dg
from psycopg import Cursor, sql

from contributions_pipeline.lib.iter import batched


def get_sql_binary_format_copy_query_for_landing_table(
    table_name: str, table_columns_name: list[str]
) -> sql.Composed:
    """
    Create a postgres COPY query from STDIN ready to take rows with the same ordering
    as the table_columns_name parameters.

    Parameters:
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted. For example for individual contributions.
                        ["cmte_id", "amndt_ind", "rpt_tp,transaction_pgi", "image_num",
                         "transaction_tp", ..., "sub_id"]
    """
    table_columns_identifiers = [
        sql.Identifier(column_name) for column_name in table_columns_name
    ]
    composed_table_columns = sql.SQL(",").join(table_columns_identifiers)
    table_name_identifier = sql.Identifier(table_name)

    return sql.SQL(
        """COPY {table_name} ({table_columns})
        FROM STDIN (FORMAT BINARY)
        """
    ).format(table_name=table_name_identifier, table_columns=composed_table_columns)


def get_sql_truncate_query(table_name: str) -> sql.Composed:
    """
    Create a truncate query used to remove data and reset table stats in postgres.

    WARNING: using this query will absolutely deletes all the data on the table,
    it's possible to rollback on postgres if you're in a transaction. It is also
    considered a DML, so take precautions when using this query!

    Parameters:
    table_name: the name of the table that's going to be truncated.
    """
    table_name_identifier = sql.Identifier(table_name)
    return sql.SQL("TRUNCATE {table_name}").format(table_name=table_name_identifier)


def get_sql_delete_by_year_query(
    table_name: str, column_name: str, year: int, mode: str | None = None
) -> sql.Composed:
    """
    Build a DELETE SQL query that removes rows for a given year.

    :param table_name: table name
    :param column_name: column name to check year from
    :param year: year (int)
    :param mode: how to get year from column:
                 - None or "column" -> direct year column
                 - "extract"        -> EXTRACT(YEAR FROM col::timestamp)
    """
    table = sql.Identifier(table_name)

    if mode == "extract":
        year_literal = sql.Literal(int(year))
        condition = sql.SQL("EXTRACT(YEAR FROM {col}::timestamp) = {year}").format(
            col=sql.Identifier(column_name), year=year_literal
        )
    else:
        year_literal = sql.Literal(str(year))
        condition = sql.SQL("{col} = {year}").format(
            col=sql.Identifier(column_name), year=year_literal
        )

    return sql.SQL("DELETE FROM {table} WHERE {condition}").format(
        table=table, condition=condition
    )


def get_sql_count_query(table_name: str) -> sql.Composed:
    """
    Create a count query for all rows in the table specifies.

    NOTE: currently it uses `COUNT *` which could be a little slow especially
    on bigger tables.

    Parameters:
    table_name: name of table which rows will be counted
    """
    table_name_identifier = sql.Identifier(table_name)
    return sql.SQL("SELECT COUNT(*) FROM {table_name}").format(
        table_name=table_name_identifier
    )


COPY_INSERT_BATCH_NUM = int(
    typing.cast(str, dg.EnvVar("COPY_INSERT_BATCH_NUM").get_value(default="10_000_000"))
)
LOG_INVALID_ROWS = dg.EnvVar("LOG_INVALID_ROWS").get_value(default="true") == "true"


def insert_parsed_file_to_landing_table(
    pg_cursor: Cursor,
    csv_reader: Reader | typing.Generator[list[str], None, None] | list[tuple],
    table_name: str,
    table_columns_name: list[str],
    row_validation_callback: Callable[[list[str]], bool],
    transform_row_callback: Callable[[list[str]], list[str]] | None = None,
):
    """
    Insert all validated rows in generator to a landing table specified by `table_name`
    params.

    This function assumed that all of the rows have the same columns and columns count.
    If there's possibility that one or more of the row could be invalid, use the
    `row_validation_callback` to validate each row.

    Parameters:
    pg_cursor: postgres cursor, you can get it from `pg_connection.cursor()`
    table_name: the name of the table that's going to be used. for example
                `federal_individual_contributions_landing`
    table_columns_name: list of columns name in order of the data files that's going to
                        be inserted. For example for individual contributions.
                        ["cmte_id", "amndt_ind", "rpt_tp,transaction_pgi", "image_num",
                         "transaction_tp", ..., "sub_id"]
    data_validation_callback: callable to validate the cleaned rows, if the function
                              return is false, the row will be ignored, a warning
                              will be shown on the logs
    transaction_row_callback: a hook to modify (add, remove, etc) columns to the
                              existing row. Useful when you want to add a static
                              value columns (e.g. which partition it comes from)
    """

    logger = dg.get_dagster_logger(f"{table_name}_parsed_file_insert")

    landing_table_copy_query = get_sql_binary_format_copy_query_for_landing_table(
        table_name=table_name, table_columns_name=table_columns_name
    )

    for chunk_rows in batched(iterable=csv_reader, n=COPY_INSERT_BATCH_NUM):
        chunk_len = len(chunk_rows)
        start_time = time.time()
        with pg_cursor.copy(statement=landing_table_copy_query) as copy_cursor:
            for row_number, row_data in enumerate(chunk_rows):
                if not row_validation_callback(row_data):
                    if LOG_INVALID_ROWS:
                        logger.warning(
                            (
                                f"Row no.{row_number + 1} is not valid.",
                                f"Parsed data: {row_data}",
                            )
                        )
                    continue

                row_data = (
                    transform_row_callback(row_data)
                    if transform_row_callback is not None
                    else row_data
                )
                copy_cursor.write_row(row_data)

        elapsed = time.time() - start_time

        logger.info(f"batch done: {round(chunk_len / elapsed, 3)} rows/s")
