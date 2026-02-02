from psycopg import sql

from contributions_pipeline.lib.ingest import (
    get_sql_binary_format_copy_query_for_landing_table,
    get_sql_truncate_query,
)


class TestCommonIngest:
    """Test suite for common ingestion function"""

    def test_get_sql_truncate_query(self):
        """Test SQL truncate query generation."""
        test_cases = [
            "test_table",
            "federal_individual_contributions_landing",
            "table_with_special_chars$%",
        ]

        for table_name in test_cases:
            result = get_sql_truncate_query(table_name=table_name)
            # Check that the result is a Composed SQL object
            assert isinstance(result, sql.Composed)

            # Convert to string representation and check it contains key components
            result_str = str(result)
            assert "TRUNCATE" in result_str
            assert table_name in result_str

    def test_get_sql_copy_query_for_federal_table(self):
        """Test SQL COPY query generation for federal tables."""
        table_name = "test_table"
        columns = ["col1", "col2", "col3"]

        result = get_sql_binary_format_copy_query_for_landing_table(
            table_name=table_name, table_columns_name=columns
        )

        # Check that the result is a Composed SQL object
        assert isinstance(result, sql.Composed)

        # Check that it contains the table name
        assert table_name in str(result)

        # Check that all column names are included
        for col in columns:
            assert col in str(result)

        # Test with more complex column names
        complex_columns = ["column_with_underscore", "CamelCase", "snake_case_name"]
        result = get_sql_binary_format_copy_query_for_landing_table(
            table_name="complex_table", table_columns_name=complex_columns
        )
        assert isinstance(result, sql.Composed)
        assert "complex_table" in str(result)
        for col in complex_columns:
            assert col in str(result)
