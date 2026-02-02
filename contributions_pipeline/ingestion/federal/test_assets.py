import io
import os
import zipfile
from unittest import mock
from zipfile import ZipFile

import pytest
import responses
from psycopg import sql

from contributions_pipeline.ingestion.federal.assets import (
    FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
    fec_fetch_all_one_cycle_files,
    get_current_cycle_year,
    get_file_path_for_combined_federal_one_cycle_bulk_downloads,
    get_result_directory_path_for_federal_bulk_download_one_cycle,
    get_url_for_federal_bulk_download_one_cycle,
)

# Constants for test configuration
TEST_REPORT_PREFIX = "test_prefix"
TEST_CONTENT = "|TEST-ID-123|Acme Inc.|Latin1 characters ¾ø	ß|\x00|"
TEST_ZIP_FILE_NAME = "random_file_name_not_important.txt"
TEST_CURRENT_YEAR = 2026  # Even year for testing
TEST_START_YEAR = 2020


@pytest.fixture
def mock_zip_content():
    """
    Fixture providing dynamically generated mock zip file content.
    Creates a zip file in memory containing a test text file.
    """
    # Create a BytesIO buffer to hold our zip file
    buffer = io.BytesIO()

    # Create a zip file in the buffer
    with ZipFile(buffer, mode="w") as zip_file:
        # Add a mock text file to the zip
        zip_file.writestr(TEST_ZIP_FILE_NAME, TEST_CONTENT)

    # Return the zip file content
    buffer.seek(0)
    return buffer.read()


@pytest.fixture
def mock_zip_content_with_directory():
    """
    Fixture providing mock zip file content with a directory structure.
    """
    buffer = io.BytesIO()

    with ZipFile(buffer, mode="w") as zip_file:
        # Add a directory
        zip_file.writestr("test_dir/", "")
        # Add a file in that directory
        zip_file.writestr("test_dir/nested_file.txt", TEST_CONTENT)
        # Add a file in the root
        zip_file.writestr(TEST_ZIP_FILE_NAME, TEST_CONTENT)

    buffer.seek(0)
    return buffer.read()


@pytest.fixture
def mock_current_year():
    """Mock the current year for testing."""
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value.year = TEST_CURRENT_YEAR - 1  # Odd year
        yield mock_datetime


class TestFecAssets:
    """
    Test suite for FEC asset functionality.

    These tests verify the FEC data fetching and extraction pipeline
    using mocked HTTP responses and filesystem operations. All external
    dependencies are mocked to ensure tests are hermetic and repeatable.
    """

    def test_get_url_for_federal_bulk_download_one_cycle(self):
        """Test URL construction for FEC bulk downloads."""
        # Test with different years and prefixes
        test_cases = [
            (
                2020,
                "indiv",
                "https://www.fec.gov/files/bulk-downloads/2020/indiv20.zip",
            ),
            (2022, "cm", "https://www.fec.gov/files/bulk-downloads/2022/cm22.zip"),
            (2024, "cn", "https://www.fec.gov/files/bulk-downloads/2024/cn24.zip"),
            (
                2026,
                TEST_REPORT_PREFIX,
                f"https://www.fec.gov/files/bulk-downloads/2026/{TEST_REPORT_PREFIX}26.zip",
            ),
        ]

        for year, prefix, expected_url in test_cases:
            result = get_url_for_federal_bulk_download_one_cycle(
                year=year, prefix=prefix
            )
            assert result == expected_url, f"Failed for year={year}, prefix={prefix}"

    def test_get_result_directory_path_for_federal_bulk_download_one_cycle(self):
        """Test directory path construction for FEC bulk downloads."""
        test_cases = [
            (2020, "indiv", "./fec/indiv/2020"),
            (2022, "cm", "./fec/cm/2022"),
            (2024, "cn", "./fec/cn/2024"),
            (2026, TEST_REPORT_PREFIX, f"./fec/{TEST_REPORT_PREFIX}/2026"),
        ]

        for year, prefix, expected_path in test_cases:
            result = get_result_directory_path_for_federal_bulk_download_one_cycle(
                year=year, prefix=prefix
            )
            assert result == expected_path, f"Failed for year={year}, prefix={prefix}"

    def test_get_file_path_for_combined_federal_one_cycle_bulk_downloads(self):
        """Test file path construction for combined FEC data."""
        test_cases = [
            ("indiv", "./fec/indiv/combined.txt"),
            ("cm", "./fec/cm/combined.txt"),
            ("cn", "./fec/cn/combined.txt"),
            (TEST_REPORT_PREFIX, f"./fec/{TEST_REPORT_PREFIX}/combined.txt"),
        ]

        for prefix, expected_path in test_cases:
            result = get_file_path_for_combined_federal_one_cycle_bulk_downloads(
                prefix=prefix
            )
            assert result == expected_path, f"Failed for prefix={prefix}"

    def test_get_current_cycle_year(self, mock_current_year):
        """Test current cycle year calculation."""
        # Mock returns TEST_CURRENT_YEAR - 1 (odd year), function should return
        # TEST_CURRENT_YEAR (even year)
        assert get_current_cycle_year() == TEST_CURRENT_YEAR

        # Test with even year
        mock_current_year.now.return_value.year = TEST_CURRENT_YEAR
        assert get_current_cycle_year() == TEST_CURRENT_YEAR

    @responses.activate
    @mock.patch("pathlib.Path.mkdir")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists", side_effect=lambda _: True)
    @mock.patch("os.makedirs", side_effect=lambda path, exist_ok=True: None)
    def test_fetch_one_cycle_files_basic(
        self,
        mock_makedirs,
        mock_os_exists,
        mock_os_remove,
        mock_mkdir,
        mock_zip_content,
    ):
        """
        Test that FEC bulk data files are correctly downloaded and extracted.

        This test verifies that:
        1. The correct URL is constructed for the FEC data download
        2. Directory structure is created with appropriate permissions
        3. Downloaded zip file is extracted to the correct location
        4. Temporary zip file is deleted after extraction
        """
        # Arrange
        year = 2025
        expected_url = get_url_for_federal_bulk_download_one_cycle(
            year=year, prefix=TEST_REPORT_PREFIX
        )
        expected_extract_path = (
            get_result_directory_path_for_federal_bulk_download_one_cycle(
                year=year, prefix=TEST_REPORT_PREFIX
            )
        )
        expected_temp_zip_path = os.path.join(
            expected_extract_path, f"temp_archive_{year}.zip"
        )
        expected_extracted_file_path = os.path.join(
            expected_extract_path, TEST_ZIP_FILE_NAME
        )

        # Mock HTTP response
        responses.add(
            method="GET",
            url=expected_url,
            body=mock_zip_content,
            content_type="application/zip",
            status=200,
        )

        # Mock files
        mock_files = mock.mock_open()
        # NOTE: the value is for the unzip, somehow it doesn't work with mock_open
        # but works with MagicMock with BytesIO, and it needs to be on io.open
        mock_zip_files = mock.MagicMock(
            side_effect=lambda *args, **kwargs: io.BytesIO(mock_zip_content)
        )

        # Act
        with (
            mock.patch("builtins.open", mock_files),
            mock.patch("io.open", mock_zip_files),
        ):
            fec_fetch_all_one_cycle_files(
                start_year=year,
                fec_report_type_prefix=TEST_REPORT_PREFIX,
                human_friendly_name="Test FEC Files",
            )

        # Assert

        # Verify correct directory creation
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Verify HTTP request was made correctly
        assert len(responses.calls) == 1
        assert responses.calls[0].request.url == expected_url

        # Verify zip file is written
        mock_files.assert_any_call(expected_temp_zip_path, "wb")

        # Verify content is written
        mock_files.assert_any_call(expected_extracted_file_path, "wb")

        # Verify temp file is deleted
        mock_os_exists.assert_any_call(expected_temp_zip_path)
        mock_os_remove.assert_any_call(expected_temp_zip_path)

    @responses.activate
    @mock.patch("pathlib.Path.mkdir")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists", side_effect=lambda _: True)
    @mock.patch("os.makedirs")
    def test_fetch_one_cycle_files_with_directory_structure(
        self,
        mock_makedirs,
        mock_os_exists,
        mock_os_remove,
        mock_mkdir,
        mock_zip_content_with_directory,
    ):
        """Test extraction of zip files containing directory structures."""
        # Arrange
        year = 2024  # Use an even year that will be in the range

        # Force execution on just one year for this test by setting start_year
        # = current_year
        expected_url = get_url_for_federal_bulk_download_one_cycle(
            year=year, prefix=TEST_REPORT_PREFIX
        )
        expected_extract_path = (
            get_result_directory_path_for_federal_bulk_download_one_cycle(
                year=year, prefix=TEST_REPORT_PREFIX
            )
        )
        expected_temp_zip_path = os.path.join(
            expected_extract_path, f"temp_archive_{year}.zip"
        )

        # Mock HTTP response
        responses.add(
            method="GET",
            url=expected_url,
            body=mock_zip_content_with_directory,
            content_type="application/zip",
            status=200,
        )

        # Mock files
        mock_files = mock.mock_open()
        mock_zip_files = mock.MagicMock(
            side_effect=lambda *args, **kwargs: io.BytesIO(
                mock_zip_content_with_directory
            )
        )

        # We need to mock the current_cycle_year function directly to isolate the test
        with mock.patch(
            "contributions_pipeline.ingestion.federal.assets.get_current_cycle_year",
            return_value=year,
        ):
            # Act
            with (
                mock.patch("builtins.open", mock_files),
                mock.patch("io.open", mock_zip_files),
            ):
                fec_fetch_all_one_cycle_files(
                    start_year=year,  # Start at the same year as current year
                    fec_report_type_prefix=TEST_REPORT_PREFIX,
                    human_friendly_name="Test FEC Files with Directories",
                )

            # Assert
            # Verify directory creation for nested structure
            assert mock_makedirs.call_count >= 1

            # Verify zip file was written
            mock_files.assert_any_call(expected_temp_zip_path, "wb")

            # Verify temp zip file cleanup
            mock_os_remove.assert_called_with(expected_temp_zip_path)

    @mock.patch("psycopg_pool.ConnectionPool")
    @mock.patch("dagster.get_dagster_logger")
    @mock.patch(
        "contributions_pipeline.ingestion.federal.assets.get_current_cycle_year"
    )
    @mock.patch("os.path.exists")
    def test_fec_insert_files_to_landing_table(
        self, mock_path_exists, mock_current_year, mock_logger, mock_pool
    ):
        """Test the function that inserts FEC data into landing tables."""
        # Set up to process only one year
        mock_current_year.return_value = 2020  # Only process one cycle
        mock_logger.return_value = mock.MagicMock()

        # Create a mock cursor and mock transaction
        mock_connection = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_copy_cursor = mock.MagicMock()

        # Setup the cursor nesting
        mock_pool.connection.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.copy.return_value.__enter__.return_value = mock_copy_cursor

        # Set up to handle only one file, exists for 2020
        def path_exists_side_effect(path):
            return "2020" in path and "test.txt" in path

        mock_path_exists.side_effect = path_exists_side_effect

        # Mock file operations
        mock_file = mock.mock_open(
            read_data="|col1|col2|col3|col4|\n|val1|val2|val3|val4|"
        )

        # Create a custom CSV reader mock that returns specific rows
        class MockCsvReader:
            def __init__(self, data):
                self.data = data

            def __iter__(self):
                return iter(self.data)

        test_data = [
            ["col1", "col2", "col3", "col4"],
            ["val1", "val2", "val3", "val4"],
            ["bad", "row"],  # This is an invalid row that should be skipped
        ]

        with (
            mock.patch("builtins.open", mock_file),
            mock.patch("csv.reader", return_value=MockCsvReader(test_data)),
        ):
            # Define test parameters
            from contributions_pipeline.ingestion.federal.assets import (
                fec_insert_files_to_landing_of_one_type,
            )

            # Execute function under test
            fec_insert_files_to_landing_of_one_type(
                postgres_pool=mock_pool,
                start_year=2020,
                fec_report_type_prefix="test",
                data_file_path="/test.txt",
                table_name="test_table",
                table_columns_name=["col1", "col2", "col3", "col4"],
                data_validation_callback=lambda row: len(row) == 4,
            )

            # Verify truncate and count queries were executed
            assert (
                mock_cursor.execute.call_count == 2
            )  # Now called twice (truncate + count)

            # Check first call (truncate)
            truncate_arg = mock_cursor.execute.call_args_list[0][1]["query"]
            assert isinstance(truncate_arg, sql.Composed)
            assert "TRUNCATE" in str(truncate_arg)
            assert "test_table" in str(truncate_arg)

            # Check second call (count)
            count_arg = mock_cursor.execute.call_args_list[1][1]["query"]
            assert isinstance(count_arg, sql.Composed)
            assert "COUNT" in str(count_arg)
            assert "test_table" in str(count_arg)

            # Verify copy cursor was created with a SQL COPY query
            copy_arg = mock_cursor.copy.call_args[1]["statement"]
            assert isinstance(copy_arg, sql.Composed)
            assert "COPY" in str(copy_arg)
            assert "test_table" in str(copy_arg)
            # Check for binary format in the query
            assert "FORMAT BINARY" in str(copy_arg)

            # Verify the write_row was called
            assert mock_copy_cursor.write_row.call_count >= 1
            # Check that our valid row was written
            mock_copy_cursor.write_row.assert_any_call(["val1", "val2", "val3", "val4"])

    @mock.patch("psycopg_pool.ConnectionPool")
    @mock.patch("dagster.get_dagster_logger")
    @mock.patch(
        "contributions_pipeline.ingestion.federal.assets.get_current_cycle_year"
    )
    @mock.patch("os.path.exists")
    def test_fec_insert_files_to_landing_table_file_not_found(
        self, mock_path_exists, mock_current_year, mock_logger, mock_pool
    ):
        """Test handling of missing files during FEC data insertion."""
        # Set up to process only one year
        mock_current_year.return_value = 2020
        mock_logger.return_value = mock.MagicMock()

        # Create mock cursor objects
        mock_connection = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_copy_cursor = mock.MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.copy.return_value.__enter__.return_value = mock_copy_cursor

        # Simulate file not found
        mock_path_exists.return_value = False

        # Execute function under test
        from contributions_pipeline.ingestion.federal.assets import (
            fec_insert_files_to_landing_of_one_type,
        )

        # This should not raise an exception, as it should handle FileNotFoundError
        fec_insert_files_to_landing_of_one_type(
            postgres_pool=mock_pool,
            start_year=2020,
            fec_report_type_prefix="test",
            data_file_path="/missing_file.txt",
            table_name="test_table",
            table_columns_name=["col1", "col2", "col3", "col4"],
            data_validation_callback=lambda row: len(row) == 4,
        )

        # Verify truncate and count queries were still executed
        assert mock_cursor.execute.call_count == 2

        # Check first call (truncate)
        truncate_arg = mock_cursor.execute.call_args_list[0][1]["query"]
        assert "TRUNCATE" in str(truncate_arg)

        # Check second call (count)
        count_arg = mock_cursor.execute.call_args_list[1][1]["query"]
        assert "COUNT" in str(count_arg)

        # Verify no rows were written
        assert mock_copy_cursor.write_row.call_count == 0

        # Verify warning was logged
        mock_logger.return_value.warning.assert_any_call(
            "File for year 2020 is non-existent, ignoring..."
        )

    @mock.patch("psycopg_pool.ConnectionPool")
    @mock.patch("dagster.get_dagster_logger")
    @mock.patch(
        "contributions_pipeline.ingestion.federal.assets.get_current_cycle_year"
    )
    @mock.patch("builtins.open")
    @mock.patch("os.path.exists")
    def test_fec_insert_files_to_landing_table_exception(
        self, mock_path_exists, mock_open, mock_current_year, mock_logger, mock_pool
    ):
        """Test exception handling during FEC data insertion."""
        # Set up to process only one year
        mock_current_year.return_value = 2020
        mock_logger.return_value = mock.MagicMock()

        # Create mock cursor objects
        mock_connection = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_copy_cursor = mock.MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.copy.return_value.__enter__.return_value = mock_copy_cursor

        # Make the file exist but cause an error when reading
        mock_path_exists.return_value = True
        mock_open.side_effect = Exception("Test exception")

        # Execute function under test
        from contributions_pipeline.ingestion.federal.assets import (
            fec_insert_files_to_landing_of_one_type,
        )

        # Should raise the exception
        with pytest.raises(Exception, match="Test exception") as excinfo:
            fec_insert_files_to_landing_of_one_type(
                postgres_pool=mock_pool,
                start_year=2020,
                fec_report_type_prefix="test",
                data_file_path="/error_file.txt",
                table_name="test_table",
                table_columns_name=["col1", "col2", "col3", "col4"],
                data_validation_callback=lambda row: len(row) == 4,
            )

        assert "Test exception" in str(excinfo.value)

        # Verify error was logged
        mock_logger.return_value.error.assert_called_once()

    @mock.patch(
        "contributions_pipeline.ingestion.federal.assets.fec_insert_files_to_landing_of_one_type"
    )
    @mock.patch("dagster.get_dagster_logger")
    def test_fec_landing_table_assets(self, mock_logger, mock_insert):
        """Test the assets that insert FEC data into landing tables."""
        from contributions_pipeline.ingestion.federal.assets import (
            fec_candidate_master_insert_to_landing_table,
            fec_committee_master_insert_to_landing_table,
        )

        # Create mock PostgresResource
        mock_pg = mock.MagicMock()
        mock_pg.pool = "mock_connection_pool"

        # Test the committee master asset
        mock_insert.reset_mock()
        fec_committee_master_insert_to_landing_table(mock_pg)

        # Get the actual call arguments
        _args, kwargs = mock_insert.call_args

        # Check key parameters
        assert kwargs["postgres_pool"] == mock_pg.pool
        assert kwargs["start_year"] == FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE
        assert kwargs["fec_report_type_prefix"] == "cm"
        assert kwargs["data_file_path"] == "/cm.txt"
        assert kwargs["table_name"] == "federal_committee_master_landing"
        assert len(kwargs["table_columns_name"]) == 15

        # Test the candidate master asset
        mock_insert.reset_mock()
        fec_candidate_master_insert_to_landing_table(mock_pg)

        # Get the actual call arguments
        _args, kwargs = mock_insert.call_args

        # Check key parameters
        assert kwargs["postgres_pool"] == mock_pg.pool
        assert kwargs["start_year"] == FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE
        assert kwargs["fec_report_type_prefix"] == "cn"
        assert kwargs["data_file_path"] == "/cn.txt"
        assert kwargs["table_name"] == "federal_candidate_master_landing"
        assert len(kwargs["table_columns_name"]) == 15

    @mock.patch(
        "contributions_pipeline.ingestion.federal.assets.fec_fetch_all_one_cycle_files"
    )
    @mock.patch("dagster.get_dagster_logger")
    def test_dagster_assets(self, mock_logger, mock_fetch):
        """Test the Dagster asset definitions."""
        from contributions_pipeline.ingestion.federal.assets import (
            fec_candidate_master,
            fec_committee_master,
            # fec_individual_contributions,
        )

        # Test the individual contributions asset
        # fec_individual_contributions()
        # mock_fetch.assert_called_with(
        #     start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
        #     fec_report_type_prefix="indiv",
        #     human_friendly_name="individual contributions",
        # )

        # Test the committee master asset
        mock_fetch.reset_mock()
        fec_committee_master()
        mock_fetch.assert_called_with(
            start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
            fec_report_type_prefix="cm",
            human_friendly_name="commitee master",
        )

        # Test the candidate master asset
        mock_fetch.reset_mock()
        fec_candidate_master()
        mock_fetch.assert_called_with(
            start_year=FEC_FIRST_YEAR_OF_BULK_DATA_AVAILABLE,
            fec_report_type_prefix="cn",
            human_friendly_name="candidate master",
        )

    @responses.activate
    @mock.patch("pathlib.Path.mkdir")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists", side_effect=lambda _: True)
    def test_fetch_one_cycle_files_http_error(
        self, mock_os_exists, mock_os_remove, mock_mkdir, mock_zip_content
    ):
        """Test proper handling of HTTP errors when fetching FEC data."""
        # Arrange
        year = 2022
        expected_url = get_url_for_federal_bulk_download_one_cycle(
            year=year, prefix=TEST_REPORT_PREFIX
        )

        # Mock HTTP error response
        responses.add(
            method="GET",
            url=expected_url,
            body="Not Found",
            status=404,
        )

        # Act & Assert
        with pytest.raises(RuntimeError) as excinfo:
            fec_fetch_all_one_cycle_files(
                start_year=year,
                fec_report_type_prefix=TEST_REPORT_PREFIX,
                human_friendly_name="Test FEC Files",
            )

        # Verify error message contains helpful information
        assert "Could not get data for URL" in str(excinfo.value)

        # Verify HTTP request was attempted
        assert len(responses.calls) == 1
        assert responses.calls[0].request.url == expected_url

    @responses.activate
    @mock.patch("pathlib.Path.mkdir")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists", side_effect=lambda _: True)
    @mock.patch("os.makedirs", side_effect=lambda path, exist_ok=True: None)
    def test_fetch_multiple_cycles(
        self,
        mock_makedirs,
        mock_os_exists,
        mock_os_remove,
        mock_mkdir,
        mock_zip_content,
        mock_current_year,
    ):
        """Test downloading multiple cycles of FEC data."""
        # Make current year 2026 to define range
        mock_current_year.now.return_value.year = TEST_CURRENT_YEAR

        # Set up test for multiple years
        start_year = TEST_START_YEAR
        years_to_test = range(start_year, TEST_CURRENT_YEAR + 1, 2)  # Even years only

        # Mock HTTP responses for each year
        for year in years_to_test:
            url = get_url_for_federal_bulk_download_one_cycle(
                year=year, prefix=TEST_REPORT_PREFIX
            )
            responses.add(
                method="GET",
                url=url,
                body=mock_zip_content,
                content_type="application/zip",
                status=200,
            )

        # Mock files
        mock_files = mock.mock_open()
        mock_zip_files = mock.MagicMock(
            side_effect=lambda *args, **kwargs: io.BytesIO(mock_zip_content)
        )

        # Act
        with (
            mock.patch("builtins.open", mock_files),
            mock.patch("io.open", mock_zip_files),
        ):
            fec_fetch_all_one_cycle_files(
                start_year=start_year,
                fec_report_type_prefix=TEST_REPORT_PREFIX,
                human_friendly_name="Test Multiple FEC Files",
            )

        # Assert
        # Verify correct number of HTTP requests
        assert len(responses.calls) == len(list(years_to_test))

        # Verify correct URLs were requested
        for i, year in enumerate(years_to_test):
            expected_url = get_url_for_federal_bulk_download_one_cycle(
                year=year, prefix=TEST_REPORT_PREFIX
            )
            assert responses.calls[i].request.url == expected_url

        # Verify directory creation for each year
        assert mock_mkdir.call_count == len(list(years_to_test))

        # Verify temp file cleanup for each year
        assert mock_os_remove.call_count == len(list(years_to_test))

    @responses.activate
    @mock.patch("pathlib.Path.mkdir")
    @mock.patch("os.remove")
    @mock.patch("os.path.exists", side_effect=lambda _: True)
    @mock.patch("zipfile.ZipFile")
    @mock.patch("os.makedirs", side_effect=lambda path, exist_ok=True: None)
    def test_zipfile_extraction_error_handling(
        self,
        mock_makedirs,
        mock_zipfile,
        mock_os_exists,
        mock_os_remove,
        mock_mkdir,
        mock_zip_content,
    ):
        """Test error handling during zip extraction."""
        # Arrange
        year = 2024

        # Setup HTTP mock
        responses.add(
            method="GET",
            url=get_url_for_federal_bulk_download_one_cycle(
                year=year, prefix=TEST_REPORT_PREFIX
            ),
            body=mock_zip_content,
            content_type="application/zip",
            status=200,
        )

        # Mock an error during extraction
        mock_zipfile.side_effect = zipfile.BadZipFile("Invalid zip file")

        # Mock for file operations
        mock_files = mock.mock_open()

        # Act & Assert
        with (
            mock.patch("builtins.open", mock_files),
            pytest.raises(zipfile.BadZipFile),
        ):
            fec_fetch_all_one_cycle_files(
                start_year=year,
                fec_report_type_prefix=TEST_REPORT_PREFIX,
                human_friendly_name="Test Error Handling",
            )
