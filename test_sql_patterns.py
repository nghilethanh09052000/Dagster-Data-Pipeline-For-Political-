import glob
import os
import re


def find_sql_patterns(pattern, exclude_dirs=None):
    """
    Search for specific patterns in SQL files.

    Args:
        pattern: Regular expression pattern to search for
        exclude_dirs: List of directories to exclude (optional)

    Returns:
        List of tuples (file_path, line_number, matched_text)
    """
    if exclude_dirs is None:
        exclude_dirs = []

    # Convert exclude_dirs to absolute paths
    exclude_dirs = [os.path.abspath(path) for path in exclude_dirs]

    # Get the root directory of the project
    root_dir = os.path.dirname(os.path.abspath(__file__))

    # Initialize results list
    results = []

    # Compile the pattern
    compiled_pattern = re.compile(pattern, re.IGNORECASE)

    # Get all SQL files
    for file_path in glob.glob(f"{root_dir}/**/*.sql", recursive=True):
        # Skip files in excluded directories
        if any(
            os.path.abspath(file_path).startswith(excluded_dir)
            for excluded_dir in exclude_dirs
        ):
            continue

        try:
            with open(file_path, encoding="utf-8") as file:
                for line_num, line in enumerate(file, 1):
                    match = compiled_pattern.search(line)
                    if match:
                        results.append((file_path, line_num, match.group(0)))
        except UnicodeDecodeError:
            # Skip binary files
            pass

    return results


def test_no_if_exists_in_sql():
    """Ensure 'IF NOT EXISTS' is not used in SQL files in the pipeline directory"""
    # Pattern to check for
    pattern = r"IF\s+NOT\s+EXISTS"

    # Optional: directories to exclude
    exclude_dirs = ["node_modules", ".git"]

    # Find pattern matches
    found_matches = find_sql_patterns(pattern, exclude_dirs)

    # Filter out matches to only include those in the pipeline's SQL files
    pipeline_sql_matches = []
    for file_path, line_num, matched_text in found_matches:
        pipeline_sql_matches.append((file_path, line_num, matched_text))

    # Format error message if matches found
    if pipeline_sql_matches:
        error_msg = (
            "'IF NOT EXISTS' statements found in SQL files (excluding migrations):\n"
        )
        for file_path, line_num, matched_text in pipeline_sql_matches:
            # Get relative path for clearer output
            rel_path = os.path.relpath(
                file_path, os.path.dirname(os.path.abspath(__file__))
            )
            error_msg += f"- '{matched_text}' found in {rel_path}:L{line_num}\n"

        # Fail test with detailed error message
        raise AssertionError(error_msg)
