import glob
import os
import re


def find_typos_in_files(typos_list, file_extensions, exclude_dirs=None):
    """
    Search for typos in files with specified extensions.

    Args:
        typos_list: List of typo strings to search for
        file_extensions: List of file extensions to check (e.g., ['py', 'sql'])
        exclude_dirs: List of directories to exclude (optional)

    Returns:
        List of tuples (file_path, line_number, typo_found)
    """
    if exclude_dirs is None:
        exclude_dirs = []

    # Convert exclude_dirs to absolute paths
    exclude_dirs = [os.path.abspath(path) for path in exclude_dirs]

    # Get the root directory of the project
    root_dir = os.path.dirname(os.path.abspath(__file__))

    # Initialize results list
    results = []

    # Create patterns for each typo
    patterns = [re.compile(rf"\b{typo}\b", re.IGNORECASE) for typo in typos_list]

    # Get all files with specified extensions
    for ext in file_extensions:
        for file_path in glob.glob(f"{root_dir}/**/*.{ext}", recursive=True):
            # Skip files in excluded directories
            if any(
                os.path.abspath(file_path).startswith(excluded_dir)
                for excluded_dir in exclude_dirs
            ):
                continue

            try:
                with open(file_path, encoding="utf-8") as file:
                    for line_num, line in enumerate(file, 1):
                        for i, pattern in enumerate(patterns):
                            if pattern.search(line):
                                results.append((file_path, line_num, typos_list[i]))
            except UnicodeDecodeError:
                # Skip binary files
                pass

    return results


def test_no_typos_in_code():
    """Ensure common typos are not present in the codebase"""
    # List of typos to check for
    typos = ["refersh"]

    # File extensions to check
    extensions = ["py", "sql"]

    # Optional: directories to exclude
    exclude_dirs = ["node_modules", ".git"]

    # Current file path - we need to exclude this test file itself
    current_file = os.path.abspath(__file__)

    # Find typos
    found_typos = find_typos_in_files(typos, extensions, exclude_dirs)

    # Filter out results from this test file
    found_typos = [
        result for result in found_typos if os.path.abspath(result[0]) != current_file
    ]

    # Format error message if typos found
    if found_typos:
        error_msg = "Typos found in codebase:\n"
        for file_path, line_num, typo in found_typos:
            # Get relative path for clearer output
            rel_path = os.path.relpath(
                file_path, os.path.dirname(os.path.abspath(__file__))
            )
            error_msg += f"- '{typo}' found in {rel_path}:L{line_num}\n"

        # Fail test with detailed error message
        raise AssertionError(error_msg)
