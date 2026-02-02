"""
This file is used to host many different states download script that works locally but
might got blocked/not work on Dagster+ (Dagster cloud we're using) yet.

You can run the script here by using `make manual-dl state=<state code>`.
"""

import logging
import sys

from contributions_pipeline.ingestion.states.virginia.assets import (
    va_fetch_all_files_of_all_report_type_all_time,
)


def va():
    """
    This function just calls the va fetch and fetch all the files from VA elections.
    The output of the result will be in `./states/va/...`.
    """
    logging.info("Starting to download VA data!")

    va_fetch_all_files_of_all_report_type_all_time(
        base_url="https://apps.elections.virginia.gov/SBE_CSV/CF/", report_type="State"
    )
    va_fetch_all_files_of_all_report_type_all_time(
        base_url="https://apps.elections.virginia.gov/SBE_CSV/StatementsOfOrganization/",
        report_type="Candidate",
    )

    logging.info("VA data download done, check ./states/virginia for the result")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # NOTE: no function should really have arguments currently. Also this might not be
    # the safest, but it's for local usage anyway through controlled makefile either.
    globals()[sys.argv[1]]()
