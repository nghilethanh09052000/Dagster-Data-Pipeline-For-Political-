"""
Aggregation of all defintions from all of the data pipeline.

If you've added a new defintions (based on concept and technology) add the defnitions
here. To see an example on how to create a defnitions, check the already existing
defintions.
"""

import dagster as dg

# Make sure to not import the defs vars directly. As it will
# triggers "Multiple Defnitions Error"
from . import resources as resources
from .benchmark_contributors_evaluation import (
    definitions as benchmark_contributors_evaluation,
)
from .ingestion import definitions as ingestion
from .move_to_application_db import definitions as move_to_application_db
from .reports import definitions as reports
from .transformation import definitions as transformation

defs = dg.Definitions.merge(
    ingestion.defs,
    transformation.defs,
    move_to_application_db.defs,
    resources.defs,
    reports.defs,
    benchmark_contributors_evaluation.defs,
)
