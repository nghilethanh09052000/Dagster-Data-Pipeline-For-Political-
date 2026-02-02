import dagster as dg

from .assets import (
    test_asset,
    test_playwright_chromium,
    test_playwright_webkit,
    test_resources,
)
from .federal import definitions as federal_defs
from .states.alabama import definitions as alabama_defs
from .states.alaska import definitions as alaska_defs
from .states.arizona import definitions as arizona_defs
from .states.arkansas import definitions as arkansas_defs
from .states.california import definitions as california_defs
from .states.colorado import definitions as colorado_defs
from .states.connecticut import definitions as connecticut_defs
from .states.delaware import definitions as delaware_defs
from .states.district_of_columbia import definitions as dc_defs
from .states.florida import definitions as florida_defs
from .states.georgia import definitions as georgia_defs
from .states.hawaii import definitions as hawaii_defs
from .states.idaho import definitions as idaho_defs
from .states.illinois import definitions as illinois_defs
from .states.indiana import definitions as indiana_defs
from .states.iowa import definitions as iowa_defs
from .states.kansas import definitions as kansas_defs
from .states.kentucky import definitions as kentucky_defs
from .states.louisiana import definitions as louisiana_defs
from .states.maine import definitions as maine_defs
from .states.maryland import definitions as maryland_defs
from .states.massachussets import definitions as massachussets_defs
from .states.michigan import definitions as michigan_defs
from .states.minnesota import definitions as minnesota_defs
from .states.mississippi import definitions as mississippi_defs
from .states.missouri import definitions as missouri_defs
from .states.montana import definitions as montana_defs
from .states.nebraska import definitions as nebraska_defs
from .states.nevada import definitions as nevada_defs
from .states.new_hampshire import definitions as new_hampshire_defs
from .states.new_jersey import definitions as new_jersey_defs
from .states.new_mexico import definitions as new_mexico_defs
from .states.new_york import definitions as new_york_defs
from .states.north_carolina import definitions as north_carolina_defs
from .states.north_dakota import definitions as north_dakota_defs
from .states.ohio import definitions as ohio_defs
from .states.oklahoma import definitions as oklahoma_defs
from .states.oregon import definitions as oregon_defs
from .states.pennsylvania import definitions as pennsylvania_defs
from .states.rhode_island import definitions as rhode_island_defs
from .states.south_carolina import definitions as south_carolina_defs
from .states.tennessee import definitions as tennessee_defs
from .states.texas import definitions as texas_defs
from .states.utah import definitions as utah_defs
from .states.vermont import definitions as vermont_defs
from .states.virginia import definitions as virginia_defs
from .states.washington import definitions as washington_defs
from .states.west_virginia import definitions as west_virginia_defs
from .states.wisconsin import definitions as wisconsin_defs
from .states.wyoming import definitions as wyoming_defs

defs = dg.Definitions.merge(
    dg.Definitions(
        assets=[
            test_asset,
            test_resources,
            test_playwright_chromium,
            test_playwright_webkit,
        ]
    ),
    federal_defs.defs,
    virginia_defs.defs,
    california_defs.defs,
    massachussets_defs.defs,
    minnesota_defs.defs,
    missouri_defs.defs,
    alabama_defs.defs,
    alaska_defs.defs,
    arkansas_defs.defs,
    arizona_defs.defs,
    west_virginia_defs.defs,
    washington_defs.defs,
    texas_defs.defs,
    oklahoma_defs.defs,
    pennsylvania_defs.defs,
    colorado_defs.defs,
    dc_defs.defs,
    georgia_defs.defs,
    florida_defs.defs,
    nebraska_defs.defs,
    new_hampshire_defs.defs,
    indiana_defs.defs,
    iowa_defs.defs,
    hawaii_defs.defs,
    wisconsin_defs.defs,
    tennessee_defs.defs,
    kansas_defs.defs,
    montana_defs.defs,
    kentucky_defs.defs,
    new_mexico_defs.defs,
    louisiana_defs.defs,
    connecticut_defs.defs,
    new_jersey_defs.defs,
    michigan_defs.defs,
    mississippi_defs.defs,
    south_carolina_defs.defs,
    north_dakota_defs.defs,
    new_york_defs.defs,
    utah_defs.defs,
    north_carolina_defs.defs,
    idaho_defs.defs,
    wyoming_defs.defs,
    delaware_defs.defs,
    maine_defs.defs,
    rhode_island_defs.defs,
    vermont_defs.defs,
    oregon_defs.defs,
    ohio_defs.defs,
    illinois_defs.defs,
    maryland_defs.defs,
    nevada_defs.defs,
)
