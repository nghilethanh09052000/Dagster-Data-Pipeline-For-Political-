import dagster as dg
from playwright.sync_api import sync_playwright

from contributions_pipeline.resources import PostgresResource


@dg.asset()
def test_asset(context: dg.AssetExecutionContext):
    context.log.info("It works!")


@dg.asset(deps=[test_asset], pool="pg")
def test_resources(
    context: dg.AssetExecutionContext, pg: dg.ResourceParam[PostgresResource]
):
    with pg.pool.connection() as conn:
        query = conn.execute("SELECT 1").fetchone()

        if query is None:
            context.log.error("Query failed!")
            return

        context.log.info(f"DB Query test result {query[0]}")


@dg.asset()
def test_playwright_chromium(context: dg.AssetExecutionContext):
    """
    Test on if Playwright w/ chromium works as intended on Dagster+

    The test will be as simple as
    1. Opening up "raisemore.app", it should redirect to "join.raisemore.app"
    2. Get the the webiste title
    """

    with sync_playwright() as pr:
        # Opt-in to new headless mode by using chromium channel
        browser = pr.chromium.launch(headless=True, channel="chromium")
        browser_context = browser.new_context()

        page = browser_context.new_page()

        context.log.info("Opening raisemore and waiting for network idle...")
        page.goto("https://raisemore.app")
        page.wait_for_load_state("networkidle")

        context.log.info(f"Page title are {page.title()}, closing...")

        browser_context.close()
        browser.close()


@dg.asset()
def test_playwright_webkit(context: dg.AssetExecutionContext):
    """
    Test on if Playwright w/ webkit works as intended on Dagster+

    The test will be as simple as
    1. Opening up "raisemore.app", it should redirect to "join.raisemore.app"
    2. Get the the webiste title
    """

    with sync_playwright() as pr:
        # Opt-in to new headless mode by using chromium channel
        browser = pr.webkit.launch(headless=True)
        browser_context = browser.new_context()

        page = browser_context.new_page()

        context.log.info("Opening raisemore and waiting for network idle...")
        page.goto(
            "https://raisemore.app",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        page.wait_for_timeout(2000)

        context.log.info(f"Page title are {page.title()}, closing...")

        browser_context.close()
        browser.close()
