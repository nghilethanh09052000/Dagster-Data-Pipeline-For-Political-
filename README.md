# Data Pipeline

## Setup

This project uses:

- `uv` for Python package management
- `ruff` for linting and formatting
- `pyright` for type checking
- `pre-commit` for running checks before committing
- `prisma` for database migration

### Initial Setup

1. Install uv, and nvm:

To install uv do the following command.

```bash
curl -sSf https://astral.sh/uv/install.sh | bash
```

To install nvm (for Node, because we're using prisma), do the following command.

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.2/install.sh | bash
```

2. Create a virtual environment and install dependencies:

To create virtual environment and install dependencies for Dagster, do the following.

```bash
cd pipeline
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

To install prisma (for database migration) and other JS toolchain dependencies, do the following.

```bash
nvm use
npm i
```

3. The pre-commit hooks are integrated with the project's husky setup.
   The hooks will run automatically with every commit, regardless of which files are changed.

### Proxy Setup

If you're testing out states that uses Proxy to get the automation working (some states only allows connection from IP from the same states).
There's an extra setup with the provider. **Below are states using proxy.**

1. New Hampshire (NH)
2. Vermont (VT)

To get the proxy working, do the following.

1. Login to the proxy provider (logins on the bottom of this README)
2. Check what is your IP address (beware that most ISP have dynamic IP, thus you might need to re-do this after a while)
3. Go to the state proxy
4. Add your IP to the whitelist (might as well remove your old one if you've added it before)

## Development

### Python Task

- Run linting and formatting manually:

  ```bash
  # Run ruff linter
  uv run ruff check
  
  # Run ruff formatter
  uv run ruff format
  
  # Run type checking
  uv run pyright
  ```

- Pre-commit will automatically run these checks before each commit

### Database Migrations

Database migrations are managed using Prisma:

- Apply migrations:

  ```bash
  make migrate
  ```

- Create a new migration:

  ```bash
  make new-migration name=your_migration_name
  ```

## Project Structure

Inspired by [concept structuring](https://docs.dagster.io/guides/build/projects/structuring-your-dagster-project#option-2-structured-by-concept) from Dagster docs.

To make the Dagster pipeline more structured, it has been arranged based on the concept (`ingestion`, `transformation`, feel free to add more!),
and also technology. Root folders is for typical Dagster assets, please add `dbt` (or other tech, like `dlt`) folder for DBT related assets.

Here's how the project structure is meant to be.

```text
.
└── contributions_pipeline/
    ├── ingestion/
    │   └── assets.py
    │   └── test_assets.py
    │   └── resources.py
    │   └── test_resources.py
    │   └── definitions.py
    │   └── dbt/
    │       ├── assets.py
    │       ├── test_assets.py
    │       ├── resources.py
    │       ├── test_resources.py
    │       └── definitions.py
    ├── transformation/
    │   ├── dbt/
    │   │   ├── assets.py
    │   │   ├── test_assets.py
    │   │   ├── resources.py
    │   │   ├── test_resources.py
    │   │   └── partitions.py
    └── resources.py
    └── definitions.py
└── prisma/
    └── schema.prisma
```

## Configuration

### Environment Variables

List of used environment variables on the pipeline.

| Name | Required | Example Value | Description |
| --------------- | --------------- | --------------- | --------------- |
| `PIPELINE_PG_CONNSTRING` | Yes |  `postgres://username:password@host:5433/database` | Standard Postgres connection string standard. Used for [`pg` resources](./contributions_pipeline/resources.py). Further reference can be found in [Postgres docs](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS) |

## Manual Ingest Process

To make some states work (hopefully only for v1 of the pipeline). There's some ingest process that needs to be done manually every month. Below should describe each state situation and operating procedure to make the state data most recent.

> [!WARNING]
> If you're adding state that needs some manual process.
> Please add the procedure to add the data here!
>
> Also please make sure that all schedules for it are either disabled, or doesn't have any schedule at all!

### New York

New York public reporting site requires a more advanced scraping method as it will throw `403` status code if scraped with `requests`/`httpx`. There's already a pipeline added to process the downloaded data, and the rest of the process needed. Below is the procedure to update NY data.

1. Open up [NY Public Reporting campaign data download page](https://publicreporting.elections.ny.gov/DownloadCampaignFinanceData/DownloadCampaignFinanceData). US VPN is not really required.
2. Download the zip archive file for each of the parameters
    - Data Type: `Filer Data`.
    - Data Type: `Disclosure Report`. Report Year: `All`
        1. Report Type: `State Candidate`
        2. Report Type: `State Committee`
        3. Report Type: `County Candidate`
        4. Report Type: `County Committee`
3. Upload each of the zip (without any modification) to Supabase Storage bucket `hardcoded-downloads`, folder `ny`.

### Virginia

Virginia site utilizes bot protection system. Thus we need to mirror data on their [bulk download site](https://apps.elections.virginia.gov/SBE_CSV/CF/) to our Supabase bucket. To make it easier to download all of the data for the whole year from 1999-now there's a script that will help with it, as with residential IP and non-server device it seems to still works. Below is the procedure to update VA data.

1. Download all the data using helper script using `make manual-dl state=va`
2. Upload all of the data without modification (on `./state/virginia`) to to Supabase Storage bucket `hardcoded-downloads`, folder `va`.

### Oregon

Oregon have this system where you can do request to the main page, but as you want to do any action (click button, send form, etc) it requires you to have CSRF token. To get that the token you need to do a query, which that endpoint (`POST https://secure.sos.state.or.us/orestar/JavaScriptServlet`) is bot protected. If you request the page with pre-existing CSRF token, it could result in the website sending you a captcha. To update the data, do the following.

1. Go to Oregon SOS [Orestar Transaction Search](https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do)
2. As the previous year are already meticulously manually scraped, all you need to do is update the current year data. So set the "Transaction Date" "From" to "01/01/{current year}" and "To" to "12/31/{current year}", e.g. from "01/01/2025" to "12/31/2025".
3. Click on the "Export To Excel Format" on the top of the table
4. Upload that ".xls" (without any modification) to Supabase Storage bucket `hardcoded-downloads`, folder `or/{current year}` (e.g. `or/2025`)

### Maryland

Simple to day, there's a anti-bot system that uses Captcha to make sure we could not scrape the site at all. The previous years are already uploaded to Supabase Storage, thus all you need to do to update it just to update the current year. To do it you can follow the process down below.

1. Go to [Maryland Campaign Reporting Information System](https://campaignfinance.maryland.gov/Public/ViewReceipts?theme=vista)
2. Adjust the search to the current year, on "Financial Information" part, put the start date as `01/01/{current year}` and end date as `12/31/{current year}` (e.g. if the current year is 2025, then start date is `01/01/2025` and end date is `12/31/2025`)
3. Click on the CSV icon on the left top side of the result table
4. Upload that ".csv" (without any modification) to Supabase Storage bucket `hardcoded-downloads`, folder `md/{current year}` (e.g. `md/2025`)

### Nevada

Only way to get the full data dump is to register and run reports for campaign finance, and believe me it's worth it. Though you do need to register to their system to get it. So first thing first, you need to do this one-time process of registration. To register simply go to [data download page](https://www.nvsos.gov/sos/online-services/data-download) and it should prompt you to login or register, just choose to register, and do the whole ordeal, it's not much at all though don't worry!

Now that you're registered, you can setup a report. To do so follow the following steps.

1. Go to [data download page](https://www.nvsos.gov/sos/online-services/data-download) and login if you haven't. If it asks you where you want the data, just choose to send link to your email
2. Click the green button name "Create New Report"
3. Select for "Campaign Finance", and for the Report Type choose "Full, Unabridged Database Dump"
4. Click "Next"
5. On file format, choose "CSV" and make sure to tick the "First row contains column names"
6. Then "Run Report & Save Settings", after that check your email and there should be a link

To update the data for Nevada, all you need to do is the following. If you already have a report, make sure to re-run the report and get the latest data.

1. Download the data given by the email, then unzip it
2. For the files, you need to rename the data to `CampaignFinance.Cnddt.csv`, `CampaignFinance.Grp.csv`, `CampaignFinance.Rpr.csv`, `CampaignFinance.Cntrbtrs.csv`, `CampaignFinance.Cntrbt.csv`, and `CampaignFinance.Expn.csv`. Or basically remove the IDs and time part of the file, just make sure all the files are named according to the list given.
3. Delete the previous file on Supabase Storage bucket `hardcoded-downloads`, folder `nv`.
4. Upload all the files on Supabase Storage bucket `hardcoded-downloads`, folder `nv`.

### Ohio

While all the download file is listed as being from "FTP", the actual download URL still actually goes through bot detection which includes a captcha to detect bot. This renders our system unable to access all of the files. Currently all the files is stored in Supabase. To update the file, follow the steps below.

1. Go to the [FTP bulk data download site](https://www6.ohiosos.gov/ords/f?p=119:73:0:101677873215429), or go to [search report page](https://www.ohiosos.gov/campaign-finance/search/) and click on "FTP SITE".
2. Download the following files,
    - On "Candidate Files"
        - Active Candidate List
        - Candidate Cover Pages
        - Candidate Contributions--{current year}
        - Candidate Expenditures--{current year}
    - On "PAC Files"
        - Active PAC List
        - PAC COVER PAGES
        - PAC Contributions--{current year}
        - PAC Expenditures--{current year}
    - On "Party Files"
        - Party Cover Pages
        - Party Contributions--{current year}
        - Party Expenditures--{current year}
3. Delete the previous file (only the one that has the same file name as the one you've downloaded) on Supabase Storage bucket `hardcoded-downloads`, folder `oh`, there should be sub-folder for each different file types as well (e.g. all "PAC Files" will be in `oh/pac_files`, etc).
4. Upload the file on the right sub-folder.

### Rhode Island

The contributions site Rhode Island uses Cloudflare Bot Proection, seems quite bit more strict than the other system that other states uses. To update the file do the following.

1. Get the latest contributions
    - Go to [Rhode Island ERTS Contributions Search](https://ricampaignfinance.com/RIPublic/Contributions.aspx).
    - Set the search date from the `01/01/{current_year}`, through `12/31/{current_year}`, and set `%` as the "Donor Last Name or Organization Name".
    - Click on "(Export Detail to comma delimited file)", **make sure you've allowed pop-ups on this page!**. Then click "View/Save" on the pop-up.
    - Save the file with the following format `contributions_{current_year}`
2. Get the latest expenditures
    - Go to [Rhode Island ERTS Expenditures Search](https://ricampaignfinance.com/RIPublic/Expenditures.aspx).
    - Set the search date from the `01/01/{current_year}`, through `12/31/{current_year}`, and set `%` as the "Payee Last Name or Organization Name".
    - Click on "(Export Detail to comma delimited file)", **make sure you've allowed pop-ups on this page!**. Then click "View/Save" on the pop-up.
    - Save the file with the following format `expenditures_{current_year}`
3. Go to Supabase Storage bucket `hardcoded-downloads` sub-folder `ri`.
4. Delete the files with the same name, and upload the one in.

## Logins

Below are shared services that's being used by the pipeline basically permanently.

