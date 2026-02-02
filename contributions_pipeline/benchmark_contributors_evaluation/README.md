# Benchmark Contributors Evaluation Module

This module evaluates data pipeline accuracy by comparing hand-researched ground truth contributions against the `individual_contributions` table.

## Overview

The benchmark process:
1. **Ground Truth Collection**: Manually research 100 donations per state (2020-2025) from official sources
2. **Storage**: Store in `ground_truth_contributions_manually_gathered_sample` table
3. **Measurement**: Dagster job compares ground truth against `individual_contributions`
4. **Reporting**: Results sent to Slack with match rates by state

## Database Table

### `ground_truth_contributions_manually_gathered_sample`

Fields:
- `id` (SERIAL PRIMARY KEY)
- `state` (TEXT) - State code (CA, TX, NY, etc.)
- `fname` (TEXT) - First name
- `lname` (TEXT) - Last name
- `zip` (TEXT) - ZIP code (5 digits)
- `name_concat` (TEXT, GENERATED) - Auto-calculated: `UPPER(fname|last|zip_first_5)`
- `amount` (NUMERIC) - Donation amount
- `date` (DATE) - Donation date
- `source_url` (TEXT, optional) - URL where data was found
- `notes` (TEXT, optional) - Additional notes
- `created_at` (TIMESTAMP) - When record was created

## How to Use

### Step 1: Manually Research Ground Truth Data

1. Download official state contribution data (e.g., Cal-Access for CA)
2. Manually select 100 donations from 2020-2025
3. Record in Google Sheet or CSV with columns: `state`, `fname`, `lname`, `zip`, `amount`, `date`, `source_url`, `notes`

### Step 2: Load Data into Database

```sql
-- Load from CSV
COPY ground_truth_contributions_manually_gathered_sample (
    state, fname, lname, zip, amount, date, source_url, notes
)
FROM '/path/to/ground_truth_data.csv'
WITH (FORMAT csv, HEADER true);
```

### Step 3: Run Dagster Job

The job runs automatically on schedule (daily at 2 AM ET) or can be triggered manually:

```bash
dagster job execute benchmark_contributors_evaluation_job
```

### Step 4: View Results

Results are:
- Logged in Dagster UI
- Sent to Slack channel `#dagster-reports`
- Available as asset outputs in Dagster

## Assets

1. **`benchmark_match_rate_by_state`**: Calculates match rate by state for all contributions
2. **`benchmark_match_rate_by_state_unique_people`**: Calculates match rate by state for unique people
3. **`send_benchmark_report_to_slack`**: Sends formatted report to Slack

## Match Rate Calculation

Match rate = (Number of ground truth contributions that match `individual_contributions`) / (Total ground truth contributions) × 100%

Matching is done via `name_concat`:
- Ground truth: `UPPER(fname|last|zip_first_5)`
- Pipeline: `individual_contributions.name_concat`

## Status Levels

- **High Confidence**: ≥80% match rate
- **Medium Confidence**: 50-79% match rate
- **Low Confidence**: 30-49% match rate
- **Very Low Confidence**: <30% match rate

## App-Side Monitoring (Inngest Job)

After importing ground truth contacts into the synthetic org, populate the state mapping table:

1. **Export state mapping from pipeline DB:**
   ```sql
   SELECT DISTINCT name_concat, state
   FROM ground_truth_contributions_manually_gathered_sample
   ORDER BY state, name_concat;
   ```

2. **Import into app DB mapping table:**
   ```sql
   INSERT INTO benchmark_people_state_mapping (name_concat, state, organization_id)
   VALUES 
       ('SMITH|JOHN|90210', 'CA', 'synthetic-org-id'),
       ('DOE|JANE|90211', 'CA', 'synthetic-org-id'),
       ...
   ON CONFLICT (name_concat, organization_id) DO UPDATE SET
       state = EXCLUDED.state;
   ```

3. **Inngest job runs daily** at 3 AM ET to monitor match rates from app side

## Notes

- Ground truth data should NOT be sampled from `individual_contributions` or any pipeline table
- Data must be manually researched from official state sources
- Start with 1 state (recommended: California) before expanding
- Remember to populate `benchmark_people_state_mapping` table after importing contacts

