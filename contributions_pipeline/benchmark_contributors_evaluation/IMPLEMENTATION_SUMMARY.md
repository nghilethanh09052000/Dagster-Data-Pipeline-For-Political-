# Benchmark Evaluation Implementation Summary

## ✅ Completed Implementation

### 1. Pipeline Side (Dagster)

**Location**: `pipeline/contributions_pipeline/benchmark_contributors_evaluation/`

**Files Created**:
- `assets.py` - Dagster assets for benchmark calculation
- `definitions.py` - Job and schedule definitions
- `README.md` - Documentation
- `ground_truth_template.csv` - CSV template for data entry

**Database Migration**:
- `pipeline/prisma/migrations/20260125122012_add_ground_truth_contributions_table/migration.sql`
- Creates `ground_truth_contributions_manually_gathered_sample` table

**Features**:
- ✅ Compares ground truth → `individual_contributions` (pipeline DB)
- ✅ Calculates match rate by state
- ✅ Calculates match rate by state for unique people
- ✅ Sends results to Slack (`#dagster-reports`)
- ✅ Runs daily at 2 AM ET
- ✅ Status classification (High/Medium/Low/Very Low Confidence)

### 2. App Side (Inngest)

**Location**: `src/app/api/cron/benchmarkMonitoring.ts`

**Database Migration**:
- `prisma/migrations/20260125122809_add_benchmark_state_mapping/migration.sql`
- Creates `benchmark_people_state_mapping` table for state tracking

**Features**:
- ✅ Queries people from `synthetic-org-data-monitor` organization
- ✅ Compares against app `individual_contributions` table
- ✅ Calculates match rate by state
- ✅ Calculates match rate by state for unique people
- ✅ Runs daily at 3 AM ET (after pipeline job)
- ✅ Logs results

**Registration**:
- ✅ Added to `src/app/api/inngest/route.route.ts`

## 📋 Next Steps

### Immediate Actions:

1. **Run Migrations**:
   ```bash
   # Pipeline DB
   cd pipeline && make migrate
   
   # App DB  
   npx prisma migrate deploy
   ```

2. **Manually Research Ground Truth Data**:
   - Start with California (100 donations from 2020-2025)
   - Use Cal-Access source: https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip
   - Record in Google Sheet or CSV
   - Load into `ground_truth_contributions_manually_gathered_sample` table

3. **Import into Synthetic Org**:
   - Export unique fname, lname, zip from ground truth
   - Import into `synthetic-org-data-monitor` org via frontend
   - Populate `benchmark_people_state_mapping` table (see `populate_state_mapping.sql`)

4. **Test Jobs**:
   - Test Dagster job: `dagster job execute benchmark_contributors_evaluation_job`
   - Test Inngest job: Trigger manually or wait for scheduled run

## 🔄 Workflow

```
1. Research → 2. Load to Pipeline DB → 3. Import to App → 4. Populate Mapping → 5. Jobs Run
```

## 📊 Two Monitoring Systems

### Pipeline Monitoring (Dagster)
- **Source**: `ground_truth_contributions_manually_gathered_sample` (pipeline DB)
- **Target**: `individual_contributions` (pipeline DB)
- **Purpose**: Measure pipeline data quality
- **Schedule**: Daily at 2 AM ET

### App Monitoring (Inngest)
- **Source**: `people` table in `synthetic-org-data-monitor` org (app DB)
- **Target**: `individual_contributions` (app DB)
- **Purpose**: Measure app-side data quality (what went wrong between pipeline and app)
- **Schedule**: Daily at 3 AM ET

## 🎯 Success Criteria

- [x] Pipeline Dagster job created and registered
- [x] App Inngest job created and registered
- [x] Database tables created
- [ ] Ground truth data researched and loaded (100 CA donations)
- [ ] Data imported into synthetic org
- [ ] State mapping populated
- [ ] Jobs tested and running
- [ ] Reports generated successfully

## 📝 Notes

- Both jobs are ready to run once ground truth data is loaded
- Jobs will handle empty data gracefully (log warnings)
- State mapping is optional but recommended for accurate state-by-state reporting
- Start with 1 state (CA) before expanding to others


