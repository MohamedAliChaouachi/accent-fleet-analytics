# M11 · Scheduled runs — cron on the Azure VM

Once M1–M7 are green and `incremental_flow()` runs cleanly from `06_deployment/01_incremental_mode_demo.ipynb`, move the orchestration off your laptop and onto the VM.

## 1. Deploy the code on the VM

```bash
ssh azureuser@<vm-public-ip>
sudo mkdir -p /opt/accent-fleet-analytics
sudo chown azureuser:azureuser /opt/accent-fleet-analytics
cd /opt/accent-fleet-analytics
git clone git@github.com:<your-org>/accent-fleet-analytics.git .
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Create a `.env` on the VM (use a dedicated service account, **not** `alice` or `bob`):

```ini
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=accent_fleet
PG_USER=accent_pipeline
PG_PASSWORD=<service-account-password>
PG_SCHEMA_STAGING=staging
PG_SCHEMA_WAREHOUSE=warehouse
PG_SCHEMA_MARTS=marts

PIPELINE_ENV=prod
PIPELINE_LOG_LEVEL=INFO
PIPELINE_BATCH_SIZE=50000
PIPELINE_OVERLAP_MINUTES=10
PIPELINE_INCREMENTAL_LOOKBACK_MINUTES=5
```

## 2. Install the cron entry

```bash
sudo nano /etc/cron.d/accent-fleet
```

```cron
# Run every 5 minutes. Log to syslog with tag accent-fleet.
*/5 * * * * azureuser cd /opt/accent-fleet-analytics && /opt/accent-fleet-analytics/.venv/bin/python scripts/run_batch.py --mode incremental 2>&1 | logger -t accent-fleet
```

`run_batch.py --mode incremental` is the **only** mode that should run on a schedule. Bootstrap and backfill are one-offs driven from the notebooks.

## 3. Observability

Tail the logs:

```bash
sudo journalctl -t accent-fleet -f
```

Or query the run log directly:

```sql
SELECT run_id, mode, status, started_at, finished_at,
       rows_loaded, error_message
FROM warehouse.etl_run_log
ORDER BY started_at DESC LIMIT 20;
```

Any row with `status = 'failed'` should trigger an alert (Slack webhook / email) — see `src/accent_fleet/monitoring/` for the hook point.

## 4. Rollback recipe

If a bad release starts polluting the warehouse, either:

1. **Fast path:** disable the cron (`sudo rm /etc/cron.d/accent-fleet`), fix the issue in a feature branch, merge, pull on the VM, re-enable.
2. **Data path:** the watermark cannot rewind by design. If data is corrupted, truncate the affected fact table for a specific month range and re-run `scripts/run_batch.py --mode backfill` over that window. Do **not** reset the watermark manually — the UPSERT on natural keys handles the replay safely.
