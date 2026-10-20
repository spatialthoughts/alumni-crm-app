# Deployment plan: Cloud Run + IAP + Litestream

Status: **plan only, nothing built yet.** Written 2026-10-20.

Host the Flask app as it is on Google Cloud Run, behind Identity-Aware Proxy (IAP) for
Google login, with the SQLite database persisted to Cloud Storage by Litestream.
Target cost: ~$0/month (inside the free tiers).

## Why this shape

- **The app has no login and has write endpoints** (edit, merge, CSV upload). Login is
  enforced by Google in front of the app (IAP), not by code we write and maintain.
  IAP is enabled directly on the Cloud Run service — no load balancer, which used to
  cost ~$18/month. It protects the `*.run.app` URL itself, so there is no way around it.
- **Cloud Run containers have no persistent disk.** SQLite must not live on a Cloud
  Storage FUSE mount (no POSIX locking). Instead Litestream restores `crm.db` from a
  bucket at container start and streams every committed change back to it (~1 s lag).
- **Exactly one writer.** `--max-instances 1` keeps a single container, so a single
  Litestream process, writing to the replica.

Alternatives considered and rejected: static Hugo + Drive API (rewrite), YAML in the
website repo (loses forms), Fly.io (we'd own the OAuth code), e2-micro VM + Cloudflare
Access (OS to patch; DNS is still on WordPress.com nameservers).

## Architecture

```
browser ──HTTPS──> IAP (Google login, allowlist) ──> Cloud Run service "crm"
                                                     max 1 instance, scale to zero
                                                     ┌─────────────────────────────┐
                                                     │ litestream replicate -exec  │
                                                     │   gunicorn app:app          │
                                                     │ /data/crm.db  (in-memory fs)│
                                                     └──────────────┬──────────────┘
                                                                    │ WAL frames, ~1 s
                                                                    ▼
                                                  gs://<bucket>/crm.db  (versioned)
```

## Decisions to confirm before building

| # | Decision | Proposed |
|---|---|---|
| D1 | GCP project | New project `st-alumni-crm` under the spatialthoughts.com organization |
| D2 | Region | `us-central1` (Tier 1 pricing, free-tier eligible) |
| D3 | Who can log in | Only `ujaval@spatialthoughts.com` initially. Anyone outside the Workspace org (e.g. a TA's gmail) needs a custom OAuth client — see step 6b |
| D4 | URL | The `https://crm-<hash>-uc.a.run.app` URL. A custom domain needs DNS changes, which are still on WordPress.com |
| D5 | Canonical DB | The bucket becomes the only source of truth. The Google Drive copy (`$CRM_DB`) is frozen at cutover as a backup and then no longer written |
| D6 | Monthly backups | A Cloud Run job, triggered by Cloud Scheduler on the 1st of each month, writes a full, integrity-checked copy to a separate backup bucket and keeps 24 months (B11). An optional extra copy goes to Google Drive (B11e) |

## Part A — Code changes in this repo (small; routes and templates untouched)

1. **`Dockerfile`**
   - `python:3.11-slim`, install `flask` + `gunicorn` only. `requirements.txt` also pulls
     pandas, matplotlib and cartopy for the offline scripts, so split it into
     `requirements.txt` (app runtime) and `requirements-scripts.txt` (import/map scripts).
     `app.py` imports `import_crm`, which imports pandas at module level — either move the
     helpers the app uses (`clean_str`, `parse_date`, `upsert_*`, `ensure_email`) into a
     pandas-free module, or accept pandas in the image (~+60 MB; it's fine but slower cold
     starts). **Proposed:** move them to `crm_helpers.py`, have both `app.py` and
     `import_crm.py` import from it, and keep `parse_date`'s behaviour identical.
   - Install a pinned Litestream release binary (check the current release and its
     config syntax at build time; pin the exact version).
   - `ENV CRM_DB=/data/crm.db`.
   - Entrypoint script `run.sh`:
     ```sh
     litestream restore -if-db-not-exists -if-replica-exists "$CRM_DB"
     exec litestream replicate -exec "gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app"
     ```
     `-exec` makes Litestream run gunicorn as a child process, and on SIGTERM it does a final
     sync before exiting.
2. **`litestream.yml`**: one db (`/data/crm.db`), one GCS replica
   (`gcs://${LITESTREAM_BUCKET}/crm.db`). Credentials come from the Cloud Run service
   account — no key files.
3. **`db.py` → `get_db()`**: add `PRAGMA busy_timeout = 5000`. Set
   `PRAGMA journal_mode = WAL` once on the seeded file (it persists in the file); Litestream
   requires WAL.
4. **CSRF guard in `app.py`** (`before_request`, ~6 lines): reject a `POST` whose `Origin`
   header is missing or whose host is not `request.host`. That blocks cross-site form posts
   to the 9 POST forms without touching any template. (IAP stops strangers, but a page on
   another site could still submit a form using your logged-in IAP cookie.)
5. **Optional, show who is logged in**: read the `X-Goog-Authenticated-User-Email` header IAP
   adds and show it in the navbar. Do not use it for authorization. IAP has already done that.
6. **`.dockerignore`**: `*.db`, `.git`, `static/screenshots/`, `__pycache__/`. This keeps
   `test.db` and any stray real DB out of the image.
7. **`deploy.sh`**: the `gcloud run deploy` command from step B7, with the project, region
   and bucket as variables.
8. **README**: a "Deployment" section that points to this file; describe the local
   workflow with `CRM_DB` as development-only.
9. **`backup.py`** (monthly backup job, run by the same image with a different command):
   1. `litestream restore -o /tmp/crm.db gcs://${LITESTREAM_BUCKET}/crm.db`. It only reads
      the replica, so it is safe while the app is running.
   2. Open the restored file with `sqlite3` and run `PRAGMA integrity_check`; it must
      return `ok`. Record the row counts of the six tables.
   3. Upload it to `gs://${BACKUP_BUCKET}/crm-YYYY-MM-DDTHHMMSSZ.db` with the row counts
      as object metadata. The timestamp makes every name unique, so the job only ever
      creates objects and never needs overwrite or delete permission.
   4. Exit non-zero on any failure, so the job execution shows as failed.

   This adds `google-cloud-storage` to the image's runtime requirements.

Local test before any cloud work: `docker build` + `docker run` with a local-file
Litestream replica (`file:///tmp/replica`). Check that it restores, an edit survives a
container restart, and the CSV upload works.

## Part B — Google Cloud configuration (one-time)

Run as `ujaval@spatialthoughts.com`. `PROJECT_ID`, `PROJECT_NUMBER`, `REGION`, `BUCKET`
are placeholders.

### B1. Project and billing

```sh
gcloud projects create st-alumni-crm --organization=<ORG_ID>   # gcloud organizations list
gcloud billing projects link st-alumni-crm --billing-account=<BILLING_ACCOUNT_ID>
gcloud config set project st-alumni-crm
gcloud config set run/region us-central1
```

Being under the org lets IAP use the Google-managed OAuth client for org users, so no
consent-screen setup is needed (D3).

### B2. Budget alert (do this before anything can cost money)

Billing → Budgets & alerts → a $1/month budget for this project, with alerts at 50%, 90% and 100%
to your email. (Or `gcloud billing budgets create ...`.)

### B3. Enable APIs

```sh
gcloud services enable run.googleapis.com iap.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com storage.googleapis.com
```

### B4. Bucket for the Litestream replica

```sh
gcloud storage buckets create gs://st-alumni-crm-db \
  --location=us-central1 --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://st-alumni-crm-db --versioning
```

- Single-region `us-central1` keeps it in the free tier (5 GB-months Standard).
- Add a lifecycle rule to delete noncurrent object versions after 30 days, so versioning
  doesn't grow without limit. Litestream also keeps its own snapshots and retention.

### B5. Runtime service account (least privilege)

```sh
gcloud iam service-accounts create crm-run --display-name="Alumni CRM runtime"
gcloud storage buckets add-iam-policy-binding gs://st-alumni-crm-db \
  --member=serviceAccount:crm-run@st-alumni-crm.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin
```

This grant is on the bucket only, not the project. Don't use the default compute service
account, which has Editor on the whole project.

### B6. Seed the replica from the current database

1. Close the local Flask app. Nothing may write to the Drive copy from here on.
2. Copy `$CRM_DB` to a scratch path, then `sqlite3 copy.db "PRAGMA journal_mode=WAL;"`
   and `PRAGMA integrity_check;`.
3. Run Litestream locally against the copy (authenticated with
   `gcloud auth application-default login`) until it reports the first snapshot to
   `gcs://st-alumni-crm-db/crm.db`, then stop it.
4. Check: `litestream restore -o /tmp/check.db gcs://st-alumni-crm-db/crm.db`, then compare
   the row counts with the original (people 1321, enrollments 2001, batches 140, courses 18,
   certifications 1335 as of 2026-10-20).

### B7. Deploy with IAP on

```sh
gcloud run deploy crm \
  --source . \
  --region=us-central1 \
  --service-account=crm-run@st-alumni-crm.iam.gserviceaccount.com \
  --no-allow-unauthenticated \
  --iap \
  --min-instances=0 --max-instances=1 \
  --no-cpu-throttling \
  --cpu=1 --memory=512Mi \
  --execution-environment=gen2 \
  --set-env-vars=LITESTREAM_BUCKET=st-alumni-crm-db
```

- `--no-cpu-throttling` (instance-based billing) is **required**. Without it the CPU is
  throttled between requests and Litestream can't stream changes. The free tier for this
  mode is ~240k vCPU-seconds/month (~66 hours of uptime); the instance stops ~15 min
  after the last request.
- `--max-instances=1` is required for a single writer.
- `--source .` builds with Cloud Build and stores the image in Artifact Registry
  (`cloud-run-source-deploy`). Both are inside their free tiers at this size. Add a
  cleanup policy to Artifact Registry so old images are deleted.

### B8. Let IAP invoke the service

```sh
gcloud beta services identity create --service=iap.googleapis.com --project=st-alumni-crm
gcloud run services add-iam-policy-binding crm --region=us-central1 \
  --member=serviceAccount:service-<PROJECT_NUMBER>@gcp-sa-iap.iam.gserviceaccount.com \
  --role=roles/run.invoker
```

If the first IAP enable fails with "Setting IAM permissions failed", redeploy after this step.

### B9. Grant login access

```sh
gcloud iap web add-iam-policy-binding \
  --member=user:ujaval@spatialthoughts.com \
  --role=roles/iap.httpsResourceAccessor \
  --region=us-central1 --resource-type=cloud-run --service=crm
```

Repeat for each person. Remove someone with `remove-iam-policy-binding`; list with
`get-iam-policy` (same flags). Granting `domain:spatialthoughts.com` would admit every
account in the Workspace, so grant individuals instead.

#### B9b. Only if someone outside the Workspace org needs access

Create a custom OAuth client (consent screen audience **External**, redirect URI
`https://iap.googleapis.com/v1/oauth/clientIds/<CLIENT_ID>:handleRedirect`), then apply it
with an `iap_settings.yaml` (`oauth_settings.client_id/client_secret`) via
`gcloud iap settings set iap_settings.yaml --project=st-alumni-crm`. Skip this until it's
needed.

### B10. Verify

- `gcloud run services describe crm --region=us-central1` shows `Iap Enabled: true`.
- In a private browser window, the URL asks for Google login. A non-granted account gets
  "You don't have access".
- `curl -i <url>` without a login returns a redirect to the login page or 401/403, never
  app HTML.
- Edit a test person's notes, then force a new instance (`gcloud run services update crm
  --update-env-vars=BUMP=1`) and confirm the edit is still there.
- Check that the replica in the bucket changes after an edit (`gcloud storage ls -l gs://st-alumni-crm-db/crm.db/`).
- Billing → Reports after a few days: the total should be $0.00.

### B11. Monthly backups

Monthly snapshots go to a second bucket that the app can't write to. A bug, a bad merge,
or a corrupted Litestream replica can't reach them.

**B11a. Backup bucket** (Nearline storage class: cheap to store, 30-day minimum, which
doesn't matter for monthly files):

```sh
gcloud storage buckets create gs://st-alumni-crm-backups \
  --location=us-central1 --default-storage-class=NEARLINE \
  --uniform-bucket-level-access --public-access-prevention
# Keep 24 months: lifecycle rule deleting objects older than 730 days
cat > /tmp/lifecycle.json <<'EOF'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 730}}]}
EOF
gcloud storage buckets update gs://st-alumni-crm-backups --lifecycle-file=/tmp/lifecycle.json
# Optional: a bucket retention policy, so nobody (including you) can delete a backup
# before it's 30 days old
gcloud storage buckets update gs://st-alumni-crm-backups --retention-period=30d
```

**B11b. Backup service account.** It can read the replica and create backups, and nothing
else:

```sh
gcloud iam service-accounts create crm-backup --display-name="Alumni CRM monthly backup"
gcloud storage buckets add-iam-policy-binding gs://st-alumni-crm-db \
  --member=serviceAccount:crm-backup@st-alumni-crm.iam.gserviceaccount.com \
  --role=roles/storage.objectViewer
gcloud storage buckets add-iam-policy-binding gs://st-alumni-crm-backups \
  --member=serviceAccount:crm-backup@st-alumni-crm.iam.gserviceaccount.com \
  --role=roles/storage.objectCreator
```

`objectCreator` can't overwrite or delete, so even a broken job can't damage earlier
backups. The app's `crm-run` account gets no access to this bucket.

**B11c. The Cloud Run job** (the same image as the service):

```sh
gcloud services enable cloudscheduler.googleapis.com
gcloud run jobs deploy crm-backup \
  --image=<IMAGE_URL of the current crm revision> \
  --region=us-central1 \
  --service-account=crm-backup@st-alumni-crm.iam.gserviceaccount.com \
  --command=python --args=backup.py \
  --set-env-vars=LITESTREAM_BUCKET=st-alumni-crm-db,BACKUP_BUCKET=st-alumni-crm-backups \
  --max-retries=2 --task-timeout=10m --memory=512Mi
gcloud run jobs execute crm-backup --region=us-central1 --wait   # first run by hand
```

`deploy.sh` should update the job's image whenever it deploys the service, so the two
never drift apart.

**B11d. Schedule it** (03:00 UTC on the 1st of each month):

```sh
gcloud iam service-accounts create crm-scheduler --display-name="Alumni CRM scheduler"
gcloud run jobs add-iam-policy-binding crm-backup --region=us-central1 \
  --member=serviceAccount:crm-scheduler@st-alumni-crm.iam.gserviceaccount.com \
  --role=roles/run.invoker
gcloud scheduler jobs create http crm-monthly-backup \
  --location=us-central1 \
  --schedule="0 3 1 * *" --time-zone="Etc/UTC" \
  --http-method=POST \
  --uri="https://run.googleapis.com/v2/projects/st-alumni-crm/locations/us-central1/jobs/crm-backup:run" \
  --oauth-service-account-email=crm-scheduler@st-alumni-crm.iam.gserviceaccount.com
```

To be told when a run fails: in Cloud Monitoring, create a log-based alert on
`resource.type="cloud_run_job" AND resource.labels.job_name="crm-backup" AND severity>=ERROR`
that emails you. Without this, a failing backup goes unnoticed.

To restore from a monthly backup: `gcloud storage cp gs://st-alumni-crm-backups/<file> ./`
and open it locally. To put it back into production, wipe the replica path, re-seed with
B6 from that file, and redeploy while the app is idle.

**B11e. Optional: also keep a copy on Google Drive.** Service accounts have no Drive storage
of their own, so pushing to Drive from the cloud means a Workspace shared drive plus Drive
API code. The simpler route is a monthly `launchd` job on the Mac, run on the 2nd of each
month, that copies the newest object to the old Drive folder:

```sh
latest=$(gcloud storage ls gs://st-alumni-crm-backups/ | sort | tail -1)
gcloud storage cp "$latest" "$HOME/Library/CloudStorage/GoogleDrive-ujaval@spatialthoughts.com/My Drive/Spatial Thoughts/Apps/backups/"
```

This copy only happens when the Mac is on, so it's a convenience, not the backup itself.

## Part C — Operating rules

- **Deploy only when the app is idle.** A new revision starts its own instance and restores
  from the bucket. If the old instance is still alive and writing, two Litestream
  processes write to the same replica, and that can corrupt it. Wait ~15 min after
  last use (or check that no instance is running in the console metrics) before `deploy.sh`.
- **Offline scripts read a restored copy**, never the live replica:
  `litestream restore -o ./crm-local.db gcs://st-alumni-crm-db/crm.db`, then
  `CRM_DB=./crm-local.db python render_country_map.py`. Bulk imports go through the app's
  Upload page. Don't import locally and push the file back.
- **Backups, at two levels**:
  - Litestream snapshots and bucket versioning give point-in-time recovery over the
    last few weeks.
  - The monthly job (B11) gives 24 integrity-checked monthly copies in a separate bucket
    the app can't write to.
  - Check the backup bucket's newest object once a quarter.
- **Restore drill** (once, after go-live): restore to a timestamp with
  `litestream restore -timestamp ...` and open the result locally.

## Part D — Phases

| Phase | Work | Done when |
|---|---|---|
| 1 | Part A code changes; local Docker test with a file replica | Edits survive container restart locally |
| 2 | B1–B5 (project, budget, APIs, bucket, service account) | Budget alert active, bucket private |
| 3 | B6 seed with **`test.db`** first; B7–B10 | Full verification passes on fake data |
| 4 | Cutover: wipe the test replica, B6 with the real `$CRM_DB`, redeploy, B10 again | Row counts match; Drive copy frozen and renamed `crm-frozen-YYYYMMDD.db` |
| 5 | B11 monthly backups: bucket, service account, job, schedule, failure alert | First manual run produces an object whose metadata row counts match |
| 6 | README and memory note updated (`CRM_DB` is development-only) | — |

## Cost estimate (monthly)

| Item | Usage | Cost |
|---|---|---|
| Cloud Run (instance-based) | a few hours of uptime | $0 (free tier ~66 h) |
| IAP on Cloud Run | — | $0 |
| Cloud Storage | < 50 MB + operations | $0 (free tier 5 GB) |
| Cloud Build / Artifact Registry | occasional deploys, < 0.5 GB images | $0 |
| Egress | small HTML pages | $0 |
| Monthly backups | 24 × ~0.5 MB in Nearline, 1 job run/month | ~$0 (cents at most; Cloud Scheduler includes 3 free jobs per billing account) |

## Open questions to verify during build

- Whether direct IAP-on-Cloud-Run is GA or still preview. The docs didn't say on
  2026-10-20; the commands above come from the current docs and don't need `gcloud beta`
  except to create the service identity.
- Litestream's current config syntax for the GCS replica (`replica:` vs `replicas:`
  changed between major versions) and whether `restore` flags are unchanged. Pin the version.
- The free-tier vCPU-second allowance for instance-based billing. Confirm on the Cloud Run
  pricing page, and let the budget alert catch it if it's wrong.
