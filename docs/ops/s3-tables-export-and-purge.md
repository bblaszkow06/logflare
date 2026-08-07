# S3 Tables backend: data export and purge runbook

This doc shows how to export and purge data from an S3 Tables backend. Both flows run outside
Logflare, with the standalone DuckDB CLI; Logflare is only involved as the place to look up the
backend's config (table bucket ARN, namespace, credentials).

Shared background: all sources of a backend write into three consolidated Iceberg tables —
`otel_logs`, `otel_metrics`, `otel_traces` — in the backend's namespace, day-partitioned on
`timestamp` and clustered by `(source_uuid, timestamp)` (O11Y-2215). Any per-source operation
must cover all three tables. Dotted column names (`"events.timestamp"`, …) are flat columns and
must be double-quoted in DuckDB.

## 1. Export runbook

Goal: copy all events of `(source_uuid, project)` in a time range to a regular S3 bucket, as
Parquet. Read-only against the table bucket — safe to run at any time, no coordination with
ingestion needed.

### Prerequisites

- **DuckDB CLI** with the `aws`, `httpfs`, and `iceberg` extensions (installed in the script
  below).
- **Backend config**: `table_bucket_arn`, `namespace`, and credentials, from the backend's
  config in Logflare. The region is the 4th `:`-separated component of the ARN.
- **Data coordinates**: the source UUID(s) (the source `token` in Logflare) and the `project`
  value the mapping config writes.
- **IAM**: the backend's ingest credentials cover table-bucket reads; additionally you need
  `s3:PutObject` + `s3:ListBucket` on the **destination** bucket (a regular S3 bucket, outside
  the table bucket's IAM surface).

> **Performance warning**: until the write-side sort from O11Y-2215 is deployed *and* has taken
> effect (pre-existing files are only re-clustered by compaction), a `source_uuid`/`project`
> filter cannot prune files — the query scans **all data of the backend** for the covered days.
> This affects cost and duration only, not correctness. The `timestamp` bounds do prune day
> partitions regardless — always set them.

### Steps

```sql
-- duckdb (in-memory session is fine)
INSTALL aws; INSTALL httpfs; INSTALL iceberg;
LOAD aws; LOAD httpfs; LOAD iceberg;

-- Secret 1: table-bucket catalog credentials.
CREATE OR REPLACE SECRET s3t_catalog (
  TYPE s3,
  KEY_ID '<access_key_id>',
  SECRET '<secret_access_key>',
  REGION '<region-from-arn>'
);

ATTACH '<table_bucket_arn>' AS s3t (TYPE iceberg, ENDPOINT_TYPE s3_tables);
USE s3t."<namespace>";

-- Secret 2: destination bucket, SCOPEd so these credentials are only used for it
-- (and the catalog secret is never used to sign destination writes). Required when
-- credentials or region differ; harmless otherwise.
CREATE OR REPLACE SECRET export_dest (
  TYPE s3,
  KEY_ID '<dest_key_id>',
  SECRET '<dest_secret>',
  REGION '<dest_region>',
  SCOPE 's3://<export-bucket>'
);

-- 1. Export. The explicit transaction matters: table metadata is refreshed per
--    transaction, so BEGIN pins one snapshot for both the COPY and the
--    verification counts — otherwise concurrent ingestion could legitimately
--    drift the counts between statements.
BEGIN;

COPY (
  SELECT *
  FROM otel_logs
  WHERE source_uuid = '<source_uuid>'
    AND project = '<project>'
    AND "timestamp" >= TIMESTAMPTZ '2026-07-01 00:00:00+00'
    AND "timestamp" <  TIMESTAMPTZ '2026-08-01 00:00:00+00'
) TO 's3://<export-bucket>/exports/<source_uuid>/otel_logs'
  (FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES '512MB');

-- 2. Verify inside the same transaction:
SELECT count(*) FROM otel_logs
WHERE source_uuid = '<source_uuid>' AND project = '<project>'
  AND "timestamp" >= TIMESTAMPTZ '2026-07-01 00:00:00+00'
  AND "timestamp" <  TIMESTAMPTZ '2026-08-01 00:00:00+00';

SELECT count(*)
FROM read_parquet('s3://<export-bucket>/exports/<source_uuid>/otel_logs/*.parquet');
-- both counts must match

COMMIT;

-- 3. Repeat steps 1-2 for otel_metrics and otel_traces if the source ingests those types.
```

Variations: `(FORMAT json)` for NDJSON output; add `PARTITION_BY` to the `COPY` options for
Hive-style output layout. Note `project` is a nullable, mapper-populated column — if unsure what
a source writes there, sample it first (`SELECT project, count(*) … GROUP BY 1`) or drop the
`project` predicate and rely on `source_uuid`.

## 2. Purge runbook

Goal: given an SQL predicate, make matching rows unreadable immediately, with physical removal
following automatically. Understand the model first:

> DuckDB `DELETE` on Iceberg is **merge-on-read**: it commits positional delete files that mask
> rows; the bytes remain in existing Parquet files and in every earlier snapshot (time travel
> still sees them). Physical removal happens later, through S3 Tables managed maintenance — see
> [When does the data physically disappear?](#when-does-the-data-physically-disappear) below.

### Prerequisites

- **DuckDB CLI ≥ 1.5.3.** Support for `DELETE` on **partitioned** tables (ours are
  day-partitioned) landed in duckdb-iceberg v1.5.3; the DuckDB 1.5.1 embedded in ADBC v0.12.1
  is too old. Verify after `INSTALL iceberg`:
  `SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg';`
- **AWS CLI** with `aws s3tables` command support, if you want to check or adjust maintenance
  settings.
- **Backend config and data coordinates**: as in the export runbook, with the source UUID(s) or
  the compliance predicate identifying the rows to purge.
- **IAM**: the `DELETE` commit uses the same data/metadata write permissions ingestion already
  has. Checking or adjusting maintenance settings additionally needs
  `s3tables:Get/PutTableMaintenanceConfiguration`, `s3tables:GetTableMaintenanceJobStatus`, and
  `s3tables:PutTableBucketMaintenanceConfiguration`.

### Step 0 — Stop the ingestion for the affected data

Two reasons this is not optional for a strict purge:

1. `DELETE` masks only rows visible in the snapshot it scans. The pipeline commits a new
   snapshot up to every `batch_timeout` (5–60 s); matching rows committed mid-purge survive.
2. The external DELETE commit contends with the pipeline's `fast_append` commits. The pipeline
   retries a failed batch twice (on top of iceberg-rust's internal 30 s commit retry) and then
   **drops the events with only a log warning** (`"Dropping N S3 Tables events"`).

Preferred: stop the upstream sender(s), or detach the source(s) from the backend for the purge
window. If neither is possible, accept the race and rely on the re-run loop in Step 2 — and
watch logs for the drop warning.

### Step 1 — DELETE in each table

Same session bootstrap as the export runbook (catalog secret + `ATTACH` + `USE`), then:

```sql
DELETE FROM otel_logs
WHERE source_uuid = '<source_uuid>'
  AND "timestamp" >= TIMESTAMPTZ '…' AND "timestamp" < TIMESTAMPTZ '…';

-- repeat for otel_metrics and otel_traces
```

For arbitrary compliance predicates substitute the WHERE clause (e.g.
`event_message LIKE '%leaked-token%'`) — but keep a `timestamp` bound whenever the incident
window is known: an unbounded predicate scans every file of every day and widens the conflict
window with ingestion.

### Step 2 — Verify logical deletion (re-run loop)

```sql
SELECT count(*) FROM otel_logs WHERE <same predicate>;  -- expect 0
```

If ingestion was not fully stopped: repeat Step 1 + Step 2 until the count is **0 on two
consecutive checks separated by at least `batch_timeout`**. Each statement runs in its own
transaction and sees the latest snapshot, so no session restart is needed between iterations.

### When does the data physically disappear?

The DELETE only masks rows. Physical removal is delegated to three S3 Tables managed
maintenance jobs, which run on AWS's schedule with **no on-demand trigger**:

1. **Compaction** (per table, enabled by default) rewrites data files and *applies the
   row-level deletes* — this removes the bytes from the current table state.
2. **Snapshot expiry** (per table) removes snapshots older than `maxSnapshotAgeHours` beyond
   `minSnapshotsToKeep` — this ends time-travel access to the purged rows and marks the old
   files noncurrent.
3. **Unreferenced file removal** (per bucket) permanently deletes noncurrent objects after
   `nonCurrentDays`.

Until all three have run, the purged rows remain readable **via time travel** by anyone with
table read access. How long that takes is a function of the maintenance settings:

- AWS account defaults (`maxSnapshotAgeHours=120`, `nonCurrentDays=10`, `unreferencedDays=3`)
  put the end-to-end chain at **~2–3 weeks**.
- At the API minimums (`maxSnapshotAgeHours=1`, `nonCurrentDays=1`, `unreferencedDays=1`) it
  shrinks to **~2 days**.

These settings are not written in stone: they can be set per table / per bucket via
`aws s3tables put-table-maintenance-configuration` and
`put-table-bucket-maintenance-configuration`, and the intended posture can simply be enforced
at table-creation time (see [Eventual Logflare changes](#5-eventual-logflare-changes)) so the
purge SLA is a property of the backend rather than something toggled around each purge. Whether
the chain has caught up past your DELETE can be checked with:

```bash
aws s3tables get-table-maintenance-job-status \
  --table-bucket-arn '<table_bucket_arn>' --namespace '<namespace>' --name otel_logs
```

**Warning — do not break snapshot management.** It fails *for the whole table* (nothing
expires, the purge never becomes physical) if the table has user-defined branches/tags or the
`history.expire.max-snapshot-age-ms` / `history.expire.min-snapshots-to-keep` Iceberg
properties. Logflare provisions tables with neither (only `logflare.schema-version` and
`commit.retry.total-timeout-ms`) — never add them via `ALTER TABLE SET TBLPROPERTIES`.

**A stalled maintenance chain stalls the purge.** If compaction jobs fail repeatedly, the
positional deletes are never materialized and physical deletion never happens — watch the
job status. Nuclear fallback: `CREATE TABLE tmp AS SELECT * FROM t WHERE NOT <predicate>` +
swap (downtime; last resort).

## 4. Eventual Logflare changes

Recorded here so the manual runbook has a paved path to automation (none of this is scheduled):

1. **Pin maintenance config at table creation** (`CatalogManager`): call
   `PutTableMaintenanceConfiguration` after `ensure_table`, so the purge SLA and storage costs
   stop depending on unpinned AWS account defaults. Keep the invariant of never setting
   `history.expire.*` properties (worth a comment in `IcebergSchema.table_properties/1`).
2. **`Maintenance.purge/3` / `Maintenance.export/4` module**: own DuckDB connection, bypassing —
   not weakening — the SELECT-only endpoint query path. Requires bumping `adbc`/DuckDB so the
   embedded engine gains partitioned DELETE (iceberg-rust 0.9.1 delete-write support is too
   immature to do this in the NIF).
3. **`QuerySup.recycle/1`** wrapping the snippet from §3 + cluster fan-out; the same primitive
   serves backend-config changes and idle shutdown.
4. **NOT NULL `source_uuid`** (O11Y-2215) closes the NULL-tenancy hole for good.
5. **Telemetry/alarm on conflict-drops** in `Pipeline.ack_backend_failures/2`, so purge-induced
   (or compaction-induced) ingest loss is observable.
6. **Per-backend ingest-pause flag** consulted by the pipeline, for race-free purges without
   touching senders.
