# Data Pipeline Template

A generic, config-driven Spark Declarative Pipeline (SDP) that dynamically ingests data from S3 into bronze streaming tables and processes them into silver tables with deduplication, clustering, and data-skipping indexes -- all powered by a single JSON configuration file.

## Architecture

```
S3 Source Location                    Unity Catalog
+-------------------------------+     +----------------------------------+
| dp_config_template.json       |     | catalog.bronze_db                |
| app_downloads/*.parquet       | --> |   bronze_app_downloads           |
| users/*.json                  | --> |   bronze_users                   |
| receipts/*.json               | --> |   bronze_receipts                |
| locations/*.parquet           | --> |   bronze_locations               |
+-------------------------------+     +----------------------------------+
                                              |
                                              v  (dedup + cluster)
                                      +----------------------------------+
                                      | catalog.silver_db                |
                                      |   silver_app_downloads           |
                                      |   silver_users                   |
                                      |   silver_receipts                |
                                      |   silver_locations               |
                                      +----------------------------------+
```

**Bronze layer** -- Raw ingestion via Auto Loader with automatic checkpointing, schema evolution, and int-to-bigint upcasting.

**Silver layer** -- Deduplicated on primary key, with Liquid Clustering, data-skipping stats columns, and data quality expectations.

## Project Structure

```
datapipeline_template/
├── databricks.yml                              # Asset Bundle config (dev/prod targets)
├── dp_config_template.json                     # Per-entity pipeline configuration
├── README.md
├── resources/
│   ├── sdp_pipeline.pipeline.yml               # SDP pipeline resource definition
│   └── sdp_job.job.yml                         # Scheduled job resource (daily trigger)
└── src/
    └── sdp_etl/
        └── transformations/
            ├── 00_config_loader.py              # Config reader + utility functions
            ├── 01_bronze_ingestion.py           # Dynamic bronze streaming tables
            └── 02_silver_processing.py          # Dynamic silver tables with dedup
```

## Prerequisites

- Databricks CLI installed and authenticated (`databricks configure`)
- Unity Catalog enabled workspace
- Serverless compute enabled
- S3 external location configured in Unity Catalog for the source bucket

## Pipeline Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `source_location` | Full S3 path containing data folders and `dp_config_template.json` | -- |
| `catalog_name` | Unity Catalog name for output tables | -- |
| `bronze_db` | Schema name for bronze (raw ingestion) tables | -- |
| `silver_db` | Schema name for silver (deduplicated) tables | -- |
| `external_location` | Optional S3 path for external (unmanaged) tables. When set, tables are created at `{external_location}/bronze/{entity}/` and `{external_location}/silver/{entity}/`. When empty, tables are managed (default). | `""` |
| `trigger_frequency` | Job schedule frequency (`daily`, `hourly`, `weekly`) | `daily` |

All parameters are defined as variables in `databricks.yml` and passed to the pipeline via the `configuration` block in the pipeline resource YAML. Python code reads them at runtime using `spark.conf.get()`.

## Configuration File

Place a `dp_config_template.json` file at the root of your S3 source location. Each top-level key represents a table/entity, and its subfolder under the source location contains the raw data files.

### Example

For source location `s3://punchh-data-pipeline/tarun-test/`, the expected S3 layout is:

```
s3://punchh-data-pipeline/tarun-test/
├── dp_config_template.json
├── app_downloads/
│   └── *.parquet
├── users/
│   └── *.json
├── receipts/
│   └── *.json
└── locations/
    └── *.parquet
```

### Config Fields

| Field | Type | Description |
|-------|------|-------------|
| `raw_file_format` | string | File format in the source folder (`parquet`, `json`, `csv`, `avro`) |
| `clustering_cols` | list | Columns for Liquid Clustering on the silver table |
| `skipping_indexes` | list | Columns for Delta data-skipping stats (`delta.dataSkippingStatsColumns`) |
| `unique_primary_key` | list | Columns forming the primary key for silver deduplication |
| `renamed_columns` | list | Reserved for future column renaming support |
| `expect_all_or_drop` | dict | Data quality expectations (name -> SQL expression); rows failing all are dropped |
| `deleted_file_retention_duration` | string | Optional. Duration to keep deleted data files before physical deletion (e.g., `"interval 30 days"`). Maps to `delta.deletedFileRetentionDuration` table property. When omitted, Delta defaults to `interval 1 week`. |

### Example Entry

```json
{
    "app_downloads": {
        "raw_file_format": "parquet",
        "clustering_cols": ["id", "op"],
        "skipping_indexes": ["id", "op", "business_id"],
        "unique_primary_key": ["id"],
        "renamed_columns": [],
        "expect_all_or_drop": {
            "has_timestamp": "created_at IS NOT NULL",
            "has_id": "Id IS NOT NULL"
        },
        "deleted_file_retention_duration": "interval 30 days"
    }
}
```

## How It Works

### Bronze Ingestion (`01_bronze_ingestion.py`)

For each entity in the config, a **streaming table** is dynamically created using the factory pattern:

1. **Auto Loader** (`cloudFiles` format) reads incrementally from `{source_location}/{entity}/`
2. Schema is inferred automatically with `cloudFiles.inferColumnTypes` and evolves via `addNewColumns` mode
3. All `IntegerType` / `ShortType` columns are upcast to `BigInt` (LongType)
4. Audit columns are added: `_ingested_at`, `_source_file`, `_file_modification_time`

### Silver Processing (`02_silver_processing.py`)

For each entity, a **table** is created that reads the corresponding bronze table and applies:

1. **Deduplication** -- `ROW_NUMBER()` window partitioned by `unique_primary_key`, ordered by `_ingested_at DESC`, keeping only the latest row
2. **Liquid Clustering** -- via `cluster_by` on configured `clustering_cols`
3. **Data-skipping indexes** -- via `delta.dataSkippingStatsColumns` table property
4. **Data quality** -- `dp.expect_all_or_drop()` enforces configured expectations, dropping rows that violate all rules

### Checkpointing

SDP automatically manages Auto Loader checkpoints for each streaming table. On pipeline restart, each bronze table resumes from the last successfully processed file -- no manual checkpoint configuration is needed.

### External Tables

By default, all tables are created as **managed tables** (data stored in the Unity Catalog managed storage location). To use **external (unmanaged) tables** instead, set the `external_location` pipeline parameter to an S3 path:

```yaml
variables:
  external_location: "s3://my-bucket/external/"
```

When configured, tables are stored at:
- Bronze: `{external_location}/bronze/{entity}/`
- Silver: `{external_location}/silver/{entity}/`

When `external_location` is empty or omitted, the pipeline falls back to managed tables (default behaviour). The S3 path must be registered as an external location in Unity Catalog.

### Deleted File Retention

Each entity can configure how long logically deleted data files are kept before physical deletion by VACUUM, using the `deleted_file_retention_duration` field in the config JSON. This maps to the `delta.deletedFileRetentionDuration` Delta table property.

```json
"deleted_file_retention_duration": "interval 30 days"
```

When omitted, Delta uses its default retention of `interval 1 week` (7 days). Values must use the CalendarInterval format (e.g., `"interval 7 days"`, `"interval 2 weeks"`, `"interval 30 days"`). Databricks recommends keeping the retention at 7 days or higher to avoid issues with long-running queries or streaming jobs.

## Getting Started

### 1. Configure your workspace

Update `databricks.yml` with your workspace host URL and variable values:

```yaml
targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
    variables:
      source_location: "s3://your-bucket/your-prefix/"
      catalog_name: "your_catalog"
      bronze_db: "bronze"
      silver_db: "silver"
      external_location: ""  # empty = managed tables; set to S3 path for external tables
```

### 2. Upload config to S3

Place your `dp_config_template.json` at the root of the `source_location` S3 path, alongside the data folders.

### 3. Validate the bundle

```bash
databricks bundle validate
```

### 4. Deploy

```bash
# Deploy to dev (default target)
databricks bundle deploy

# Deploy to production
databricks bundle deploy --target prod
```

### 5. Run the pipeline

```bash
# Run the pipeline
databricks bundle run sdp_etl

# Run with full refresh (reprocess all data)
databricks bundle run sdp_etl --full-refresh
```

The scheduled job (`sdp_daily_job`) will also trigger the pipeline automatically at 6 AM PT daily.

## Adding New Entities

To ingest a new table, simply:

1. Add a new entry to `dp_config_template.json` in S3
2. Create the corresponding data folder under the source location
3. Re-run the pipeline -- it will automatically pick up the new entity

No code changes required.

## Customization

### Change the schedule

Edit `resources/sdp_job.job.yml` and update the `quartz_cron_expression`:

| Frequency | Cron Expression |
|-----------|----------------|
| Daily at 6 AM | `0 0 6 * * ?` |
| Hourly | `0 0 * * * ?` |
| Every Monday at 6 AM | `0 0 6 ? * MON` |

### Override per environment

Add target-specific overrides in `databricks.yml` under each target's `variables` block. For example, use a different S3 path and catalog per environment.
