"""
02_silver_processing.py
-----------------------
Dynamically creates one **silver table** (materialized view) per entity
defined in the dp_config_template.json config file.

Each silver table:
  - Reads from the corresponding bronze streaming table
  - Deduplicates rows on the configured unique primary key(s), keeping the
    most recently ingested record (latest _ingested_at)
  - Applies Liquid Clustering on the configured clustering columns
  - Enables bloom-filter data-skipping indexes on configured columns
  - Enforces data quality expectations via @dp.expect_all_or_drop()
  - Supports external (unmanaged) tables when external_location is configured
  - Supports configurable deleted-file retention via
    delta.deletedFileRetentionDuration
  - Supports soft deletes: when soft_deletes="Y", op='D' records are retained
    in silver tables; otherwise they are filtered out during dedup

Silver tables are created in the *silver_db* schema using fully-qualified
names (catalog_name.silver_db.silver_<entity>), while bronze tables live in
the pipeline's default target schema (bronze_db).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import json

# ---------------------------------------------------------------------------
# Pipeline parameters
# ---------------------------------------------------------------------------
source_location = spark.conf.get("source_location", "")  # noqa: F821
if source_location:
    source_location = source_location.rstrip("/")

catalog_name = spark.conf.get("catalog_name")  # noqa: F821
bronze_db = spark.conf.get("bronze_db")  # noqa: F821
silver_db = spark.conf.get("silver_db")  # noqa: F821

# Optional: external storage location for creating external (unmanaged) tables.
# When empty, tables are created as managed tables (default behaviour).
external_location = spark.conf.get("external_location", "")  # noqa: F821
if external_location:
    external_location = external_location.rstrip("/")

# Soft deletes: when "Y", op='D' records are kept in silver tables and
# an _active database with filtered views is created (see 03_active_views.py).
# When not "Y" (default), op='D' records are filtered out during dedup.
soft_deletes = spark.conf.get("soft_deletes", "N")  # noqa: F821

# ---------------------------------------------------------------------------
# Load per-table config from S3
# ---------------------------------------------------------------------------
_config_path = f"{source_location}/dp_config_template.json"
_config_text = "".join(
    [row.value for row in spark.read.text(_config_path).collect()]  # noqa: F821
)
pipeline_config: dict = json.loads(_config_text)


# ---------------------------------------------------------------------------
# Dynamic silver table factory
# ---------------------------------------------------------------------------

def create_silver_table(table_name: str, table_config: dict):
    """
    Register a silver table for *table_name* with SDP.

    The silver layer applies:
      1. Primary-key deduplication (ROW_NUMBER window)
      2. Liquid Clustering for query performance
      3. Data-skipping index on wildely use columns
      4. Data quality expectations (expect_all_or_drop)
    """

    # -- Clustering keys (Liquid Clustering) --------------------------------
    clustering_cols = table_config.get("clustering_cols", [])

    # -- Data-skipping index table properties --------------------------------
    skipping_cols = table_config.get("skipping_indexes", [])
    tbl_props = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
    if skipping_cols:
        tbl_props["delta.dataSkippingStatsColumns"] = ",".join(skipping_cols)

    # -- Deleted-file retention (optional per-entity config) -----------------
    retention = table_config.get("deleted_file_retention_duration", "")
    if retention:
        tbl_props["delta.deletedFileRetentionDuration"] = retention

    # -- Data quality expectations ------------------------------------------
    expectations = table_config.get("expect_all_or_drop", {})

    # -- Primary key columns for deduplication ------------------------------
    pk_cols = table_config.get("unique_primary_key", ["id"])

    # -- Fully-qualified bronze source table name ---------------------------
    bronze_fqn = f"{catalog_name}.{bronze_db}.bronze_{table_name}"

    # -- External table path (only when external_location is configured) -----
    ext_path = (
        f"{external_location}/silver/{table_name}/"
        if external_location
        else None
    )

    # -- Build the table function and apply decorators manually ---------------
    #    We conditionally apply @dp.expect_all_or_drop only when expectations
    #    are defined, because passing an empty dict may raise an error.
    #    Decorators are applied bottom-up: expectations first, then dp.table.

    def _silver_table():
        # Read the full bronze table (batch read for materialized view)
        bronze_df = spark.read.table(bronze_fqn)  # noqa: F821

        # Deduplicate: keep the latest ingested row per primary key
        window_spec = Window.partitionBy(
            *[F.col(c) for c in pk_cols]
        ).orderBy(F.col("_ingested_at").desc())

        deduped_df = (
            bronze_df
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        # When soft deletes is disabled, filter out deleted records.
        # When enabled, op='D' rows are kept and _active views provide
        # a filtered perspective (see 03_active_views.py).
        if soft_deletes.upper() != "Y":
            deduped_df = deduped_df.filter(F.col("op") != "D")

        return deduped_df

    # Apply data quality expectations (if any are configured)
    if expectations:
        _silver_table = dp.expect_all_or_drop(expectations)(_silver_table)

    # Register as an SDP table with clustering and bloom-filter properties
    _silver_table = dp.table(
        name=f"{catalog_name}.{silver_db}.silver_{table_name}",
        comment=f"Silver deduplicated table for {table_name}",
        path=ext_path,
        cluster_by=clustering_cols if clustering_cols else None,
        table_properties=tbl_props,
    )(_silver_table)

    return _silver_table


# ---------------------------------------------------------------------------
# Create a silver table for every entity in the config
# ---------------------------------------------------------------------------
for _table_name, _table_config in pipeline_config.items():
    create_silver_table(_table_name, _table_config)
