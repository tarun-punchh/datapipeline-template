"""
01_bronze_ingestion.py
----------------------
Dynamically creates one **streaming table** per entity defined in the
dp_config_template.json config file.

Each bronze table:
  - Reads from the entity's S3 subfolder using Auto Loader (cloudFiles)
  - Infers and evolves schema automatically (addNewColumns mode)
  - Upcasts all IntegerType / ShortType columns to BigInt (LongType)
  - Adds audit metadata columns: _ingested_at, _source_file,
    _file_modification_time
  - Supports external (unmanaged) tables when external_location is configured
  - Supports configurable deleted-file retention via
    delta.deletedFileRetentionDuration

Checkpointing:
  SDP automatically manages Auto Loader checkpoints per streaming table.
  On restart the pipeline resumes from the last successfully processed file
  -- no manual checkpoint path configuration is needed.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Re-read the shared config (SDP files share the same SparkSession but are
# independent modules -- we read conf values directly rather than importing).
import json

source_location = spark.conf.get("source_location", "")  # noqa: F821
if source_location:
    source_location = source_location.rstrip("/")

catalog_name = spark.conf.get("catalog_name")  # noqa: F821
bronze_db = spark.conf.get("bronze_db")  # noqa: F821

# Optional: external storage location for creating external (unmanaged) tables.
# When empty, tables are created as managed tables (default behaviour).
external_location = spark.conf.get("external_location", "")  # noqa: F821
if external_location:
    external_location = external_location.rstrip("/")

_config_path = f"{source_location}/dp_config_template.json"
_config_text = "".join(
    [row.value for row in spark.read.text(_config_path).collect()]  # noqa: F821
)
pipeline_config: dict = json.loads(_config_text)

# Import the upcast utility (defined in 00_config_loader which runs first)
from pyspark.sql.types import IntegerType, ShortType


def upcast_int_to_bigint(df):
    """Cast every IntegerType / ShortType column to bigint."""
    for field in df.schema.fields:
        if isinstance(field.dataType, (IntegerType, ShortType)):
            df = df.withColumn(field.name, F.col(field.name).cast("bigint"))
    return df


# ---------------------------------------------------------------------------
# Dynamic bronze table factory
# ---------------------------------------------------------------------------

def create_bronze_table(table_name: str, table_config: dict):
    """
    Register a bronze streaming table for *table_name* with SDP.

    The factory pattern (function-that-decorates-a-function) is the standard
    way to create tables dynamically in a loop within SDP / DLT pipelines.
    """
    raw_file_format = table_config.get("raw_file_format", "parquet")
    data_path = f"{source_location}/{table_name}/"

    # -- Table properties ------------------------------------------------------
    tbl_props = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }

    # Deleted-file retention (optional per-entity config)
    retention = table_config.get("deleted_file_retention_duration", "")
    if retention:
        tbl_props["delta.deletedFileRetentionDuration"] = retention

    # -- External table path (only when external_location is configured) -------
    ext_path = (
        f"{external_location}/bronze/{table_name}/"
        if external_location
        else None
    )

    @dp.table(
        name=f"{catalog_name}.{bronze_db}.bronze_{table_name}",
        comment=f"Bronze raw ingestion for {table_name} from {data_path}",
        path=ext_path,
        table_properties=tbl_props,
    )
    def _bronze_table():
        # Read incrementally from S3 using Auto Loader
        raw_df = (
            spark.readStream  # noqa: F821
            .format("cloudFiles")
            .option("cloudFiles.format", raw_file_format)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(data_path)
        )

        # Upcast int -> bigint to avoid overflow in downstream processing
        raw_df = upcast_int_to_bigint(raw_df)

        # Add audit / metadata columns
        raw_df = (
            raw_df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn(
                "_file_modification_time",
                F.col("_metadata.file_modification_time"),
            )
        )

        return raw_df

    return _bronze_table


# ---------------------------------------------------------------------------
# Create a bronze table for every entity in the config
# ---------------------------------------------------------------------------
for _table_name, _table_config in pipeline_config.items():
    create_bronze_table(_table_name, _table_config)
