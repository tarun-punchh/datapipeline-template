"""
00_config_loader.py
-------------------
Loads the dp_config_template.json from the S3 source location and exposes
pipeline-wide parameters and utility functions used by subsequent
transformation files (01_bronze_ingestion.py, 02_silver_processing.py).

SDP executes files in lexicographic order, so the "00_" prefix guarantees
this module is evaluated first and its module-level variables are available
when other files import from it (or, more precisely, they share the same
`spark` session and can re-read the same conf values).

NOTE: In SDP, each .py file runs in the same SparkSession.  We store the
parsed config in a module-level variable so downstream files can import it.
"""

import json

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ShortType

# ---------------------------------------------------------------------------
# 1. Pipeline parameters (set via pipeline YAML configuration block)
# ---------------------------------------------------------------------------
source_location = spark.conf.get("source_location", "")  # noqa: F821
if source_location:
    source_location = source_location.rstrip("/")

catalog_name = spark.conf.get("catalog_name")  # noqa: F821
bronze_db = spark.conf.get("bronze_db")  # noqa: F821
silver_db = spark.conf.get("silver_db")  # noqa: F821

# ---------------------------------------------------------------------------
# 2. Load the per-table configuration from S3
# ---------------------------------------------------------------------------
_config_path = f"{source_location}/dp_config_template.json"
_config_text = "".join(
    [row.value for row in spark.read.text(_config_path).collect()]  # noqa: F821
)
pipeline_config: dict = json.loads(_config_text)

# ---------------------------------------------------------------------------
# 3. Utility: upcast all int / short columns to bigint (LongType)
# ---------------------------------------------------------------------------

def upcast_int_to_bigint(df):
    """
    Scan the DataFrame schema and cast every IntegerType / ShortType column
    to LongType (bigint).  This avoids downstream overflow issues when
    source integer columns contain values that exceed the 32-bit range.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, (IntegerType, ShortType)):
            df = df.withColumn(field.name, F.col(field.name).cast("bigint"))
    return df
