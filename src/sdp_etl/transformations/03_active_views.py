"""
03_active_views.py
------------------
When soft deletes are enabled (soft_deletes = "Y"), this module creates:

  1. A schema named ``{catalog_name}.{silver_db}_active``
  2. One SQL view per entity inside that schema, named
     ``silver_{entity}``, which mirrors the corresponding silver table
     but filters out deleted records (``op != 'D'``).

This allows consumers to query the ``_active`` database for a clean
view of only non-deleted records, while the primary silver database
retains the full history including soft-deleted rows.

When soft_deletes is not "Y" (the default), this module is a no-op.

NOTE: The views are created via ``spark.sql()`` outside of SDP's dataset
management (dp.table / dp.view).  They are lightweight SQL views that
point to the SDP-managed silver tables and do not need to be part of
the pipeline DAG.
"""

import json

# ---------------------------------------------------------------------------
# Pipeline parameters
# ---------------------------------------------------------------------------
source_location = spark.conf.get("source_location", "")  # noqa: F821
if source_location:
    source_location = source_location.rstrip("/")

catalog_name = spark.conf.get("catalog_name")  # noqa: F821
silver_db = spark.conf.get("silver_db")  # noqa: F821
soft_deletes = spark.conf.get("soft_deletes", "N")  # noqa: F821

# ---------------------------------------------------------------------------
# Only proceed when soft deletes are enabled
# ---------------------------------------------------------------------------
if soft_deletes.upper() == "Y":

    # -- Load per-table config from S3 -------------------------------------
    _config_path = f"{source_location}/dp_config_template.json"
    _config_text = "".join(
        [row.value for row in spark.read.text(_config_path).collect()]  # noqa: F821
    )
    pipeline_config: dict = json.loads(_config_text)

    # -- Active database name -----------------------------------------------
    active_db = f"{silver_db}_active"

    # -- Create the _active schema if it does not already exist -------------
    spark.sql(  # noqa: F821
        f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{active_db}`"
    )

    # -- Create one view per entity -----------------------------------------
    for table_name in pipeline_config:
        silver_fqn = f"`{catalog_name}`.`{silver_db}`.`silver_{table_name}`"
        view_fqn = f"`{catalog_name}`.`{active_db}`.`silver_{table_name}`"

        spark.sql(  # noqa: F821
            f"CREATE OR REPLACE VIEW {view_fqn} "
            f"AS SELECT * FROM {silver_fqn} WHERE op != 'D'"
        )
