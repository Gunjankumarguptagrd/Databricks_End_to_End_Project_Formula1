# Databricks notebook source
# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# DBTITLE 1,Cell 2
control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

from pyspark.sql import functions as F

# --------------------------------------------
# Get batch folders from landing path
# --------------------------------------------
landing_batches = sorted([
    file.name.rstrip("/")
    for file in dbutils.fs.ls(landing_folder_path)
    if file.isDir()
])

# --------------------------------------------
# Read already tracked batches
# --------------------------------------------
if spark.catalog.tableExists(control_table):

    tracked_batches = [
        row.batch_id
        for row in (
            spark.table(control_table)
                 .filter(F.col("status").isin("in_progress", "completed"))
                 .select("batch_id")
                 .distinct()
                 .collect()
        )
    ]

else:
    tracked_batches = []

# --------------------------------------------
# Identify earliest unprocessed batch
# --------------------------------------------
new_batches = sorted(
    list(set(landing_batches) - set(tracked_batches))
)

next_batch = new_batches[0] if new_batches else None

# --------------------------------------------
# Print details
# --------------------------------------------
print(f"Landing batches        : {landing_batches}")
print(f"Tracked batches        : {tracked_batches}")
print(f"Next batch to process  : {next_batch}")

# --------------------------------------------
# Set Databricks task values
# --------------------------------------------
if next_batch is None:

    dbutils.jobs.taskValues.set(
        key="p_batch_id",
        value=""
    )

    dbutils.jobs.taskValues.set(
        key="has_batch",
        value="false"
    )

else:

    dbutils.jobs.taskValues.set(
        key="p_batch_id",
        value=next_batch
    )

    dbutils.jobs.taskValues.set(
        key="has_batch",
        value="true"
    )