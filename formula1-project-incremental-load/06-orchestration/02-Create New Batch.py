# Databricks notebook source
# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# DBTITLE 1,Cell 2
control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

v_batch_id = dbutils.widgets.get("p_batch_id").strip()

if v_batch_id:

    in_progress_df = (
        spark.createDataFrame(
            [Row(batch_id=v_batch_id, status="IN_PROGRESS")]
        )
        .withColumn(
            "created_timestamp",
            F.current_timestamp().cast("timestamp")
        )
        .withColumn(
            "updated_timestamp",
            F.current_timestamp().cast("timestamp")
        )
    )

    (
        in_progress_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(control_table)
    )

    print(f"Marked Batch {v_batch_id} as in progress.")

else:
    raise Exception("Batch id is missing")