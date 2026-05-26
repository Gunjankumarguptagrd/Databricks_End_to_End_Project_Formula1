# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f'{catalog_name}.{gold_schema}.dim_races'

# COMMAND ----------

target_table

# COMMAND ----------

# Store source table, Circuit and Races doing filter by batchid
circuits_df =(
     spark.table(f"{catalog_name}.{silver_schema}.circuits")
        .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

# doing filter by batchid
races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
    .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join races & circuits using circuit_id
# MAGIC

# COMMAND ----------

dim_races_df = (
            races_df
                .join(
                    circuits_df, 
                    circuits_df.circuit_ID == races_df.circuit_ID, 
                    'inner'
                )
                .select(
                    races_df.season,
                    races_df.round,
                    races_df.race_name,
                    races_df.race_date,
                    circuits_df.circuit_name,
                    circuits_df.locality,
                    circuits_df.country,
                )

)

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# write datafrom into gold table
write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

