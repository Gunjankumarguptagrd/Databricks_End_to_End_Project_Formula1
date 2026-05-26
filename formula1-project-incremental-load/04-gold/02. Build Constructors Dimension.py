# Databricks notebook source
# MAGIC %md
# MAGIC # Read silver constructor table
# MAGIC - read gold ref_nationality_region table
# MAGIC - join both datafrma
# MAGIC - select required column
# MAGIC - transform in gold talbe

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f'{catalog_name}.{gold_schema}.dim_constructors'

# COMMAND ----------

target_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating 2 DF and Read

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")


# COMMAND ----------

# join 2 DF
dim_constructors_df = (
    constructors_df
        .join(ref_nationality_region_df, 
              constructors_df.nationality == ref_nationality_region_df.nationality, 
              "left"              
              )
        .select(
            constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")   
        )
)


# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# DBTITLE 1,Cell 10
# # write datafrom into gold table
# (
#     dim_constructors_df
#         .write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(target_table)
# )

# COMMAND ----------

# write datafrom into gold table
write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

