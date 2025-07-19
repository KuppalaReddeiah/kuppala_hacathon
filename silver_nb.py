# Databricks notebook source
# MAGIC %md ##### Imports

# COMMAND ----------

from pyspark.sql.functions import date_format
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

# MAGIC %md ##### Reading and doing cleaning for sales table

# COMMAND ----------

df = spark.read.table('bronze_kuppala.salesorder_kuppala')
# display(df)

df_duplicatecheck = df.dropDuplicates(["salesOrderLineId", "salesOrderId", "ProductId"])
# df_duplicatecheck.display()

df_nullcheck_sales = df_duplicatecheck.filter(
    (df_duplicatecheck["salesOrderLineId"].isNotNull()) & (df_duplicatecheck["ProductId"].isNotNull())
)
# df_nullcheck_sales.display()

# COMMAND ----------

# MAGIC %md ##### Duplicate records

# COMMAND ----------


window_spec = Window.partitionBy("salesOrderLineId", "salesOrderId", "ProductId").orderBy("salesOrderLineId")
df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
df_duplicatechec_records = df_with_rownum.filter(df_with_rownum["row_num"] > 1).drop("row_num")
display(df_duplicatechec_records)

# COMMAND ----------

# MAGIC %md ##### Reading and doing cleaning for product table

# COMMAND ----------

df = spark.read.table('bronze_kuppala.product_kuppala')
# display(df)

df_duplicatecheck = df.dropDuplicates(["ProductId"])
# df_duplicatecheck.display()

df_nullcheck_product = df_duplicatecheck.filter(df_duplicatecheck["ProductId"].isNotNull())
df_nullcheck_product.display()

# df2_datecheck = df2_nullcheck.withColumn("year", date_format("year", "dd-MM-yyyy"))
# # df2_datecheck.display()

# COMMAND ----------

# MAGIC %md ##### Duplicate Records

# COMMAND ----------


window_spec = Window.partitionBy("ProductId").orderBy("ProductId")
df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
df_duplicatecheck_records1 = df_with_rownum.filter(df_with_rownum["row_num"] > 1).drop("row_num")
display(df_duplicatecheck_records1)

# COMMAND ----------

# MAGIC %md ##### Creating Silver DataBase

# COMMAND ----------

spark.sql("create database if not exists silver_kuppala")

# COMMAND ----------

# MAGIC %md ##### Writing into silver tables

# COMMAND ----------

try:
    df_nullcheck_sales.write.format("delta").mode("overwrite").saveAsTable("silver_kuppala.sales_kuppala")
    df_nullcheck_product.write.format("delta").mode("overwrite").saveAsTable("silver_kuppala.product_kuppala")
except Exception as e:
    print(f'Error writing to table: {e}')