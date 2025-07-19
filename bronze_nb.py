# Databricks notebook source
# MAGIC %md ##### Imports

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md ##### Reading all files using mountpoint

# COMMAND ----------

# List all files in the mount directory
files = dbutils.fs.ls("/mnt/BayerHackathonMount1")
display(files)

# COMMAND ----------

# MAGIC %md ##### Reading sales table from source

# COMMAND ----------

try:
  print('Reading salesorder csv file')
  
  salesorder_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/BayerHackathonMount1/SalesOrderline.csv")
  display(salesorder_df)

  salesorder_df = salesorder_df1.withColumn('load_date', current_timestamp())
  
  read_count = salesorder_df.count()
  print(f'count is {read_count}')

except Exception as e:
  print(f'Error reading csv file: {e}')

# COMMAND ----------

# MAGIC %md ##### Creating bronze DataBase

# COMMAND ----------

spark.sql("create database if not exists bronze_kuppala")

# COMMAND ----------

# MAGIC %md ##### Loading tables into bronze

# COMMAND ----------

try:
    salesorder_df1.write.format("delta").mode("overwrite").saveAsTable("bronze_kuppala.salesorder_kuppala")
    # display(salesorder_df1)
    # salesorder_df1.printSchema()
except Exception as e:
    print(f'Error writing to table: {e}')

# COMMAND ----------

# spark.sql("create table if not exists hive_metastore.bronze_kuppala.salesorder_kuppala")
# spark.sql("drop table if exists hive_metastore.bronze_kuppala.salesorder_kuppala")

# COMMAND ----------

# MAGIC %md ##### Reading Product files from source

# COMMAND ----------

try:
  print('Reading salesorder csv file')
  
  product_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/mnt/BayerHackathonMount1/Product.csv")
  display(product_df)

  product_df1 = product_df.withColumn('load_date', current_timestamp())
  
  read_count = product_df.count()
  print(f'count is {read_count}')

except Exception as e:
  print(f'Error reading csv file: {e}')

# COMMAND ----------

# MAGIC %md ##### removing whitesapces

# COMMAND ----------


product_df1 = product_df1.select([col(c).alias(c.replace(' ', '')) for c in product_df1.columns])
display(product_df1)

# COMMAND ----------

# MAGIC %md ##### Writing into bronze layer

# COMMAND ----------

try:
    product_df1.write.format("delta").mode("overwrite").saveAsTable("bronze_kuppala.product_kuppala")
    display(product_df1)
    # product_df1.printSchema()
except Exception as e:
    print(f'Error writing to table: {e}')