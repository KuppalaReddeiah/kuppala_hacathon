# Databricks notebook source
# MAGIC %md ##### Imports

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import col
from pyspark.sql.functions import sum, col, round

# COMMAND ----------

# MAGIC %md ##### Reading sales table from silver

# COMMAND ----------

prod_df = spark.read.table("silver_kuppala.sales_kuppala")
prod_df.display()

# COMMAND ----------

# MAGIC %md ##### Reading product table from silver

# COMMAND ----------

sales_df = spark.read.table("silver_kuppala.product_kuppala")
sales_df.display()

# COMMAND ----------

# MAGIC %md ##### Join condition

# COMMAND ----------

df_joined = prod_df.join(sales_df, ['ProductId'], 'inner')
df_joined.display()

# COMMAND ----------

# MAGIC %md ##### removing extra characters

# COMMAND ----------

df_joined = df_joined.withColumn('Sellingprice', regexp_replace(col('Sellingprice'), ',', ''))
display(df_joined)

# COMMAND ----------

# MAGIC %md ##### changing datatype from string to int

# COMMAND ----------

df_joined = df_joined.withColumn('Sellingprice', col('Sellingprice').cast('int'))\
    .withColumn('Costprice', col('Costprice').cast('int'))
display(df_joined)

# COMMAND ----------

# MAGIC %md ##### performing validations

# COMMAND ----------

df_joined1 = df_joined.withColumn('salesamount', col('quantity') * col('Sellingprice'))\
    .withColumn('costamount', col('Costprice') * col('quantity'))\
    .withColumn('margin', col('Sellingprice') - col('Costprice'))
df_joined1.display()

# COMMAND ----------


df_joined1.groupBy('ProductId', 'ProductName').agg(sum('salesamount').alias('totalsales'), sum('costamount').alias('totalcost'), sum('margin').alias('totalmargin')).withColumn('marginpercent', col('totalmargin') / col('totalsales') * 100).display()


# COMMAND ----------

# from pyspark.sql.functions import sum, col, round

# result_df = df_joined1.groupBy('ProductId', 'ProductName') \
#     .agg(
#         sum('salesamount').alias('totalsales'),
#         sum('costamount').alias('totalcost'),
#         sum('margin').alias('totalmargin')
#     ) \
#     .withColumn('marginpercent', col('totalmargin') / col('totalsales') * 100, 4) \
#     .select('ProductId', 'ProductName', 'totalsales', 'totalcost', 'totalmargin', 'marginpercent')

# display(result_df)

# COMMAND ----------



result_df = df_joined1.groupBy('ProductId', 'ProductName') \
    .agg(
        sum('salesamount').alias('totalsales'),
        sum('costamount').alias('totalcost'),
        sum('margin').alias('totalmargin')
    ) \
    .withColumn('marginpercent', round(col('totalmargin') / col('totalsales') * 100, 4)) \
    .select('ProductId', 'ProductName', 'totalsales', 'totalcost', 'totalmargin', 'marginpercent')

display(result_df)

# read_count = result_df.count()

# COMMAND ----------

# spark.sql("drop table if exists hive_metastore.gold_kuppala.final_output")

# COMMAND ----------

# MAGIC %md ##### Creating gold DataBase

# COMMAND ----------

spark.sql("create database if not exists gold_kuppala")

# COMMAND ----------

# MAGIC %md ##### Loading data into gold layer

# COMMAND ----------

try:
    result_df.write.format("delta").mode("overwrite").saveAsTable("gold_kuppala.final_data")
except Exception as e:
    print(f'Error writing to table: {e}')