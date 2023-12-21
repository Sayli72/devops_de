# Databricks notebook source
display(dbutils.fs.ls("/mnt/globalmart_clean_data/bronze/"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Products Data

# COMMAND ----------

products_json = spark.read.format("json").option("multiLine","true").load("/mnt/globalmart_clean_data/bronze/products.json")

# COMMAND ----------

# products_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read customers reviws data

# COMMAND ----------

customer_reviews_df = spark.read.format("csv").option("header","true").load("/mnt/globalmart_clean_data/bronze/customer_reviews.csv")

# COMMAND ----------

# customer_reviews_df.display()
