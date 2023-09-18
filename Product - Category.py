# Databricks notebook source
# MAGIC %md
# MAGIC # Создадим необходимые для задания датафреймы 

# COMMAND ----------

from pyspark.sql.functions import col, explode, split, when

# COMMAND ----------

df_products = spark.createDataFrame([
    (1, "Cat"),
    (2, "Dog"),
    (3, "Pen"),
    (4, "Paper"),
    (5, "Rock"),
    (6, "Scissor"),
    (7, "Shrek"),
    (8, "Ball"),
    (9, "PS4"),
    (10, "Coffee")
], ["product_id", "product_name"])

df_categories = spark.createDataFrame([
    (1, "Animals"),
    (2, "Toys"),
    (3, "Misc"),
    (4, "Music"),
    (5, "Fun"),
    (6, "Films"),
    (7, "Games"),
    (8, "Food"),
    (9, "Tech"),
    (10, "Drinks")
], ["category_id", "category_name"])

df_relationships = spark.createDataFrame([
    ("1", "1"),
    ("2", "2"),
    ("3", "3"),
    ("4", None),
    ("5", "4 5"),
    ("6", "6 7"),
    ("7", "7 8"),
    ("8", "8 9"),
    ("9", "9 10"),
    ("10", "10 1 2")
], ["product_id", "category_ids"])

df_products.show()
df_categories.show()
df_relationships.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Видоизменим df_relationships, разбив каждую строку со множестовом category_ids (one-to-many) так, чтобы у нас появились пары product_id - category_id (one-to-one)

# COMMAND ----------

df_relationships = df_relationships.withColumn("category_id", explode(split(col("category_ids"), "\\s+"))).drop("category_ids")

df_relationships.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Объеденим все таблицы в одну по id и выберем из нее нужные нам столбцы 

# COMMAND ----------

df_product_category = df_products.join(df_relationships, on="product_id", how="left") \
                      .join(df_categories, on="category_id", how="left") \
                      .select(col("product_name"), col("category_name"))

df_product_category.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Обработаем null значения в category_name

# COMMAND ----------

df_product_category = df_product_category.withColumn("category_name", when(col("category_name").isNull(), "No Category").otherwise(col("category_name")))
display(df_product_category)

# COMMAND ----------

# MAGIC %md
# MAGIC # Таблица готова
