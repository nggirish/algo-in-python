# Databricks notebook source
from pyspark.sql.functions import sum as sum_, lag, col, coalesce, lit
from pyspark.sql.window import Window

# COMMAND ----------

df = sc.parallelize([
    (217498, 100000001, 'A'), (217498, 100000025, 'A'), (217498, 100000124, 'A'),
    (217498, 100000152, 'B'), (217498, 100000165, 'C'), (217498, 100000177, 'C'),
    (217498, 100000182, 'A'), (217498, 100000197, 'B'), (217498, 100000210, 'B'),
    (854123, 100000005, 'A'), (854123, 100000007, 'A')
]).toDF(["user_id", "timestamp", "actions"])

# COMMAND ----------

display(df)

# COMMAND ----------

w = Window.partitionBy("user_id").orderBy("timestamp")

# COMMAND ----------

is_first = coalesce(
  (lag("actions", 1).over(w) != col("actions")).cast("bigint"),
  lit(1)
)

# COMMAND ----------

def test_f():
  return "test"

# COMMAND ----------

order = sum_("is_first").over(w)

# COMMAND ----------

(df
 .withColumn("lag_c",(lag("actions", 1).over(w) != col("actions")))
 .withColumn("is_first", is_first)
 .withColumn("order", order)
 ).show()

# COMMAND ----------

display(df
    .withColumn("is_first", is_first)
    .withColumn("order", order)
    .groupBy("user_id", "actions","order")
    .count())

# COMMAND ----------

help(sqlContext.read)

# COMMAND ----------

