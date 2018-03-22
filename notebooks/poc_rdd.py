# Databricks notebook source
from itertools import groupby

# COMMAND ----------

df = sc.parallelize([
    (217498, 100000001, 'A'), (217498, 100000025, 'A'), (217498, 100000124, 'A'),
    (217498, 100000152, 'B'), (217498, 100000165, 'C'), (217498, 100000177, 'C'),
    (217498, 100000182, 'A'), (217498, 100000197, 'B'), (217498, 100000210, 'B'),
    (854123, 100000005, 'A'), (854123, 100000007, 'A')
]).toDF(["user_id", "timestamp", "actions"])

# COMMAND ----------

 display(df.rdd.map(lambda row: (row.user_id, row))
    .groupByKey().toDF(['key','value']))

# COMMAND ----------

def recalculate(records):
  actions = [r.actions for r in sorted(records[1], key=lambda r: r.timestamp)]
  groups = [list(g) for k, g in groupby(actions)]
  return [(records[0], g[0], len(g), i+1) for i, g in enumerate(groups)]

# COMMAND ----------

 display(df.rdd.map(lambda row: (row.user_id, row))
    .groupByKey().flatMap(recalculate)
    .toDF(['user_id', 'actions', 'nf_of_occ', 'order']))

# COMMAND ----------

range(1,4)

# COMMAND ----------

