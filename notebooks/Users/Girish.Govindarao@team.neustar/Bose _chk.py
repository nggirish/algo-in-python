# Databricks notebook source
# MAGIC %md
# MAGIC #### [Specifications](https://jira.marketshare.com/confluence/display/DBS/DataBricks+coding+guidelines)
# MAGIC ##### Project: XYZ Project
# MAGIC ##### Description: Variable Group
# MAGIC Author: Your Name
# MAGIC ###### Release History
# MAGIC * 2018-02-21 Bug Fix 101
# MAGIC * 2018-02-20 Initial Version

# COMMAND ----------

from pyspark.sql.functions import *
from dmutil import *

# COMMAND ----------

align_date(align_to="SU", align_BeginEnd="B")

# COMMAND ----------

dbutils.widgets.text('from_date','1900-01-01')
dbutils.widgets.text('to_date','1900-01-01')
dbutils.widgets.text('path_prefix','/mnt/s3/prod')
dbutils.widgets.text('in_path','%s/<client_project>/<phase>/<variablegroup>/<raw|scm|src>/')
dbutils.widgets.text('lkp_path','%s/bose_bose_us_eu_strategy/lkp/map_valid.csv')
dbutils.widgets.text('out_path','%s/<client_project>/<phase>/out/<pre|trn>_<variablegroup>.csv')

# COMMAND ----------

from_date = dbutils.widgets.get("from_date")
to_date = dbutils.widgets.get("to_date")
path_prefix = dbutils.widgets.get("path_prefix")

in_f1 = dbutils.widgets.get("in_path") % path_prefix
in_lkp = dbutils.widgets.get("lkp_path") % path_prefix
out_file = dbutils.widgets.get("out_path") % path_prefix

# COMMAND ----------

df_in = sqlContext.read.format('csv').options(header='true').load(in_f1).selectExpr(
  "X_DT",
  "X_GEO",
  "X_PRD",
  "X_CH",
  "O_UNI",
  "O_REV"
)


df_lkp = sqlContext.read.format('csv').options(header='true').load(in_lkp).selectExpr(
  "X_CH",	
  "X_PRD",	
  "X_GEO",	
  "Value"
)

df_in.createOrReplaceTempView("sales")
df_lkp.createOrReplaceTempView("lkp")

# COMMAND ----------

df_out = sql(
"""
with sales_summary
(
select
  a.x_dt,
  a.x_geo,
  a.x_prd,
  a.x_ch,
  b.value,
  sum (a.o_uni) as o_uni,
  sum (a.o_rev) as o_rev
from 
  sales a
  left join 
  lkp  b
on
  a.x_ch=b.x_ch
  and a.x_prd=b.x_prd
  and a.x_geo=b.x_geo
group by 1, 2, 3, 4, 5
)
select
  X_DT, 
  X_GEO,
  X_PRD,
  X_CH,
  case when o_rev < 0 then 0 else o_rev * value end as O_REV,
  case when o_uni < 0 then 0 else o_uni * value end as O_UNI
from
  sales_summary  
"""
)


# COMMAND ----------

df_out = to_skinny(df_out).where('value is not null')
df_out = fill_gap(df_out,ts_start=from_date, ts_end=to_date, align_to='SU', align_BeginEnd='B', fill_method='forward')
df_out = fill_gap(df_out,ts_start=from_date, ts_end=to_date, align_to='SU', align_BeginEnd='B', fill_method='backward', stack_format='W')

# COMMAND ----------

display(df_out)

# COMMAND ----------

chk = checksum(df_out, df_in, metric=['M1','M2'], metric_in=['m1+m2+m3','m4'], metric_raw=['IMPRESSION','SPEND']).collect()
if chk:
  raise ValueError("Checksum failed %s" % chk)
print("Checksum Passed")

# COMMAND ----------

write_output(df_out,out_file,dbutils)