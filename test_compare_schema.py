import bdq
from bdq import spark
from datetime import datetime
import pyspark.sql.functions as F

schema = "id1:long,id2:long,name:string,first_login_dt:date,last_login_ts:timestamp,credits:float,likes:long,active:boolean"

df1 = spark.createDataFrame([
[ 1, 1, "Grzegorz", datetime(2017, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.7, 123234234345, True],
[ 2, 1, "Tim",      datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 54545,      True],
[ 3, 1, "Mike",     datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False]
], schema)

df2 = spark.createDataFrame([
[ 1, 1, "Grzegorz", datetime(2018, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.9, 123234234345, True],
[ 3, 1, "Mike",     datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
[ 2, 2, "Timmy",    datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 8754857845, True]
], schema)

assert bdq.schema_compare(df1.schema, df2.schema) == {
  'added': set(), 
  'removed': set(), 
  'changed': {}, 
  'not_changed': {
    'likes', 'name', 'id1', 'last_login_ts', 'id2', 'credits', 'active', 'first_login_dt'
    }
  }

df2_changed = df2 \
  .drop('first_login_dt') \
  .withColumn('new_data', F.lit(None).cast('date')) \
  .withColumn('likes', F.col('likes').cast('int'))

assert bdq.schema_compare(df2.schema, df2_changed.schema) == {
  'added': {'first_login_dt'}, 
  'removed': {'new_data'}, 
  'changed': {
    'likes': {'before': 'bigint', 'after': 'int'}
  },
  'not_changed': {
    'name', 'id1', 'last_login_ts', 'id2', 'credits', 'active'
  }
}