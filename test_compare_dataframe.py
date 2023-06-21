import bdq
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

schema_diff = bdq.compare_schemas(df1.schema, df2.schema)
assert not schema_diff['added'] and not schema_diff['removed'] and not schema_diff['changed']

df_diff = bdq.compare_dataframes(df1, df2, ['id1', 'id2'], True)