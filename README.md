# What is bdq?
BDQ stands for Big Data Quality, a set of tools/function that help you every day assert quality of datasets you have processed or ingested. Library leverages power of spark, hence it processes your quality checks at scale offered by spark and databricks.

## Comparing dataframe schemas
```python
import bdq
from datetime import datetime
import pyspark.sql.functions as F

df2 = spark.createDataFrame([
[ 1, 1, "Grzegorz", datetime(2018, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.9, 123234234345, True],
[ 3, 1, "Mike",     datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
[ 2, 2, "Timmy",    datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 8754857845, True]
], schema)

df2_changed = df2 \
  .drop('first_login_dt') \
  .withColumn('new_data', F.lit(None).cast('date')) \
  .withColumn('likes', F.col('likes').cast('int'))

assert bdq.compare_schemas(df2.schema, df2_changed.schema) == {
  'added': {'first_login_dt'}, 
  'removed': {'new_data'}, 
  'changed': {
    'likes': {'before': 'bigint', 'after': 'int'}
  },
  'not_changed': {
    'name', 'id1', 'last_login_ts', 'id2', 'credits', 'active'
  }
}
```

## Comparing dataframe's data

```python
import bdq

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

#returns dicts with added, removed, changed and not changed dataframes, and record counts
df_diff = bdq.compare_dataframes(df1, df2, ['id1', 'id2'], True)

#show all info about compare results
bdq.display_compare_dataframes_results(df_diff)

>> Added records count: 1
>> +---+---+-----+--------------+-------------------+-------+----------+------+
>> |id1|id2|name |first_login_dt|last_login_ts      |credits|likes     |active|
>> +---+---+-----+--------------+-------------------+-------+----------+------+
>> |2  |2  |Timmy|2018-01-01    |2018-02-01 12:34:56|36.7   |8754857845|true  |
>> +---+---+-----+--------------+-------------------+-------+----------+------+
>> 
>> Removed records count: 1
>> +---+---+----+--------------+-------------------+-------+-----+------+
>> |id1|id2|name|first_login_dt|last_login_ts      |credits|likes|active|
>> +---+---+----+--------------+-------------------+-------+-----+------+
>> |2  |1  |Tim |2018-01-01    |2018-02-01 12:34:56|36.7   |54545|true  |
>> +---+---+----+--------------+-------------------+-------+-----+------+
>> 
>> Changed records count: 1
>> +---+---+---------------------------------------------------------------------+
>> |id1|id2|changed                                                              |
>> +---+---+---------------------------------------------------------------------+
>> |1  |1  |{first_login_dt -> {2017-01-01, 2018-01-01}, credits -> {26.7, 26.9}}|
>> +---+---+---------------------------------------------------------------------+

Not changed records count: 1
```

## Surrogate key generation
with support of optional uppercasing and trimming of key columns

```python
from datetime import datetime
from bdq.functions import surrogate_key_hash, surrogate_key_string

schema = "id1:long,id2:long,name:string,likes:int"

df1 = spark.createDataFrame([
  [ 1, 1, "GrzeGorz", 1 ],
  [ 1, 1, "Grzegorz",  1 ],
  [ 1, 1, "Grzegorz      ",  1 ],
  [ 1, None, "Grzegorz", 1],
  [ 1, None, None, 1],
  [ 2, 1, "Tim", 1 ],
  [ 3, 1, "Mike", 1 ]
], schema)

sk_df = df1.select(
  '*', 
  surrogate_key_hash(['id1', 'id2', 'name'], rtrim=True).alias('hash'),
  surrogate_key_string(['id1', 'id2', 'name'], rtrim=True).alias('hash_input')
)

>> +---+----+--------------+-----+-------------------------------------------------------------+---------------------------+
>> |id1|id2 |name          |likes|hash                                                         |hash_input                 |
>> +---+----+--------------+-----+-------------------------------------------------------------+---------------------------+
>> |1  |1   |GrzeGorz      |1    |[6F 21 99 99 4C F2 93 56 2E 7C C3 29 F9 6A 42 2F 6D 62 EC 4B]|[1, 1, GRZEGORZ]           |
>> |1  |1   |Grzegorz      |1    |[6F 21 99 99 4C F2 93 56 2E 7C C3 29 F9 6A 42 2F 6D 62 EC 4B]|[1, 1, GRZEGORZ]           |
>> |1  |1   |Grzegorz      |1    |[6F 21 99 99 4C F2 93 56 2E 7C C3 29 F9 6A 42 2F 6D 62 EC 4B]|[1, 1, GRZEGORZ]           |
>> |1  |null|Grzegorz      |1    |[4E 9E 3A 2B C8 37 90 A0 6A 26 9A FE 7A 78 8B CA 99 34 68 1C]|[1, @~<null>~@, GRZEGORZ]  |
>> |1  |null|null          |1    |[44 FF 88 2E 2A 28 3B EE EE 53 C8 7D 0F 92 D1 68 3E A2 F2 E5]|[1, @~<null>~@, @~<null>~@]|
>> |2  |1   |Tim           |1    |[A8 7A B0 29 94 9F 5C D1 78 D4 28 89 8B F1 75 CD AC 70 B6 61]|[2, 1, TIM]                |
>> |3  |1   |Mike          |1    |[9D 6C 76 C5 17 D0 44 46 47 1A 71 ED 4A 08 75 84 1D 5C 45 09]|[3, 1, MIKE]               |
>> +---+----+--------------+-----+-------------------------------------------------------------+---------------------------+
```

## PK & FK intergrity checks
with support of showing sample data from fact table that did not satisfy integrity

```python
from bdq import fact_dim_broken_relationship
from bdq.functions import surrogate_key_hash, surrogate_key_string

fact_df = spark.createDataFrame([
    [ "Grzegorz", "IT", "EU" ],
    [ "Mark", "IT", "EU" ],
    [ "Justin", "IT  ", "EU    " ],
    [ "Alice1", "IT", "USA" ],
    [ "Alice2", "IT", "USA" ],
    [ "Alice3", "IT", "USA" ],
    [ "Alice4", "IT", "USA" ],
    [ "Alice5", "IT", "USA" ],
    [ "Bob", "Sales", "USA" ],
    [ "Bob2", "Sales", "USA" ],
    [ "Bob3", "Sales", "USA" ]
  ], "Name:string,Dept:string,Country:string") \
  .withColumn("dept_fk", surrogate_key_hash(["Dept", "Country"], rtrim=True))

dim_df = spark.createDataFrame([
    ["IT", "EU", "European IT department"],
    ["Sales", "USA", "USA Sales dept."]
  ], "department:string,cntry:string,comment:string") \
  .withColumn("dept_pk", surrogate_key_hash(["department", "cntry"], rtrim=True))

#direct column comparison, hence upper, lower, extra spaces affect the results
broken_df = fact_dim_broken_relationship(
  fact_df=fact_df,
  fk_columns=["Dept", "Country"],
  dim_df=dim_df,
  pk_columns=["department", "cntry"],
  sample_broken_records=2
)

>> +----+-------+------------------------------------------------------------------------------------+
>> |Dept|Country|sample_records                                                                      |
>> +----+-------+------------------------------------------------------------------------------------+
>> |IT  |USA    |[{IT, USA, Alice1, ��xz4q_��\ra�-��Vz}, {IT, USA, Alice2, ��xz4q_��\ra�-��Vz}]|
>> |IT  |EU     |[{IT  , EU    , Justin, ��H'V4�\rY,g�I�����}]                                      |
>> +----+-------+------------------------------------------------------------------------------------+

# using surrogate keys will yield better results, because extra spaces are trimmed
broken_sk_df = fact_dim_broken_relationship(
  fact_df, ["dept_fk"],
  dim_df, ["dept_pk"],
  sample_broken_records=2
)

>> +----+-------+------------------------------------------------------------------------------------+
>> |Dept|Country|sample_records                                                                      |
>> +----+-------+------------------------------------------------------------------------------------+
>> |IT  |USA    |[{IT, USA, Alice1, ��xz4q_��\ra�-��Vz}, {IT, USA, Alice2, ��xz4q_��\ra�-��Vz}]|
>> +----+-------+------------------------------------------------------------------------------------+
```

## Get latest records 
with optional primary key conflict resolution, where there are multiple records being candidate for latest record, but they have different attribuets and there is no way of determining which one is the latest.

```python
from datetime import datetime as dt
from bdq import get_latest_records, get_latest_records_with_pk_confict_detection_flag

increment_data =  [
  (1, dt(2000, 1, 1, 0, 0, 0), "1001")
  ,(1, dt(2000, 1, 1, 2, 0, 0), "1002")
  ,(2, dt(2000, 1, 1, 0, 0, 0), "2001") #carbon copy duplicate
  ,(2, dt(2000, 1, 1, 0, 0, 0), "2001") #carbon copy duplicate
  ,(3, dt(2000, 1, 1, 0, 0, 0), "3001")
  ,(3, dt(2000, 1, 1, 2, 0, 0), "3002#1") #pk conflict, two entries with the same technical fields, but different atributes, which one to chose?
  ,(3, dt(2000, 1, 1, 2, 0, 0), "3002#2") #pk conflict
]

df = spark.createDataFrame(increment_data, "pk:int,change_ts:timestamp,attr:string")
df.show(truncate=True)

>> +---+-------------------+------+
>> | pk|          change_ts|  attr|
>> +---+-------------------+------+
>> |  1|2000-01-01 00:00:00|  1001|
>> |  1|2000-01-01 02:00:00|  1002|
>> |  2|2000-01-01 00:00:00|  2001|
>> |  2|2000-01-01 00:00:00|  2001|
>> |  3|2000-01-01 00:00:00|  3001|
>> |  3|2000-01-01 02:00:00|3002#1|
>> |  3|2000-01-01 02:00:00|3002#2|
>> +---+-------------------+------+

primary_key_columns = ['pk']
order_by_columns = ['change_ts']

# will randomly chose one pk in case of conflict, without any notification about conflicts
simple_get_latest_df = get_latest_records(df, primary_key_columns, order_by_columns)
simple_get_latest_df.show(truncate=True)

>> +---+-------------------+------+
>> | pk|          change_ts|  attr|
>> +---+-------------------+------+
>> |  1|2000-01-01 02:00:00|  1002|
>> |  2|2000-01-01 00:00:00|  2001|
>> |  3|2000-01-01 02:00:00|3002#1|
>> +---+-------------------+------+

# will flag records which cause conflicts and cannot be resolved
# bad records need to be quarantined and handled in special process
conflict_get_latest_df = get_latest_records_with_pk_confict_detection_flag(df, primary_key_columns, order_by_columns)
conflict_get_latest_df.show(truncate=True)

>> +---+-------------------+------+-----------------+
>> | pk|          change_ts|  attr|__has_pk_conflict|
>> +---+-------------------+------+-----------------+
>> |  1|2000-01-01 02:00:00|  1002|            false|
>> |  2|2000-01-01 00:00:00|  2001|            false|
>> |  3|2000-01-01 02:00:00|3002#2|             true|
>> |  3|2000-01-01 02:00:00|3002#1|             true|
>> +---+-------------------+------+-----------------+

```

## Find composite primary key candidates
Given list of possible columns, constructs the lists of all possible combinations of composite primary keys, and executes concurrently to determine if given set of columns is a valid primary key. Uses minimum possible amount of queries against spark by skipping validation paths that are based on already validated primary key combinations.

```python
import bdq
import pyspark.sql.functions as F

df = spark.range(0, 100) \
  .withColumn('type', F.expr('(id / 10)::int + 1')) \
  .withColumn('reminder', F.expr('id % 10')) \
  .withColumn('static', F.lit('A')) \
  .withColumn('round_robin', F.expr('id % 2'))

display(df)

all_combinations = list(bdq.get_column_names_combinations(df.columns))

bdq.validate_primary_key_candidate_combinations(df, all_combinations, max_workers=10, verbose=True)
>> [('id',), ('type', 'reminder')]
```

## Execute DAG having nodes as python functions
```python
import bdq
import time

graph = bdq.DAG()

@graph.node()
def a():
  time.sleep(2)

@graph.node()
def b():
  time.sleep(3)
  return "beeep"

@graph.node(b)
def c():
  time.sleep(5)

@graph.node(b, c, a)
def d():
  time.sleep(7)
  return "g man"

@graph.node(a)
def e():
  time.sleep(3)
  raise ValueError("omg, crash!")

@graph.node(e)
def f():
  print("this will never execute")

graph.execute(max_workers=10)

for node, state in graph.nodes.items():
  print(f"{node}: {state.result=}, {state.completed.is_set()=}, {state.exception=}")

>> Waiting for all tasks to finish...
>>   starting: <function a at 0x7f9a248c1ca0>
>>   starting: <function b at 0x7f9a248c1a60>
>>   starting: <function e at 0x7f9a248c18b0>
>>   finished: <function a at 0x7f9a248c1ca0>, result: None (still running: 2)
>>   starting: <function c at 0x7f9a248c19d0>
>>   finished: <function b at 0x7f9a248c1a60>, result: beeep (still running: 2)
>>   error: <function e at 0x7f9a248c18b0>: omg, crash! (still running: 1)
>>   starting: <function d at 0x7f9a248c1940>
>>   finished: <function c at 0x7f9a248c19d0>, result: None (still running: 1)
>>   finished: <function d at 0x7f9a248c1940>, result: d man (still running: 0)
>> All tasks finished, shutting down
>> <function a at 0x7f9a248c1ca0>: state.result=None, state.completed.is_set()=True, state.exception=None
>> <function b at 0x7f9a248c1a60>: state.result='beeep', state.completed.is_set()=True, state.exception=None
>> <function c at 0x7f9a248c19d0>: state.result=None, state.completed.is_set()=True, state.exception=None
>> <function d at 0x7f9a248c1940>: state.result='g man', state.completed.is_set()=True, state.exception=None
>> <function e at 0x7f9a248c18b0>: state.result=None, state.completed.is_set()=True, state.exception=ValueError('omg, crash!')
>> <function f at 0x7f9a248c1820>: state.result=None, state.completed.is_set()=False, state.exception=None
```

## Execute pipeline of multiple steps
using DAG component to handle pararel execution
```python
import bdq

ppn = bdq.Pipeline(spark, "retail")

# returns dataframe, and creates spark view 'raw_data_single_source'
@ppn.step()
def raw_data_single_source(p):
  return spark.range(1, 10)

# returns dataframe, and creates spark view 'raw_nice_name'
@ppn.step(returns=["raw_nice_name"])
def raw_data_single_source_with_custom_name(p):
  return spark.range(100, 110)

# returns two dataframes, and creates two spark views 'raw_data1', 'raw_data2'
@ppn.step(returns=["raw_data1", "raw_data2"])
def raw_data_multi_source(p):
  df1 = spark.range(1000, 2000)
  df2 = spark.range(2000, 3000)

  return [df1, df2]

# waits for raw data sources to finish, and combines the data into one unioned view `combine_data`
# note that dependencies are python functions, not names of views (TODO: to handle view names as well)
@ppn.step(depends_on=[raw_data_single_source, raw_data_single_source_with_custom_name, raw_data_multi_source])
def combine_data(p):
  df = table('raw_data_single_source') \
    .union(table('raw_nice_name')) \
    .union(table('raw_data1')) \
    .union(table('raw_data2'))

  return df

# splits the combined_data into 'odds' and 'even' views
@ppn.step(depends_on=[combine_data], returns=['odds', 'even'])
def split_data(p):
  df_odd = table('combine_data').filter('id % 2 == 1')
  df_even = table('combine_data').filter('id % 2 == 0')

  return [ df_odd, df_even ]

#execute pipeline, returns map of all steps with their results (dataframes they returned), or exceptions if failed
ppn()

#show some final values
print('even numbers:')
print(table('even').limit(10).collect())
print('odd numbers:')
print(table('odd').limit(10).collect())

>> Waiting for all tasks to finish...
>>  starting: raw_data_single_source() -> ['raw_data_single_source']
>>  starting: raw_data_single_source_with_custom_name() -> ['raw_nice_name']
>>  starting: raw_data_multi_source() -> ['raw_data1', 'raw_data2']
>>  finished: raw_data_single_source() -> ['raw_data_single_source'] (still running: 2)
>>  finished: raw_data_single_source_with_custom_name() -> ['raw_nice_name'] (still running: 1)
>>  starting: combine_data() -> ['combine_data']
>>  finished: raw_data_multi_source() -> ['raw_data1', 'raw_data2'] (still running: 1)
>>  starting: split_data() -> ['odds', 'even']
>>  finished: combine_data() -> ['combine_data'] (still running: 1)
>>  finished: split_data() -> ['odds', 'even'] (still running: 0)
>>All tasks finished, shutting down
>>even numbers:
>>[Row(id=2), Row(id=4), Row(id=6), Row(id=8), Row(id=100), Row(id=102), Row(id=104), Row(id=106), Row(id=108), Row(id=1000)]
>>odd numbers:
>>[Row(id=1), Row(id=3), Row(id=5), Row(id=7), Row(id=9), Row(id=101), Row(id=103), Row(id=105), Row(id=107), Row(id=109)]
```