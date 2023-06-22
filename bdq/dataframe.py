__all__ = [ 
  'compare_dataframes',
  'uncache_compare_dataframes_results', 
  'display_compare_dataframes_results', 
  'fact_dim_broken_relationship',
  'get_latest_records_window',
  'get_latest_records',
  'get_latest_records_with_pk_confict_detection_flag'
]

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

def compare_dataframes(df1:DataFrame, df2:DataFrame, key_columns:list[str], cache_results=False) -> DataFrame:
  df1 = df1.alias('df1')
  df2 = df2.alias('df2')

  df3 = df1.join(df2, key_columns, "full_outer")
  df1_only_records = df3.filter(f"df2.{key_columns[0]} is null").select('df1.*')
  df2_only_records = df3.filter(f"df1.{key_columns[0]} is null").select('df2.*')

  shared_records = df3.filter(f"df1.{key_columns[0]} is not null AND df2.{key_columns[0]} is not null") 

  diff_cols_keys = []
  diff_cols_values = []

  for name in shared_records.select('df1.*').columns:
    if name in key_columns:
      continue

    has_changes = F.expr(f"NOT(df1.{name} <=> df2.{name})").alias('has_changes')

    key = F.when(has_changes, F.lit(name))
    value = F.when(has_changes,
      F.struct(
        F.col(f"df1.{name}").cast('string').alias('before'),
        F.col(f"df2.{name}").cast('string').alias('after')
      )
    )

    diff_cols_keys.append(key)
    diff_cols_values.append(value)

  df_compare = shared_records.select(
    *key_columns,
    F.map_from_arrays(
      F.array_compact(F.array(diff_cols_keys)),
      F.array_compact(F.array(diff_cols_values))
    ).alias('changed')
  )

  if cache_results:
    df2_only_records = df2_only_records.cache()
    df1_only_records = df1_only_records.cache()
    df_compare = df_compare.cache()

  df_changed = df_compare.filter('changed is not null and size(changed) > 0')
  df_not_changed = df_compare.filter('changed is null or size(changed) == 0')

  if cache_results:
    df_changed = df_changed.cache()
    df_not_changed = df_not_changed.cache()

  res = {
    'added': df2_only_records
    ,'added_count': df2_only_records.count()
    ,'removed': df1_only_records
    ,'removed_count': df1_only_records.count()
    ,'changed': df_changed
    ,'changed_count': df_changed.count()
    ,'not_changed': df_not_changed
    ,'not_changed_count': df_not_changed.count()
  }

  if cache_results:
    df_changed.unpersist()

  return res

def uncache_compare_dataframes_results(d:dict):
  for k, v in d.items():
    if isinstance(v, DataFrame):
      v.unpersist()

def display_compare_dataframes_results(df_diff:dict, show_added_records=True, show_removed_records=True, show_changed_records=True, show_not_changed_records=False, display_function=None):
  def _display(df):
    if display_function:
      display_function(df)
    else:
      df.show(truncate=False)
  
  print('Added records count:', df_diff['added_count'])
  if show_added_records:
    _display(df_diff['added'])

  print('Removed records count:', df_diff['removed_count'])
  if show_removed_records:
    _display(df_diff['removed'])

  print('Changed records count:', df_diff['changed_count'])
  if show_changed_records:
    _display(df_diff['changed'])

  print('Not changed records count:', df_diff['not_changed_count'])
  if show_not_changed_records:
    _display(df_diff['not_changed'])

def fact_dim_broken_relationship(fact_df, fk_columns, dim_df, pk_columns, sample_broken_records=3):
  if len(pk_columns) != len(fk_columns):
    raise ValueError("pk_columns count must match fk_columns count")
  
  distinct_fact_df = fact_df.select(fk_columns).distinct().alias('f')
  distinct_dim_df = dim_df.select(pk_columns).distinct().alias('d')

  join_expr = [
    F.col(pk) == F.col(fk)
    for pk, fk in zip(pk_columns, fk_columns)
  ]

  broken_df = distinct_fact_df.join(distinct_dim_df, join_expr, 'left_anti').select('f.*')

  if not sample_broken_records:
    return broken_df
  
  sampled_broken_df = fact_df.alias('f') \
    .join(broken_df.alias('b'), fk_columns, 'inner') \
    .select('f.*') \
    .withColumn('__row_number', F.row_number().over(Window.partitionBy(fk_columns).orderBy(F.lit(1)))) \
    .filter(F.col('__row_number') <= sample_broken_records) \
    .drop('__row_number') \
    .groupBy(fk_columns) \
    .agg(F.collect_list(F.struct('*')).alias('sample_records'))

  return sampled_broken_df

def get_latest_records_window(primary_key_columns, order_by_columns):
  return Window \
    .partitionBy(primary_key_columns) \
    .orderBy([
      F.col(c).desc() 
      for c in order_by_columns
    ])

def get_latest_records(df, primary_key_columns, order_by_columns):
  latest_records_windows_spec = get_latest_records_window(primary_key_columns, order_by_columns)

  df2 = df.withColumn('__row_number', F.row_number().over(latest_records_windows_spec)) \
    .filter('__row_number == 1') \
    .drop('__row_number')

  return df2

def get_latest_records_with_pk_confict_detection_flag(df, primary_key_columns, order_by_columns):
  latest_records_windows_spec = get_latest_records_window(primary_key_columns, order_by_columns)

  checks_df = df.distinct() \
    .withColumn('__row_number', F.row_number().over(latest_records_windows_spec)) \
    .withColumn('__dense_rank', F.dense_rank().over(latest_records_windows_spec)) \
    .filter('__dense_rank == 1') \
    .withColumn('__conflict', F.col('__row_number') != 1)

  conflict_pk_df = checks_df.filter('__conflict == TRUE') \
    .select(primary_key_columns) \
    .distinct()

  join_expr = [
    F.col(f'checks.{c}') == F.col(f'conflicts.{c}')
    for c in primary_key_columns
  ]

  final_df = checks_df.alias('checks') \
    .drop('__conflict', '__row_number', '__dense_rank') \
    .join(conflict_pk_df.alias('conflicts'), join_expr, 'left_outer') \
    .selectExpr('checks.*', f'case when conflicts.{primary_key_columns[0]} is not null then TRUE else FALSE end as __has_pk_conflict')

  return final_df
