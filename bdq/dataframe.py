import pyspark.sql.functions as F
from pyspark.sql import DataFrame

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

  df_changed = df_compare.filter('changed is not null')
  df_not_changed = df_compare.filter('changed is null')

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

def display_compare_dataframes_results(df_diff:dict, show_added_records=True, show_removed_records=True, show_changed_records=True, show_not_changed_records=False):
  print('Added records count:', df_diff['added_count'])
  if show_added_records:
    display(df_diff['added'])

  print('Removed records count:', df_diff['removed_count'])
  if show_removed_records:
    display(df_diff['removed'])

  print('Changed records count:', df_diff['changed_count'])
  if show_changed_records:
    display(df_diff['changed'])

  print('Not changed records count:', df_diff['not_changed_count'])
  if show_not_changed_records:
    display(df_diff['not_changed'])