import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def dataframe_compare(df1:DataFrame, df2:DataFrame, key_columns:list[str]) -> DataFrame:
  
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

  return {
    'added': df2_only_records
    ,'removed': df1_only_records
    ,'changed': df_compare.filter('changed is not null')
    ,'not_changed': df_compare.filter('changed is null')
  }