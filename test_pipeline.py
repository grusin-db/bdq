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

# returns two dataframes, and creates two spark views raw_data1, raw_data2
@ppn.step(returns=["raw_data1", "raw_data2"])
def raw_data_multi_source(p):
  df1 = spark.range(1000, 2000)
  df2 = spark.range(2000, 3000)

  return [df1, df2]

# waits for raw data sources to finish, and combines the data into one unioned view of name `combine_data`
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
print(table('odds').limit(10).collect())

