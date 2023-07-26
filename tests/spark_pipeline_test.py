import bdq
from bdq import spark, table

def test_step_spark_temp_view():
  ppn = bdq.SparkPipeline("sample")

  # returns dataframe, and creates spark view 'raw_data_single_source'
  @ppn.step_spark_temp_view()
  def raw_data_single_source(step):
    return spark.range(1, 10)

  # returns dataframe, and creates spark view 'raw_nice_name'
  @ppn.step_spark_temp_view(outputs="raw_nice_name")
  def raw_data_single_source_with_custom_name(step):
    return spark.range(100, 110)

  # returns two dataframes, and creates two spark views 'raw_data1', 'raw_data2'
  @ppn.step_spark_temp_view(outputs=["raw_data1", "raw_data2"])
  def raw_data_multi_source(step):
    df1 = spark.range(1000, 2000)
    df2 = spark.range(2000, 3000)

    return [df1, df2]

  # waits for raw data sources to finish, and combines the data into one unioned view `combine_data`
  # note that dependencies are python functions or names of outputs
  @ppn.step_spark_temp_view(depends_on=[raw_data_single_source, raw_data_single_source_with_custom_name, 'raw_data1', 'raw_data2'])
  def combine_data(step):
    df = table('raw_data_single_source') \
      .union(table('raw_nice_name')) \
      .union(table('raw_data1')) \
      .union(table('raw_data2'))

    return df

  # splits the combined_data into 'odd' and 'even' views
  @ppn.step_spark_temp_view(depends_on=combine_data, outputs=['odd', 'even'])
  def split_data(step):
    df_odd = table('combine_data').filter('id % 2 == 1')
    df_even = table('combine_data').filter('id % 2 == 0')

    return [ df_odd, df_even ]

  # executes pipeline using concurrent threads, one per each step, following the dependency DAG
  # pipeline is a normal python callable object, as if it was a function, it returns a list of all steps
  pipeline_results = ppn(max_concurrent_steps=10)

  print('pipeline results:')
  print(pipeline_results)

  #show some final values
  print('even numbers:')
  print(table('even').limit(10).collect())
  print('odd numbers:')
  print(table('odd').limit(10).collect())

  #get skipped steps
  assert list(ppn.skipped_steps) == []

  #get errored steps (you would need to 'adjust' code of on of the steps to make it fail to see something here)
  assert list(ppn.error_steps) == []

  #get successfull steps
  assert set(ppn.success_steps.values()) == set([
    raw_data_single_source_with_custom_name,
    raw_data_multi_source,
    split_data,
    combine_data,
    raw_data_single_source
  ])

if __name__ == "__main__":
  test_step_spark_temp_view()
  