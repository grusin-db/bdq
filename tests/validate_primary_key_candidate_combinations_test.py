import bdq
from bdq import spark
import pyspark.sql.functions as F

df = spark.range(0, 100) \
  .withColumn('type', F.expr('(id / 10)::int + 1')) \
  .withColumn('reminder', F.expr('id % 10')) \
  .withColumn('static', F.lit('A')) \
  .withColumn('round_robin', F.expr('id % 2'))

display(df)

all_combinations = list(bdq.get_column_names_combinations(df.columns))

assert bdq.validate_primary_key_candidate_combinations(df, all_combinations, max_workers=10, verbose=True) == [('id',), ('type', 'reminder')]