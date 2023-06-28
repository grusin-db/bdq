import bdq
from bdq import spark
import pyspark.sql.functions as F

df = spark.range(0, 100) \
  .withColumn('type', F.expr('(id / 10)::int + 1')) \
  .withColumn('reminder', F.expr('id % 10')) \
  .withColumn('static', F.lit('A')) \
  .withColumn('round_robin', F.expr('id % 2'))

display(df)

assert bdq.validate_primary_key_candidate(df, key_columns=['id'])['failed_records'] == 0
assert bdq.validate_primary_key_candidate(df, key_columns=['type'])['failed_records'] == 100