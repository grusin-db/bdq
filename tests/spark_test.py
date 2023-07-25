from pyspark.sql import SparkSession
import pytest
from bdq import spark

def test_spark():
  spark.sql('USE default')
  data = spark.sql('SELECT * FROM diamonds')
  assert data.collect()[0][2] == 'Ideal'

if __name__ == "__main__":
  test_spark()
  