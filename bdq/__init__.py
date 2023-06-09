__version__ = "0.0.1"

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

from .schema import schema_compare