import bdq
from pyspark.sql.types import *

schema = "a: byte, b: decimal(  16 , 8   )"

assert bdq.get_schema_from_ddl_string(schema) == StructType([StructField('a', ByteType(), True), StructField('b', DecimalType(16,8), True)])

assert bdq.get_column_names_not_in_schema(["x", "a", "c", "b"], schema) == ['x', 'c']

assert bdq.get_column_names_in_schema(["x", "a", "c", "b"], schema) == ['a', 'b']

assert bdq.get_column_names_from_schema(schema) == ['a', 'b']

str(bdq.get_schema_from_ddl_string(schema)) == "StructType([StructField('a', ByteType(), True), StructField('b', DecimalType(16,8), True)])"

big_schema = "a: byte, b: decimal(  16 , 8   ), c:int, d:int, e:int, f:int, g:int"

assert list(bdq.get_column_names_combinations(["a", "b", "c"], ["f", "g"], schema=big_schema)) == [('f', 'g'),
 ('f', 'g', 'a'),
 ('f', 'g', 'b'),
 ('f', 'g', 'c'),
 ('f', 'g', 'a', 'b'),
 ('f', 'g', 'a', 'c'),
 ('f', 'g', 'b', 'c'),
 ('f', 'g', 'a', 'b', 'c')]