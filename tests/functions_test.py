from datetime import datetime
from bdq.functions import surrogate_key_hash, surrogate_key_string
from bdq import spark

schema = "id1:long,id2:long,name:string,likes:int"

df1 = spark.createDataFrame([
  [ 1, 1, "GrzeGorz", 1 ],
  [ 1, 1, "Grzegorz",  1 ],
  [ 1, 1, "Grzegorz      ",  1 ],
  [ 1, None, "Grzegorz", 1],
  [ 1, None, None, 1],
  [ 2, 1, "Tim", 1 ],
  [ 3, 1, "Mike", 1 ]
], schema)

sk_df = df1.select(
  '*', 
  surrogate_key_hash(['id1', 'id2', 'name'], rtrim=True).alias('hash'),
  surrogate_key_string(['id1', 'id2', 'name'], rtrim=True).alias('hash_input')
)

display(sk_df)