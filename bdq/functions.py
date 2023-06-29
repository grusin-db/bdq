from datetime import datetime
import pyspark.sql.functions as F

__all__ = [ 
  'surrogate_key_string',
  'surrogate_key_hash'
]

def surrogate_key_string(key_columns: list[str], rtrim=False, ltrim=False, upper=True, null_str_repr='@~<null>~@'):
  cols = []

  for n in key_columns:
    col_expr = F.col(n)
    if upper:
      col_expr = F.upper(col_expr)
    if rtrim:
      col_expr = F.rtrim(col_expr)
    if ltrim:
      col_expr = F.ltrim(col_expr)

    cols.append(F.coalesce(col_expr, F.lit(null_str_repr)).cast('string'))

  if len(cols) == 1:
    return cols[0]
  
  return F.array(cols).cast('string')

def surrogate_key_hash(key_columns, rtrim=False, ltrim=False):
  key_bits = 160
  hex_str_len = 2*key_bits//8 #hex is 2 character per byte
  return F.unhex(F.sha2(surrogate_key_string(key_columns, rtrim=rtrim, ltrim=ltrim), 224).substr(1, hex_str_len))