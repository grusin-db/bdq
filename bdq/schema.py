__all__ = [ 
  'compare_schemas',
  'dict_compare',
  'get_column_names_from_schema',
  'get_schema_from_ddl_string',
  'get_column_names_not_in_schema',
  'get_column_names_in_schema',
  'get_column_names_combinations'
]

import sys
from itertools import combinations
import pyspark.sql.types as T
from pyspark.sql import DataFrame

def compare_schemas(s1: T.StructType, s2: T.StructType) -> dict:
  def _get_type_dict(s:T.StringType):
    return { 
      f.name: f.dataType.simpleString()
      for f in s.fields
    }
  
  d1 = _get_type_dict(s1)
  d2 = _get_type_dict(s2)

  return dict_compare(d1, d2)

def dict_compare(d1:dict, d2:dict) -> dict:
  d1_keys = set(d1.keys())
  d2_keys = set(d2.keys())
  shared_keys = d1_keys.intersection(d2_keys)
  
  added = d1_keys - d2_keys
  removed = d2_keys - d1_keys
  
  modified = {
    o: {'before': d1[o], 'after': d2[o]} 
    for o in shared_keys
    if d1[o] != d2[o]
  }

  not_changed = set(o for o in shared_keys if d1[o] == d2[o])
  return { 
    'added': added, 
    'removed': removed, 
    'changed': modified, 
    'not_changed': not_changed
  }

def get_schema_from_ddl_string(s):
  return T._parse_datatype_string(s)

def get_column_names_from_schema(schema):
  if not schema:
    raise ValueError("empty schema")
  
  if isinstance(schema, str):
    schema = get_schema_from_ddl_string(schema)
  elif isinstance(schema, DataFrame):
    schema = schema.schema

  if isinstance(schema, T.StructType):
    return schema.names
    
  raise ValueError(f"invalid schema: {schema}")

def get_column_names_in_schema(columns, schema):
  schema_columns = set(get_column_names_from_schema(schema))

  return [
    c
    for c in columns
    if c in schema_columns
  ]

def get_column_names_not_in_schema(columns, schema):
  schema_columns = set(get_column_names_from_schema(schema))

  return [
    c
    for c in columns
    if c not in schema_columns
  ]
    
def get_column_names_combinations(dynamic_column_names, fixed_column_names=None, max_len=None, schema=None):
  fixed_column_names = tuple(fixed_column_names or [])
  max_len = (max_len or sys.maxsize) - len(fixed_column_names)
 
  if schema:
    not_in_schema = get_column_names_not_in_schema(fixed_column_names, schema) + get_column_names_not_in_schema(dynamic_column_names, schema)
    if not_in_schema:
      raise ValueError(f"invalud column names: {not_in_schema}")

  if fixed_column_names:
    yield fixed_column_names

  if set(fixed_column_names).intersection(dynamic_column_names):
    raise ValueError("fixed column names may not contain dynamic column names")

  def _unique(a, b):
    seen = set()
    return [
      x 
      for x in a + b
      if not (x in seen or seen.add(x))
    ]

  for n in range(1, len(dynamic_column_names) + 1):
    if n >= max_len + 1:
      break

    for c in combinations(dynamic_column_names, n):
      yield tuple(_unique(fixed_column_names, c))