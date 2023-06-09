import pyspark.sql.types as T

def schema_compare(s1: T.StructType, s2: T.StructType) -> dict:
  d1 = dict(s1.dtypes)
  d2 = dict(s1.dtypes)

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
    'modified': modified, 
    'not_changed': not_changed
  }