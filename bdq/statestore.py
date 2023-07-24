from typing import Union
import logging
import pyspark.sql.types as T
import pyspark.sql.functions as F
import json
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from copy import deepcopy

__all__ = [
  'CatalogPersistedStateStore'
]

class PersistedStateStoreBase:
  def __init__(self):
    pass
  
  def save(self, data:dict):
    pass

  def load(self) -> dict:
    pass
  
  @classmethod
  def _to_json(cls, obj):
    def _default(x):
      if isinstance(x, datetime):
        return { '_isoformat': x.isoformat() }
      raise TypeError(f'Unsupported type: {type(x)}: {x}')

    return json.dumps(obj, default=_default)

  @classmethod
  def _from_json(cls, obj):
    def _object_hook(x):
      _isoformat = x.get('_isoformat')
      if _isoformat is not None:
          return datetime.fromisoformat(_isoformat)
      return x
    
    return json.loads(obj, object_hook=_object_hook)

class CatalogPersistedStateStore(PersistedStateStoreBase):
  def __init__(self, catalog_name: str, database_name: str, table_name: str, schema: Union[str, T.StructType], event_ts_column:str, json_encoded_columns=None, log: logging.Logger=None):
    self.log = log.getChild('StateStore') if log else logging.getLogger('StateStore')
    self.catalog_name = catalog_name
    self.database_name = database_name
    self.table_name = table_name
    self.schema = schema
    self.fqn_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"
    self.event_ts_column = event_ts_column
    self.json_encoded_columns = json_encoded_columns
    self.spark = SparkSession.getActiveSession()

    if isinstance(schema, str):
      schema = T._parse_datatype_string(schema)

    for c in json_encoded_columns:
      if c not in [f.name for f in schema.fields]:
        raise ValueError(f"Column {c} does not exist in schema")

    # create table
    if not self.spark.catalog.tableExists(self.fqn_table_name):
      self.log.info(f"Creating state table: {self.fqn_table_name}")
      self.spark.createDataFrame([], self.schema) \
        .write \
        .saveAsTable(self.fqn_table_name)

    df = self.spark.table(self.fqn_table_name)

    # update schema
    if df.schema != schema:
      self.log.info(f"Updating schema of {self.fqn_table_name}")
      self.spark.createDataFrame([], self.schema) \
        .write \
        .option('mergeSchema', True) \
        .saveAsTable(self.fqn_table_name)

    # all done
    self.log.info(f"Initialized CatalogPersistedState from {self.fqn_table_name}")

  def load(self) -> dict:
    data = self.spark.table(self.fqn_table_name).orderBy(F.col(self.event_ts_column).desc()).limit(1).first()
    if not data:
      self.log.info("State not found, returning empty placeholder")
      return {}
    
    data = data.asDict()
    for c in self.json_encoded_columns:
      data[c] = self._from_json(data[c])

    return data
  
  def save(self, data: dict):
    if not isinstance(data, dict):
      raise ValueError("data must be a dict")
    
    data = deepcopy(data)

    for c in self.json_encoded_columns:
      data[c] = self._to_json(data[c])
    
    df = self.spark.createDataFrame([data], self.schema)
    df.write.mode('append').saveAsTable(self.fqn_table_name)
    self.log.info("State saved")

  @classmethod
  def clean(cls, name: str):
    return ''.join([
      n if n.isalnum() else '_'
      for n in name
    ])