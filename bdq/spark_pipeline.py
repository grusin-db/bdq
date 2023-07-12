from typing import Any
import bdq
from pyspark.sql import DataFrame, SparkSession

__all__ = [
  'SparkPipeline'
  ,'Step'
]

class SparkPipeline:
  ...

class Step():
  @property
  def _node(self) -> bdq.dag.Node:
    return self.pipeline._dag.functions[self]
  
  @property
  def state(self):
    return self._node.state
  
  @property
  def result(self):
    return self._node.result
  
  @property
  def exception(self):
    return self._node.exception
  
  @property
  def traceback(self):
    return self._node.traceback
  
  @property
  def __name__(self) -> str:
    return f"{self.name}"
  
  def __init__(self, function, returns:list[str]=None, pipeline:SparkPipeline=None):
    if function is None or not callable(function):
      raise ValueError("function must be a callable, not may not be None")
    
    self.name = function.__name__
    self.function = function
    self.returns = returns or [function.__name__]
    self.pipeline = pipeline

    # TODO: handle logging
    if isinstance(self.returns, list):
      for r in self.returns:
        if not isinstance(r, str):
          raise ValueError("returns must be a list of names(str)")

  def __repr__(self):
    return f"{self.name}"

  def __call__(self):
    f = self.function

    if self.pipeline._spark_thread_pinning_wrapper:
      #add stage logger, and wrap it in spark thread pinner code
      f = bdq.SparkUILogger.tag(f, desc=f"{self.pipeline.name}.{self.name}")
      f = self.pipeline._spark_thread_pinning_wrapper(f)

    ret = f(self.pipeline) or []
    if isinstance(ret, DataFrame):
      ret = [ ret ]

    if isinstance(ret, str) or not isinstance(ret, list):
      raise ValueError(f"Step {self.name}(...) must return list of dataframe(s) matching returns specification")

    if len(ret) != len(self.returns):
      raise ValueError(f"Step {self.name}(...) returned {len(ret)} dataframe(s), but {len(self.returns)} were expected")

    for idx, r in enumerate(ret):
      if not isinstance(r, DataFrame):
        raise ValueError(f"Step {self.name}(...) did not returnd a dataframe at index={idx} of name={self.returns[idx]}")

    for df, name in zip(ret, self.returns):
      df.createOrReplaceTempView(name)

    return ret
    
class SparkPipeline:
  def __init__(self, spark:SparkSession, name:str):
    self.name = name
    self._spark = spark or SparkSession.getActiveSession()
    
    if not self._spark:
      raise ValueError("could not get active spark session")
  
    self._spark_thread_pinning_wrapper = self._get_spark_thread_pinning_wrapper(self._spark)
    self._dag = bdq.DAG()

  def step(self, *, returns:list[str]=None, depends_on:list[Step]=None, verbose=False) -> Step:
    depends_on = depends_on or []
    
    def _wrapped(func):
      # TODO: log creation of steps
      s = Step(func, returns=returns, pipeline=self)
      deps = [n._node for n in depends_on]
      self._dag.node(depends_on=deps)(s)

      return s

    return _wrapped
  
  def visualize(self):
    return self._dag.visualize()
  
  def is_success(self):
    return self._dag.is_success()
  
  @classmethod
  def _unpack_state_from_node_list(cls, node_list: list[bdq.dag.Node]):
    return [n.function for n in node_list]

  def get_error_steps(self):
    return self._unpack_state_from_node_list(self._dag.get_error_nodes())

  def get_skipped_steps(self):
    return self._unpack_state_from_node_list(self._dag.get_skipped_nodes())

  def get_success_steps(self):
    return self._unpack_state_from_node_list(self._dag.get_success_nodes())

  def __call__(self, max_concurrent_steps=10):
    self._dag.execute(max_workers=max_concurrent_steps, verbose=True)
    if self.is_success():
      return self._unpack_state_from_node_list(self._dag.nodes)
    
    error_steps = self.get_error_steps()

    raise ValueError(f"{len(error_steps)} step(s) have failed: {error_steps}")
  
  @classmethod
  def _get_spark_thread_pinning_wrapper(cls, spark: SparkSession=None):
    try:
      if cls._is_spark_thread_pinning_supported(spark):
        from pyspark import inheritable_thread_target
        return inheritable_thread_target
    except:
      return None
        
  @classmethod
  def _is_spark_thread_pinning_supported(cls, spark: SparkSession=None) -> bool:
    try:
      from py4j.clientserver import ClientServer
      spark = spark or SparkSession.getActiveSession()
      
      return isinstance(spark.sparkContext._gateway, ClientServer)
    except:
      return False
      