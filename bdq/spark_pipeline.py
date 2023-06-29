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
    return self.pipeline._dag.nodes[self]
  
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
  def __name__(self) -> str:
    return f"(]{self.name})"
  
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
    ret = self.function(self.pipeline) or []
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
    self.spark = spark
    self.name = name
    self._dag = bdq.DAG()

  def step(self, *, returns:list[str]=None, depends_on:list[callable]=None) -> Step:
    def _wrapped(func):
      # TODO: log creation of steps
      s = Step(func, returns=returns, pipeline=self)
      return self._dag.node(depends_on=depends_on)(s)

    return _wrapped
  
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
      