import bdq
from pyspark.sql import DataFrame, SparkSession

__all__ = [
  'Pipeline'
  ,'Step'
]

class Pipeline:
  ...

class Step:
  @property
  def node(self) -> bdq.dag.Node:
    return self.pipeline.dag.nodes[self]
  
  def __init__(self, pipeline:Pipeline, function:callable, returns:list[str]=None):
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
    return f"{self.name}() -> {self.returns}"

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
    
class Pipeline:
  def __init__(self, spark:SparkSession, name:str):
    self.spark = spark
    self.name = name
    self.dag = bdq.DAG()

  def step(self, *, returns:list[str]=None, depends_on:list[callable]=None) -> Step:
    def _wrapped(func):
      # TODO: log creation of steps
      s = Step(self, func, returns=returns)
      return self.dag.node(depends_on=depends_on)(s)

    return _wrapped
  
  def is_success(self):
    return self.dag.is_success()
  
  def get_error_nodes(self):
    return self.dag.get_error_nodes()

  def get_skipped_nodes(self):
    return self.dag.get_skipped_nodes()

  def get_success_nodes(self):
    return self.dag.get_success_nodes()

  def __call__(self, max_concurrent_steps=10):
    self.dag.execute(max_workers=max_concurrent_steps, verbose=True)
    if self.is_success():
      return self.dag.nodes.values
    
    error_steps = self.dag.get_error_nodes()

    raise ValueError(f"{len(error_steps)} step(s) have failed: {error_steps}")
      