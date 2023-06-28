import bdq
from pyspark.sql import DataFrame, SparkSession

__all__ = [
  'Pipeline'
  ,'Step'
]

class Pipeline:
  ...

class Step:
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
    self._dag = bdq.DAG()

  def step(self, *, returns:list[str]=None, depends_on:list[callable]=None) -> Step:
    def _wrapped(func):
      # TODO: log creation of steps
      s = Step(self, func, returns=returns)
      return self._dag.node(depends_on=depends_on)(s)

    return _wrapped

  def __call__(self, max_concurrent_steps=10):
    # TODO: handle raising errors here
    return self._dag.execute(max_workers=max_concurrent_steps, verbose=True)