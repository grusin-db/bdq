from typing import Any
import bdq
import functools
import inspect
from pyspark.sql import DataFrame, SparkSession

__all__ = [
  'SparkPipeline'
  ,'Step'
  ,'register_custom_step'
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
  
  def __init__(self, function, returns:list[str]=None, pipeline:SparkPipeline=None, post_callback=None, post_callback_kwargs=None):
    if function is None or not callable(function):
      raise ValueError("function must be a callable, not may not be None")
    
    self.name = function.__name__
    self.function = function
    if isinstance(returns, str):
      returns = [ returns ]
    self.returns = returns or [function.__name__]
    self.pipeline = pipeline
    self.post_callback = post_callback
    self.post_callback_kwargs = post_callback_kwargs or {}

    # TODO: handle logging
    if isinstance(self.returns, list):
      for r in self.returns:
        if not isinstance(r, str):
          raise ValueError("returns must be a list of names(str)")

  def __repr__(self):
    return f"{self.name}"
  
  def __call__(self):
    def _execute_step(f):
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

      if self.post_callback:
        return self.post_callback(step=self, data=ret, **self.post_callback_kwargs)
     
      return ret

    f = self.function

    if self.pipeline._spark_thread_pinning_wrapper:
      #add stage logger, and wrap it in spark thread pinner code
      f = self.pipeline._spark_thread_pinning_wrapper(f)
      f = bdq.SparkUILogger.tag(f, desc=f"{self.pipeline.name}.{self.name}", verbose=True)
      print("spark logger ui wrapped", f)

    return _execute_step(f)
    
class SparkPipeline:
  def __init__(self, spark:SparkSession, name:str):
    self.name = name
    self._spark = spark or SparkSession.getActiveSession()
    
    if not self._spark:
      raise ValueError("could not get active spark session")
  
    self._spark_thread_pinning_wrapper = self._get_spark_thread_pinning_wrapper(self._spark)
    self._dag = bdq.DAG()

  def _default_post_step_callback(self, *, step:Step, ret:list[DataFrame]):
    for df, name in zip(ret, step.returns):
      df.createOrReplaceTempView(name)

    return step.result

  def step(self, *, returns:list[str]=None, depends_on:list[Step]=None, verbose=False, post_callback=None, post_callback_kwargs=None) -> Step:
    depends_on = depends_on or []
    post_callback = post_callback or self._default_post_step_callback
    
    def _wrapped(func):
      # TODO: log creation of steps
      s = Step(func, returns=returns, pipeline=self, post_callback=post_callback, post_callback_kwargs=post_callback_kwargs)
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

  def execute(self, max_concurrent_steps=10):
    self._dag.execute(max_workers=max_concurrent_steps, verbose=True)
    if self.is_success():
      return self._unpack_state_from_node_list(self._dag.nodes)
    
    error_steps = self.get_error_steps()

    raise ValueError(f"{len(error_steps)} step(s) have failed: {error_steps}")
  
  def __call__(self, max_concurrent_steps=10):
    return self.execute(max_concurrent_steps=max_concurrent_steps)
  
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
      

def register_custom_step(*args, **kwargs):
  def _outer(post_callback):
    @functools.wraps(post_callback)
    def _inner(self, returns:list[str]=None, depends_on:list[Step]=None, verbose=False, **passed_kwargs):
      # verify signature of the new handler, there must be better way of doing it, but here we are
      sig = inspect.signature(_inner)
      
      if "step" not in sig.parameters or "data" not in sig.parameters:
        raise ValueError(f"decorated function signature, must have parameters: `step` and `data`; instead got: {', '.join(list(sig.parameters))}")

      expected_kwargs = set(sig.parameters).difference(['step', 'data'])
      invalid_args = set(passed_kwargs).difference(set(expected_kwargs))
      if invalid_args:
        raise ValueError(f"expected: {', '.join(list(expected_kwargs))}; instead got: {', '.join(list(passed_kwargs))}")
      
      return self.step(returns=returns, depends_on=depends_on, verbose=verbose, post_callback=post_callback, post_callback_kwargs=passed_kwargs)
    
    if getattr(bdq.SparkPipeline, post_callback.__name__, None):
      raise ValueError(f"{post_callback.__name__} is already registered!")

    setattr(bdq.SparkPipeline, post_callback.__name__, _inner)

    return _inner
  
  # called without ()
  if len(args) == 1 and callable(args[0]) and len(kwargs) == 0:
    return _outer(args[0])
  
  # called with (...)
  return _outer