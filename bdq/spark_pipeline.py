from typing import Any, Callable
import bdq
import functools
import inspect
from pyspark.sql import DataFrame, SparkSession
from bdq import spark, table
from copy import deepcopy

__all__ = [
  'SparkPipeline'
  ,'Step'
]
  
class SparkPipeline:
  ...

class Step():
  @property
  def _dag(self):
    return self.pipeline._dag

  @property
  def _node(self) -> bdq.dag.Node:
    return self._dag.functions[self]
  
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
  
  def __init__(self, func, pipeline:SparkPipeline, depends_on:list[Callable], returns:list[str]=None):
    if func is None or not callable(func):
      raise ValueError("func must be a callable")
    
    self.name = func.__name__
    self.function = func
    self.returns = validate_step_returns(func, returns)
    self.pipeline = pipeline        
    depends_on = validate_step_depends_on(depends_on)

    deps = [n._node for n in depends_on]
    self._dag.node(depends_on=deps)(self)

  def __repr__(self):
    return f"{self.name}"
  
  def __call__(self):
    return execute_decorated_function(self.function, self, self.returns, item_type=Any)
    
    #FIXME: how to make it working?!
    # if self.pipeline._spark_thread_pinning_wrapper:
    #   #add stage logger, and wrap it in spark thread pinner code
    #   f = self.pipeline._spark_thread_pinning_wrapper(f)
    #   f = bdq.SparkUILogger.tag(f, desc=f"{self.pipeline.name}.{self.name}", verbose=True)
    #   print("spark logger ui wrapped", f)

    
class SparkPipeline:
  def __init__(self, spark:SparkSession, name:str):
    self.name = name
    self._spark = spark or SparkSession.getActiveSession()

    if not self._spark:
      raise ValueError("could not get active spark session")
  
    self._spark_thread_pinning_wrapper = self._get_spark_thread_pinning_wrapper(self._spark)
    self._dag = bdq.DAG()
    
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

def register_spark_pipeline_step_implementation(func):
  name:str = func.__name__

  if not name.startswith("step_"):
    raise ValueError(f"invalid name: {name}; name must start with 'step_'")

  if getattr(SparkPipeline, name, None):
    raise ValueError(f"{name} is already registered!")

  setattr(SparkPipeline, name, func)
  return func

def validate_list_of_type(obj, obj_name, item_type, default_value=None):
  if obj is None:
    obj = default_value
  
  if obj is None:
    raise ValueError(f"{obj_name} is not defined")

  if isinstance(obj, tuple):
    obj = list(obj)

  if item_type != Any:
    if isinstance(obj, item_type):
      obj = [ obj ]
  else:
    if not isinstance(obj, list):
      obj = [ obj ]

  if not isinstance(obj, list):
    raise ValueError(f"{obj_name} must be a list of {item_type}")

  if item_type != Any:
    for r in obj:
      if not isinstance(r, item_type):
        raise ValueError(f"{obj_name} must be a list of {item_type}")
    
  return obj

def validate_step_returns(func:Callable, returns:list[str]):
  return validate_list_of_type(
    obj=returns,
    obj_name="returns",
    item_type=str,
    default_value=func.__name__
  )

def validate_step_depends_on(depends_on):
  return validate_list_of_type(
    obj=depends_on,
    obj_name="depends_on",
    item_type=Callable,
    default_value=[]
  )

def execute_decorated_function(func:Callable, pipeline:SparkPipeline, returns:list[str], item_type):
  returns = validate_step_returns(func, returns)  
  
  #TODO: do some inspect and parameter probing, to detect if pipeline has to be passed or not
  data = func(pipeline)
  data = validate_list_of_type(
    obj=data, 
    obj_name=f"return value of function {func.__name__}", 
    item_type=item_type
  )

  if len(data) != len(returns):
    raise ValueError(f"Step {func.__name__}(...) returned {len(data)} {item_type}(s), but {len(returns)} were expected, to match returns specification: {returns}")

  return data

@register_spark_pipeline_step_implementation
def step_python(self, *, returns:list[str]=None, depends_on:list[Step]=None) -> Step:    
    def _step_wrapper(func):
      return Step(func, returns=returns, pipeline=self, depends_on=depends_on)
    return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark(self, *, returns:list[str]=None, depends_on:list[Step]=None) -> Step:
  def _step_wrapper(func):
    
    @functools.wraps(func)
    def _logic_wrapper(p):
      return execute_decorated_function(func, self, returns, DataFrame)

    return Step(_logic_wrapper, returns=returns, pipeline=self, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_temp_view(self, *, returns:list[str]=None, depends_on:list[Step]=None) -> Step:
  def _step_wrapper(func):
    
    @functools.wraps(func)
    def _logic_wrapper(p):
      new_returns = validate_step_returns(func, returns)
      data = execute_decorated_function(func, self, new_returns, DataFrame)

      new_data = []
      for df, name in zip(data, new_returns):          
        df.createOrReplaceTempView(name)
        new_data.append(table(name)) 

      return new_data

    return Step(_logic_wrapper, returns=returns, pipeline=self, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_table(self, *, returns:list[str]=None, depends_on:list[Step]=None, 
                      mode:str="overwrite", format:str="delta", **options:dict[str, Any]) -> Step:

  def _step_wrapper(func):
    @functools.wraps(func)
    def _logic_wrapper(p):
      new_returns = validate_step_returns(func, returns)
      data = execute_decorated_function(func, self, new_returns, DataFrame)

      new_data = []
      for df, name in zip(data, new_returns):
        df.write.format(format).mode(mode).options(**options).saveAsTable(name)
        new_data = table(name)

      return new_data
    
    return Step(_logic_wrapper, returns=returns, pipeline=self, depends_on=depends_on)
  return _step_wrapper