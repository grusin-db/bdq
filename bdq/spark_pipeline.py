from typing import Any, Callable, Union
import bdq
import functools
import inspect
import threading
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryListener, DataStreamWriter, StreamingQuery
from pyspark.sql.streaming.listener import QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent
from bdq import spark, table
from copy import deepcopy

__all__ = [
  'SparkPipeline'
  ,'Step'
  ,'register_spark_pipeline_step_implementation'
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

  def __repr__(self) -> str:
    return f"{self.name}"

  def _repr_str_(self):
    max_len = max(
      len(str(n))
      for n in self.__dict__
      if not str(n).startswith('_')
    ) + 1

    return f"\n{self.name}:\n" + "\n".join([ 
      f"  {n:{max_len}}: {str(v)[0:100]}"
      for n, v in self.__dict__.items()
      if not str(n).startswith('_')
      ])
  
  def __call__(self):
    return execute_step_decorated_function(self.function, self, self.returns, item_type=Any)
    
    #FIXME: how to make it working?!
    # if self.pipeline._spark_thread_pinning_wrapper:
    #   #add stage logger, and wrap it in spark thread pinner code
    #   f = self.pipeline._spark_thread_pinning_wrapper(f)
    #   f = bdq.SparkUILogger.tag(f, desc=f"{self.pipeline.name}.{self.name}", verbose=True)
    #   print("spark logger ui wrapped", f)

    
class SparkPipeline:
  @property
  def spark_streaming_checkpoint_location(self):
    return self.conf.get(
      'spark.sql.streaming.checkpointLocation', 
      self._spark.conf.get('spark.sql.streaming.checkpointLocation')
    )
  
  @spark_streaming_checkpoint_location.setter
  def spark_streaming_checkpoint_location(self, value):
    self.conf['spark.sql.streaming.checkpointLocation'] = value

  def __init__(self, name:str, spark:SparkSession=None):
    self.name = name
    self.conf:dict[str,str] = {}
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
  #print(f"validate_list_of_type: {obj=}, {obj_name=}, {item_type=}, {default_value=}")
  if obj is None:
    obj = default_value
  
  if obj is None:
    raise ValueError(f"{obj_name} is not defined (debug; {default_value=})")

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

def execute_step_decorated_function(func:Callable, step:Step, returns:list[str], item_type):
  returns = validate_step_returns(func, returns)  
  
  #TODO: do some inspect and parameter probing, to detect if pipeline has to be passed or not
  data = func(step)
  data = validate_list_of_type(
    obj=data, 
    obj_name=f"return value of function {func.__name__}", 
    item_type=item_type,
    default_value=[]
  )

  if len(data) != len(returns):
    raise ValueError(f"Step {func.__name__}(...) returned {len(data)} {item_type}(s), but {len(returns)} were expected, to match returns specification: {returns}")

  return data

def validate_xor_values(**kwargs):
  set_values = [k for k, v in kwargs.items() if v]
  all_name_str = ", ".join(kwargs)
  set_name_str = ", ".join(set_values) or 'None'

  if len(set_values) != 1:
    raise ValueError(f"Exactly one of {all_name_str} has to be defined, got: {set_name_str}")
  
  name = set_values[0]
  return name, kwargs[name]

def validate_spark_streaming_checkpoint_location(pipeline:SparkPipeline, name:Union[Callable, str]):
  if not pipeline.spark_streaming_checkpoint_location:
    raise ValueError("SparkPipeline's spark_streaming_checkpoint_location is not defined")
  
  if isinstance(name, Callable):
    name = name.__name__

  return f"{pipeline.spark_streaming_checkpoint_location}/{pipeline.name}/{name}"

def apply_spark_streaming_trigger(dw:DataStreamWriter, trigger_once:bool=False, trigger_availableNow:bool=False, trigger_interval:str=None):
  name, value = validate_xor_values(trigger_once=trigger_once, trigger_availableNow=trigger_availableNow, trigger_interval=trigger_interval)

  print("TRIGGER:", name, value)

  if name == 'trigger_once':
    dw = dw.trigger(once=value)
  elif name == 'trigger_availableNow':
    dw = dw.trigger(availableNow=value)
  elif name == 'trigger_interval':
    dw = dw.trigger(processingTime=value)

  return dw

@register_spark_pipeline_step_implementation
def step_python(pipeline:SparkPipeline, *, returns:list[str]=None, depends_on:list[Step]=None) -> Step:    
    def _step_wrapper(func):
      return Step(func, returns=returns, pipeline=pipeline, depends_on=depends_on)
    return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark(pipeline:SparkPipeline, *, returns:list[str]=None, depends_on:list[Step]=None) -> Step:
  def _step_wrapper(func):
    
    @functools.wraps(func)
    def _logic_wrapper(step):
      return execute_step_decorated_function(func, step, returns, DataFrame)

    return Step(_logic_wrapper, returns=returns, pipeline=pipeline, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_temp_view(pipeline:SparkPipeline, *, returns:list[str]=None, depends_on:list[Step]=None) -> Step:
  def _step_wrapper(func):
    
    @functools.wraps(func)
    def _logic_wrapper(step):
      new_returns = validate_step_returns(func, returns)
      data = execute_step_decorated_function(func, step, new_returns, DataFrame)

      new_data = []
      for df, name in zip(data, new_returns):          
        df.createOrReplaceTempView(name)
        new_data.append(table(name)) 

      return new_data

    return Step(_logic_wrapper, returns=returns, pipeline=pipeline, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_table(pipeline:SparkPipeline, *, returns:list[str]=None, depends_on:list[Step]=None, 
                      mode:str="overwrite", format:str="delta", **options:dict[str, Any]) -> Step:

  def _step_wrapper(func):
    @functools.wraps(func)
    def _logic_wrapper(step):
      new_returns = validate_step_returns(func, returns)
      data = execute_step_decorated_function(func, step, new_returns, DataFrame)

      new_data = []
      for df, name in zip(data, new_returns):
        df.write.format(format).mode(mode).options(**options).saveAsTable(name)
        new_data = table(name)

      return new_data
    
    return Step(_logic_wrapper, returns=returns, pipeline=pipeline, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_for_each_batch(pipeline:SparkPipeline, *, input_table:str, returns:list[str]=None, depends_on:list[Step]=None, 
                              trigger_once:bool=False, trigger_availableNow:bool=False, trigger_interval:str=None, 
                              options:dict=None
                              ):
  
  options = options or {}
  #TODO: resolve input table from depends_on if possible
  #TODO: handle reset?

  validate_xor_values(trigger_once=trigger_once, trigger_availableNow=trigger_availableNow, trigger_interval=trigger_interval)

  def _step_wrapper(func):
    @functools.wraps(func)
    def _logic_wrapper(step:Step):
      nonlocal returns, depends_on

      returns = validate_step_returns(func, returns)
      depends_on = validate_step_depends_on(depends_on)

      streaming_df:DataFrame = table(input_table)
      
      class MyListener(StreamingQueryListener):
        def onQueryStarted(self, event:QueryStartedEvent):
          if event.name == step.streaming_query_name:
            self._filter_runId = event.runId
            
        def onQueryProgress(self, event:QueryProgressEvent):
          pass         
             
        def onQueryTerminated(self, event:QueryTerminatedEvent):
          if event.runId != self._filter_runId:
            return
            
          # handle scenario where query finishes, user code can set this too!
          step.streaming_unblock_event.set()
      
      spark.streams.addListener(MyListener())

      relative_batch_id = 0

      def _feb_func_wrapper(df, batch_id):
        nonlocal relative_batch_id
        r = func(df, batch_id, relative_batch_id, step)
        relative_batch_id = relative_batch_id + 1
        return r
      
      step.streaming_unblock_event = threading.Event()

      dw = streaming_df.writeStream \
        .option('checkpointLocation', step.streaming_checkpoint_location) \
        .options(**options) \
        .queryName(step.streaming_query_name) \
        .foreachBatch(_feb_func_wrapper) \
        
      dw = apply_spark_streaming_trigger(dw, trigger_once=trigger_once, trigger_interval=trigger_interval, trigger_availableNow=trigger_availableNow)
      sq = dw.start()

      step.streaming_query = sq

      # wait for event; desired amount of batches
      step.streaming_unblock_event.wait()
      
      # raise if stream terminated because of error
      ex = sq.exception()
      if ex:
        raise ex

      return [table(n) for n in returns]

    # i should make new class instead of monkey patching this one, but who is to stop me?
    step = Step(_logic_wrapper, returns=returns, pipeline=pipeline, depends_on=depends_on)
    step.streaming_query_name:str = f"{step.pipeline.name}/{step.name}"
    step.streaming_checkpoint_location:str = validate_spark_streaming_checkpoint_location(pipeline, func)
    step.streaming_query:StreamingQuery = None
    step.streaming_unblock_event = None

    return step
  
  return _step_wrapper