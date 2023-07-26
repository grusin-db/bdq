from typing import Any, Callable, Union, List, Dict
import bdq
import functools
import inspect
import logging
import threading
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from bdq import spark, table, CatalogPersistedStateStore, SparkUILogger
from copy import deepcopy
from datetime import datetime

# old spark workaround
try:
  from pyspark.sql import Observation
except:
  pass

# old spark workaround
try:
  from pyspark.sql.streaming import StreamingQueryListener
  from pyspark.sql.streaming.listener import QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent
except:
  pass

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
  def result_state(self):
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
  
  @property
  def stop_ts(self):
    return self._node.stop_ts
    
  @property
  def start_ts(self):
    return self._node.start_ts
  
  @property
  def _lazy_function_spark_metrics(self):
    return self.pipeline._function_lazy_spark_metrics.get(self.name, {})
    
  @property
  def last_run_metrics(self):
    prefix = f"{self.name}."
    prefix_len = len(prefix)
    return {
      n[prefix_len:]: v
      for n, v in self.pipeline.last_run_metrics.items()
      if n.startswith(prefix)
    }
    
  def __init__(self, func, pipeline:SparkPipeline, depends_on:List[Callable], outputs:List[str]=None, spark_metrics_supported=False):
    if func is None or not callable(func):
      raise ValueError("func must be a callable")
    
    self.name = func.__name__
    self.pipeline = pipeline
    self.log:logging.Logger = self.pipeline.log.getChild(self.name)
    self.function = func
    self.outputs = validate_step_outputs(func, outputs)
    self.metrics: Dict[str, Any] = {}
    self._spark_metrics_supported = spark_metrics_supported

    if self._lazy_function_spark_metrics:
      if not self._spark_metrics_supported:
        raise ValueError(f"spark metrics are not supported by Step {self.name}")
      if not self.pipeline._state_store:
        raise ValueError(f"pipeline's state store must be enabled to use spark metrics")

    for r in self.outputs:
      s = self.pipeline._registered_outputs.get(r)
      if s and s.name != self.name:
        raise ValueError(f"{r} is already created by Step {s.name}")
      self.pipeline._registered_outputs[r] = self

    depends_on = self.pipeline._resolve_depends_on(depends_on)

    depends_on_dag_nodes = [n._node for n in depends_on]
    self._dag.node(depends_on=depends_on_dag_nodes)(self)

  def __repr__(self) -> str:
    return f"{self.name}"

  def _repr_html_(self):
    def _trunc_string(s, max_len=150):
      s = str(s)
      return s[:max_len] + (s[max_len:] and '...')

    data = [
      (n, _trunc_string(getattr(self, n)))
      for n in sorted(dir(self))
      if not str(n).startswith('_')
    ]

    pdf = pd.DataFrame.from_records(data, columns =['property', 'value'])
    return pdf.to_html()
  
  def __call__(self):
    f = self.function
    #if self.pipeline._spark_thread_pinning_wrapper:
       #add stage logger, and wrap it in spark thread pinner code
    #  f = self.pipeline._spark_thread_pinning_wrapper(f)
    f = bdq.SparkUILogger.tag(desc=f"{self.pipeline.name}#{self.name}")(f)

    return execute_step_decorated_function(f, self, self.outputs, item_type=Any)
    
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

  @property
  def steps(self) -> Dict[str, Step]:
    return {
      node.function.name: node.function
      for node in self._dag.nodes
    }

  @property
  def error_steps(self):
    return self._get_steps_with_result_state("ERROR")

  @property
  def skipped_steps(self):
    return self._get_steps_with_result_state("SKIPPED")

  @property
  def success_steps(self):
    return self._get_steps_with_result_state("SUCCESS")

  @property
  def is_success(self):
    return self._dag.is_success()
  
  @property
  def metrics(self):
    return {
      f"{step_name}.{metric_name}": metric
      for step_name, step in self.steps.items()
      for metric_name, metric in step.metrics.items()
    }

  @property
  def last_run_metrics(self):
    if self._state_store is None:
      raise ValueError("State store is not enabled")
    
    # FIXME: this should be a frozen dict
    return deepcopy(self._state_store_data.get('metrics', {}))

  def __init__(self, name: str, spark: SparkSession=None, state_store_catalog: str=None, state_store_database: str=None):
    self.name = name
    self.log:logging.Logger = logging.getLogger(self.name)
    self.log.setLevel(logging.INFO)
    self.conf:Dict[str,str] = {}
    self._registered_outputs: dict = {}
    self._spark:SparkSession = spark or SparkSession.getActiveSession()
    self._function_lazy_spark_metrics: Dict[str, dict] = {}
    self.start_ts: datetime = None
    self.stop_ts: datetime = None
    self._state_store: CatalogPersistedStateStore = None
    self._state_store_data: dict = None

    if not self._spark:
      raise ValueError("could not get active spark session")
  
    self._spark_thread_pinning_wrapper = self._get_spark_thread_pinning_wrapper(self._spark)
    self._dag = bdq.DAG(self.name)

    if state_store_catalog and state_store_database:
      with SparkUILogger(f"{self.name}#_load_state_from_store"):
        schema, json_encoded_columns= self._get_save_state_schema()
        self._state_store = CatalogPersistedStateStore(
          catalog_name=state_store_catalog, database_name=state_store_database, table_name=CatalogPersistedStateStore.clean(self.name),
          schema = schema, filter_expr=(F.col('pipeline_name') == F.lit(self.name)),
          json_encoded_columns=json_encoded_columns, event_ts_column='start_ts', log=self.log
        )
        self._state_store_data = self._state_store.load()
    
  # FIXME: how to handle that with display() ?
  def visualize(self):
    return self._dag.visualize()
  
  def spark_progressive_metric(self, *, name: str=None, expr: Union[str, Column]):
    return self.spark_metric(name=name, expr=expr, progressive=True)
                               
  def spark_metric(self, *, name: str=None, expr: Union[str, Column], progressive:bool=False):
    if not expr:
      raise ValueError("expr is not defined")

    name = name or str(expr)
    if isinstance(expr, str):
      expr = F.expr(expr)

    def _wrapper(func):
      func_metrics = self._function_lazy_spark_metrics[func.__name__] = self._function_lazy_spark_metrics.get(func.__name__, {})
      func_metrics[name] = { 'expr': expr, 'progressive': progressive }

      return func
    
    return _wrapper
  
  def _get_steps_with_result_state(self, result_state):
    return { 
      k: v 
      for k, v in self.steps.items() 
      if v.result_state == result_state
    }

  def _execute(self, max_concurrent_steps=10):
    self.start_ts = datetime.now()
    self.stop_ts = None

    self._dag.execute(max_workers=max_concurrent_steps)
    self.stop_ts = datetime.now()
    self._save_state_to_store()

    if self.is_success:
      return self.success_steps

    raise ValueError(f"Step(s) have failed: {self.error_steps}")
  
  def __call__(self, max_concurrent_steps=10):
    return self._execute(max_concurrent_steps=max_concurrent_steps)
  
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
    
  def _resolve_depends_on(self, depends_on: List[Union[Callable, str]]):
    depends_on = validate_list_of_type(
      obj=depends_on,
      obj_name="depends_on",
      item_type=(Callable, str),
      default_value=[]
    )
    
    new_depends_on = set()

    for d in depends_on:
      if isinstance(d, Callable):
        new_depends_on.add(d)
      elif isinstance(d, str):
        step = self._registered_outputs.get(d)
        if not step:
          raise ValueError(f"unresolved depends on: {d}")
        new_depends_on.add(step)

    return list(new_depends_on)
  
  def _get_save_state_schema(self):
    return bdq.get_schema_from_ddl_string("""
      pipeline_name:string,
      start_ts:timestamp,
      stop_ts:timestamp,
      metrics:string
    """), ['metrics']

  def _get_save_state(self):
    m = deepcopy(self.metrics)

    # carry over last run metrics in case current run failed to produce them
    for name, v in self.last_run_metrics.items():
      if '.progressive_spark_metric.' in name and v is not None and self.metrics[name] is None:
        m[name] = v
    
    return {
      'pipeline_name': self.name
      ,'start_ts': self.start_ts
      ,'stop_ts': self.stop_ts
      ,'metrics': m
    }
  
  def _save_state_to_store(self):
    with SparkUILogger(f"{self.name}#_save_state_to_store"):
      if self._state_store:
        self._state_store.save(self._get_save_state())
        self._state_store_data = self._state_store.load()

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

def validate_step_outputs(func:Callable, outputs:List[str]) -> List[str]:
  return validate_list_of_type(
    obj=outputs,
    obj_name="outputs",
    item_type=str,
    default_value=func.__name__
  )

def apply_spark_metrics_observers(df:DataFrame, spark_metrics:Dict[str,dict], log:logging.Logger=None):
  observers = {}
  spark_metrics = spark_metrics or {}

  for metric_name, metric_config in spark_metrics.items():
    metric_expression = metric_config['expr']
    metric_progressive = metric_config['progressive']

    metric_name = f"progressive_spark_metric.{metric_name}" if metric_progressive else f"spark_metric.{metric_name}"

    if df.isStreaming:
      # requires spark 3.4.0, will throw if running on too old version
      df = df.observe(metric_name, metric_expression.alias(metric_name))
      observers[metric_name] = metric_name

      if log:
        log.debug(f"Registered streaming metric: {metric_name}: {metric_expression}")
    else:
      # only works for batching
      obs = Observation(metric_name)
      df = df.observe(obs, metric_expression.alias(metric_name))
      observers[metric_name] = obs

      if log:
        log.debug(f"Registered batch metric: {metric_name}: {metric_expression}")
  
  return (df, observers)

def get_observed_batch_spark_metrics(observers: dict):
  return {  
    name: obs.get[name]
    for name, obs in observers.items()
    if isinstance(obs, Observation)
  }

def execute_step_decorated_function(func:Callable, step:Step, outputs:List[str], item_type):
  outputs = validate_step_outputs(func, outputs)  
  
  #TODO: do some inspect and parameter probing, to detect if pipeline has to be passed or not
  data = func(step)
  data = validate_list_of_type(
    obj=data, 
    obj_name=f"output values of function {func.__name__}", 
    item_type=item_type,
    default_value=[]
  )

  if len(data) != len(outputs):
    raise ValueError(f"Step {func.__name__}(...) returned {len(data)} {item_type}(s), but {len(outputs)} were expected, to match outputs specification: {outputs}")

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

  if name == 'trigger_once':
    dw = dw.trigger(once=value)
  elif name == 'trigger_availableNow':
    dw = dw.trigger(availableNow=value)
  elif name == 'trigger_interval':
    dw = dw.trigger(processingTime=value)

  return dw

@register_spark_pipeline_step_implementation
def step_python(pipeline:SparkPipeline, *, outputs:List[str]=None, depends_on:List[Step]=None) -> Step:    
    def _step_wrapper(func):
      return Step(func, outputs=outputs, pipeline=pipeline, depends_on=depends_on)
    return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark(pipeline:SparkPipeline, *, outputs:List[str]=None, depends_on:List[Step]=None) -> Step:
  def _step_wrapper(func):
    
    @functools.wraps(func)
    def _logic_wrapper(step):
      return execute_step_decorated_function(func, step, outputs, DataFrame)

    return Step(_logic_wrapper, outputs=outputs, pipeline=pipeline, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_temp_view(pipeline:SparkPipeline, *, outputs:List[str]=None, depends_on:List[Step]=None) -> Step:
  def _step_wrapper(func):
    
    @functools.wraps(func)
    def _logic_wrapper(step):
      new_outputs = validate_step_outputs(func, outputs)
      data = execute_step_decorated_function(func, step, new_outputs, DataFrame)

      new_data = []
      for df, name in zip(data, new_outputs):          
        df.createOrReplaceTempView(name)
        new_data.append(table(name)) 

      return new_data

    return Step(_logic_wrapper, outputs=outputs, pipeline=pipeline, depends_on=depends_on)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_table(pipeline:SparkPipeline, *, outputs:List[str]=None, depends_on:List[Step]=None, 
                      mode:str="overwrite", format:str="delta", partition_by:List[str]=None, options:dict=None, table_properties:dict=None, auto_create_table=True) -> Step:

  allowed_modes = ['overwrite', 'overwrite_partitions', 'create', 'replace', 'append']
  if mode not in allowed_modes:
    raise ValueError(f'Invalid mode: {mode}, allowed modes are: {allowed_modes}')

  options = options or {}
  table_properties = table_properties or {}


  def _step_wrapper(func):
    @functools.wraps(func)
    def _logic_wrapper(step:Step):
      new_outputs = validate_step_outputs(func, outputs)
      data = execute_step_decorated_function(func, step, new_outputs, DataFrame)
      spark:SparkSession = step.pipeline._spark

      if len(new_outputs) != 1 and step._lazy_function_spark_metrics:
        raise ValueError("spark metrics can only be used with single output")

      new_data = []
      for df, name in zip(data, new_outputs):
        (df, observers) = apply_spark_metrics_observers(df, step._lazy_function_spark_metrics, step.log)

        step.log.debug(f"Writing {name}...")
        w = df.writeTo(name).using(format).options(**options)
        if partition_by:
          w = w.partitionedBy(partition_by)
        
        for k, v in table_properties.items():
          w= w.tableProperty(k, v)

        def _create_if_table_does_not_exists(writer, name):
          if auto_create_table and not spark.catalog.tableExists(name):
            step.log.info(f"Creating new table {name}")
            writer.create()

        if mode == 'overwrite':
          w.createOrReplace()
        elif mode == 'overwrite_partitions':
          _create_if_table_does_not_exists(w, name)
          w.overwritePartitions()
        elif mode == 'create':
          w.create()
        elif mode == 'replace':
          w.replace()
        elif mode == 'append':
          _create_if_table_does_not_exists(w, name)
          w.append()

        spark_metrics = get_observed_batch_spark_metrics(observers)

        step.log.debug(f"Wrote {mode=} {name}... metrics: {spark_metrics}")
        new_data = table(name)

        step.metrics.update(spark_metrics)

      return new_data
    
    return Step(_logic_wrapper, outputs=outputs, pipeline=pipeline, depends_on=depends_on, spark_metrics_supported=True)
  return _step_wrapper

@register_spark_pipeline_step_implementation
def step_spark_for_each_batch(pipeline:SparkPipeline, *, input_table:str=None, outputs:List[str]=None, depends_on:List[Step]=None, 
                              trigger_once:bool=False, trigger_availableNow:bool=False, trigger_interval:str=None, 
                              options:dict=None, output_mode:str=None):
  options = options or {}
  depends_on = pipeline._resolve_depends_on(depends_on)
  
  if not input_table and len(depends_on) == 1 and len(depends_on[0].outputs) == 1:
    input_table = depends_on[0].outputs[0]

  if not input_table:
    raise ValueError("input_table is not defined and connot be implicitly derived from depends_on because there are multiple dependencies defined")
    
  validate_xor_values(trigger_once=trigger_once, trigger_availableNow=trigger_availableNow, trigger_interval=trigger_interval)

  def _step_wrapper(func):
    nonlocal outputs
    outputs = validate_step_outputs(func, outputs)

    @functools.wraps(func)
    def _logic_wrapper(step:Step):
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
        f = func
        f = bdq.SparkUILogger.tag(desc=f"{step.pipeline.name}#{step.name}")(f)

        nonlocal relative_batch_id
        r = f(df, batch_id, relative_batch_id, step)
        relative_batch_id = relative_batch_id + 1
        return r
      
      step.streaming_unblock_event = threading.Event()

      dw = streaming_df.writeStream \
        .option('checkpointLocation', step.streaming_checkpoint_location) \
        .options(**options) \
        .queryName(step.streaming_query_name) \
        .foreachBatch(_feb_func_wrapper) \
        
      if output_mode:
        dw = dw.outputMode(output_mode)
        
      dw = apply_spark_streaming_trigger(dw, trigger_once=trigger_once, trigger_interval=trigger_interval, trigger_availableNow=trigger_availableNow)
      sq = dw.start()

      step.streaming_query = sq

      # wait for event; desired amount of batches
      step.streaming_unblock_event.wait()
      
      # raise if stream terminated because of error
      ex = sq.exception()
      if ex:
        raise ex

      return [table(n) for n in outputs]

    # i should make new class instead of monkey patching this one, but who is to stop me?
    step = Step(_logic_wrapper, outputs=outputs, pipeline=pipeline, depends_on=depends_on)
    step.streaming_query_name:str = f"{step.pipeline.name}#{step.name}"
    step.streaming_checkpoint_location:str = validate_spark_streaming_checkpoint_location(pipeline, func)
    step.streaming_query:StreamingQuery = None
    step.streaming_unblock_event = None

    return step
  
  return _step_wrapper
