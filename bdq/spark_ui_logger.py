import functools
import threading
import logging
from pyspark.sql import SparkSession
from pyspark import SparkContext

class SparkUILogger:
  default_log_level = log_level=logging.INFO
  _thread_local = threading.local()

  def __init__(self, desc, spark:SparkSession=None, log_level=None):  
    # init stack
    if  getattr(self._thread_local, '_SparkUILogger_stack', None) is None:
      self._thread_local._SparkUILogger_stack = []

    self._stack = self._thread_local._SparkUILogger_stack
    self._desc = ".".join(self._stack + [desc]) 

    # init logger
    log_level = log_level or self.default_log_level
    self.log:logging.Logger = logging.getLogger(self._desc)
    self.log.setLevel(log_level)

    # init spark
    self._spark = spark or SparkSession.getActiveSession()

    if not self._spark:
      raise ValueError("could not get active spark session")

    self._sc = self._spark.sparkContext
    
  def set_job_description(self, desc):
    self._sc.setJobDescription(desc)
    self._sc.setLocalProperty(
      "spark.job.description",
      desc
    )

  def __enter__(self):
    self.log.debug(f"entering with stack: {self._stack}")

    self.set_job_description(self._desc)
    self._stack.append(self._desc)

  def __exit__(self, exc_type, exc_value, traceback):
    assert self._stack.pop() == self._desc, "SparkUILogger's stack is corrupted"
    desc = self._stack[-1] if self._stack else None
    self.set_job_description(desc)

    self.log.debug(f"leaving into: {desc}")

  @staticmethod
  def tag(function=None, desc=None, log_level=None):
    def actual_decorator(f):
      @functools.wraps(f)
      def wrapper(*args, **kwargs):
        d = desc if desc else f.__qualname__ 

        with SparkUILogger(d, log_level=log_level):  
          return f(*args, **kwargs)
      return wrapper
    
    if function:
      return actual_decorator(function)
    return actual_decorator