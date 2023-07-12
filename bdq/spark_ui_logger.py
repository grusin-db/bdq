import functools
import threading
from pyspark.sql import SparkSession
from pyspark import SparkContext

class SparkUILogger:  
  def __init__(self, desc, spark:SparkSession=None, verbose=False):  
    self._spark = spark or SparkSession.getActiveSession()
    
    if not self._spark:
      raise ValueError("could not get active spark session")

    self._sc = self._spark.sparkContext
    self._desc = desc
    self._verbose = verbose
    self._stack = threading.local()._SparkUILogger_stack = []
   
  def set_job_description(self, desc):
    self._sc.setLocalProperty(
      "spark.job.description",
      desc
    )

  def __enter__(self):
    if self._verbose:
      print(f">>> {self._desc}: {self._stack}")

    self.set_job_description(self._desc)
    self._stack.append(self._desc)

  def __exit__(self, exc_type, exc_value, traceback):
    assert self._stack.pop() == self._desc, "SparkUILogger's stack is corrupted"
    desc = self._stack[-1] if self._stack else None
    self.set_job_description(desc)
    if self._verbose:
      print(f"<<< {self._desc} -> {desc}")

  @staticmethod
  def tag(function=None, desc=None, verbose=False):
    def actual_decorator(f):
      @functools.wraps(f)
      def wrapper(*args, **kwargs):
        d = desc if desc else f.__qualname__ 

        with SparkUILogger(d, verbose=verbose):  
          return f(*args, **kwargs)
      return wrapper
    if function:
      return actual_decorator(function)
    return actual_decorator