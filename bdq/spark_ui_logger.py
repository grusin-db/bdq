import functools
from pyspark.sql import SparkSession
from pyspark import SparkContext

class SparkUILogger:
  stack:list[str] = []
  
  def __init__(self, desc, sc:SparkContext=None):  
    self.sc = sc
    self.desc = desc

    if not self.sc:
      spark = SparkSession.getActiveSession()
      if not spark:
        raise ValueError("could not get active spark session")
      self.sc = spark.sparkContext

  def __enter__(self):
    #print(f">>> {self.desc}: {self.stack}")
    self.sc.setJobDescription(self.desc)
    self.stack.append(self.desc)

  def __exit__(self, exc_type, exc_value, traceback):
    assert self.stack.pop() == self.desc
    desc = self.stack[-1] if self.stack else None
    self.sc.setJobDescription(desc)
    #print(f"<<< {self.desc} -> {desc}")

  @staticmethod
  def tag(function=None, desc=None):
    def actual_decorator(f):
      @functools.wraps(f)
      def wrapper(*args, **kwargs):
        d = desc if desc else f.__qualname__ 

        with SparkUILogger(d):
          return f(*args, **kwargs)
      return wrapper
    if function:
      return actual_decorator(function)
    return actual_decorator