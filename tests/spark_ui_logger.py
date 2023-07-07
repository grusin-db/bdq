from bdq import SparkUILogger, spark

def test_with_logic():
  def some_function(number):
    with SparkUILogger(f'some_function({number=})'):
      return spark.range(number).count()

  def alpha_function(number):
    with SparkUILogger(f'alpha_function({number=})'):
      return some_function(number*2) + some_function(number*3) + spark.range(number/10).count()

  with SparkUILogger('first-count'):
    spark.range(10).count()

  with SparkUILogger('2nd-count'):
    spark.range(20).count()

  some_function(1000)
  alpha_function(2000)

def test_decorator_logic():
  @SparkUILogger.tag(desc='xyz')
  def some_function2(number):
    return spark.range(number).count()

  @SparkUILogger.tag
  def alpha_function2(number):
    return some_function2(number*2) + some_function2(number*3) + spark.range(number/10).count()

  with SparkUILogger('first-count'):
    spark.range(10).count()

  with SparkUILogger('2nd-count'):
    spark.range(20).count()

  some_function2(1000)
  alpha_function2(2000)

test_with_logic()
test_decorator_logic()