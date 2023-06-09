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
  # spark ui stages/sql actions will be visible as 'xyz'
  @SparkUILogger.tag(desc='xyz')
  def some_function2(number):

    return spark.range(number).count()

  # spark ui stages/sql actions will be visible as function name, that is 'alpha_function2'
  @SparkUILogger.tag
  def alpha_function2(number):
    # two some_function2() calls should be visible in spark ui as 'xyz' (due to function above)
    # the 3rd part of result should be wrapped in 'alpha_function2
    return some_function2(number*2) + some_function2(number*3) + spark.range(number/10).count()

  # will be visible in spark ui as 'first-count'
  with SparkUILogger('first-count'):
    spark.range(10).count()

  # will be isible in spark ui as '2nd-count'
  with SparkUILogger('2nd-count'):
    spark.range(20).count()

  some_function2(1000)
  alpha_function2(2000)