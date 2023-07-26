import bdq
import time
import pytest

def test_dag():
  #create graph
  graph = bdq.DAG()

  #define nodes
  @graph.node()
  def a():
    time.sleep(2)
    return 5

  @graph.node()
  def b():
    time.sleep(3)
    return "beeep"

  #nodes can depend on other nodes
  @graph.node(depends_on=[b])
  def c():
    time.sleep(5)
    return 8

  #any amount of dependencies is allowed
  @graph.node(depends_on=[b, c, a])
  def d():
    time.sleep(7)
    #beep as many times as absolute difference between 'c' and 'a'
    return "g man say: " + b.result * abs(c.result - a.result)

  @graph.node(depends_on=[a])
  def e():
    time.sleep(3)
    #you can refer to parent nodes, to get information about their results
    raise ValueError(f"omg, crash! {a.result}")

  @graph.node(depends_on=[e])
  def f():
    print("this will never execute")
    return "this will never execute"

  @graph.node(depends_on=[a])
  def g():
    time.sleep(3)
    #we do not want to execute children nodes anymore
    #return graph.BREAK this will cause that all children/successors in graph will be skipped without errror
    return graph.BREAK

  @graph.node(depends_on=[g])
  def i():
    print("this will never execute too")
    return "this will never execute too"

  #execute DAG
  graph.execute(max_workers=10)

  #iterate over results
  print("Iterate over results...")
  for node in graph.nodes:
    print(node)

  #some asserts
  assert set(f.function.__name__ for f in graph.get_error_nodes()) == {'e'}
  assert set(f.function.__name__ for f in graph.get_skipped_nodes()) == {'f', 'g', 'i'}
  assert set(f.function.__name__ for f in graph.get_success_nodes()) == {'a', 'b', 'c', 'd'}
  assert graph.is_success() == False

  assert a.result == 5
  assert b.result == 'beeep'
  assert d.result == 'g man say: beeepbeeepbeeep'
  assert g.result == graph.BREAK

  ## let's rerun node again
  # reset state
  a.reset()
  assert a.result == None

  # run node for 2nd time
  a()
  assert a.result == 5
  # running again is also possible, without reset
  a()
  assert a.result == 5
  print("a() results:", a)

  ## let's run node that should throw exception
  e.reset()
  assert e.result == None
  assert e.exception == None

  with pytest.raises(ValueError):
    e()

  assert e.exception is not None
  assert isinstance(e.exception, ValueError)
  print("e() results:", e)

if __name__ == "__main__":
  test_dag()
  