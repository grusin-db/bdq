import bdq
import time

#create graph
graph = bdq.DAG()

#define nodes
@graph.node()
def a():
  time.sleep(2)

@graph.node()
def b():
  time.sleep(3)
  return "beeep"

#nodes can depend on other nodes
@graph.node(depends_on=[b])
def c():
  time.sleep(5)

#any amount of dependencies is allowed
@graph.node(depends_on=[b, c, a])
def d():
  time.sleep(7)
  return "g man"

@graph.node(depends_on=[a])
def e():
  time.sleep(3)
  raise ValueError("omg, crash!")

@graph.node(depends_on=[e])
def f():
  print("this will never execute")

@graph.node(depends_on=[a])
def g():
  time.sleep(3)
  return graph.BREAK

@graph.node(depends_on=[g])
def i():
  print("this will never execute too")

#execute DAG
graph.execute(max_workers=10)

#iterate over results
for fun, node in graph.nodes.items():
  print(f"{fun}: {node.result=}, {node.completed.is_set()=}, {node.exception=}, {node.state=}")

assert set(f.function.__name__ for f in graph.get_error_nodes()) == {'e'}
assert set(f.function.__name__ for f in graph.get_skipped_nodes()) == {'f', 'g', 'i'}
assert set(f.function.__name__ for f in graph.get_success_nodes()) == {'a', 'b', 'c', 'd'}
assert graph.is_success() == False