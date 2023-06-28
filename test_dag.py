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

#execute DAG
graph.execute(max_workers=10)

#iterate over results
for node, state in graph.nodes.items():
  print(f"{node}: {state.result=}, {state.completed.is_set()=}, {state.exception=}")