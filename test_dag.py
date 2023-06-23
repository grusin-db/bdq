import bdq
import time

graph = bdq.DAG()

@graph.node()
def a():
  time.sleep(2)

@graph.node()
def b():
  time.sleep(3)
  return "beeep"

@graph.node(b)
def c():
  time.sleep(5)

@graph.node(b, c, a)
def d():
  time.sleep(7)
  return "g man"

@graph.node(a)
def e():
  time.sleep(3)
  raise ValueError("omg, crash!")

@graph.node(e)
def f():
  print("this will never execute")

graph.execute(max_workers=10)

for node, state in graph.nodes.items():
  print(f"{node}: {state.result=}, {state.completed.is_set()=}, {state.exception=}")