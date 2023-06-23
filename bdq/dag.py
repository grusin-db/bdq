import threading
import concurrent.futures as CF
from collections.abc import Iterable

class Node:
  def __init__(self):
    self.children: set[callable] = set()
    self.parents: set[callable] = set()
    self.completed = threading.Event()
    self.future: CF.Future = None
    self.exception = None
    self.result = None

class DAG:
  BREAK = threading.Event()

  def __init__(self):
    self.nodes: dict[callable, Node] = {}

  def node(self, *depends_on):
    depends_on = depends_on or []

    if not isinstance(depends_on, Iterable):
      raise ValueError(f"depends_on must be a list of callables, instead got: {depends_on}")
    
    def _graph_node(node):
      self.nodes[node] = (self.nodes.get(node) or Node())

      for p in depends_on:
        if not callable(p):
          raise ValueError(f"{node=} dependency {p=} is not callable")
        self.nodes[node].parents.add(p)

      for dep in depends_on:
        self.nodes[dep] = (self.nodes.get(dep) or Node())
        self.nodes[dep].children.add(node)

      return node
    
    return _graph_node
  
  def is_dependency_met(self, node):
    for p in self.nodes[node].parents:
      if not self.nodes[p].completed.is_set() or self.nodes[p].result == DAG.BREAK:
        return False
      
    return True
  
  def execute(self, max_workers, verbose=True):
    lock = threading.RLock()
    running_nodes = 0
    all_nodes_finished_event = threading.Event()
    executor = CF.ThreadPoolExecutor(max_workers=max_workers)
    
    def _get_done_callback(node: Node):
      def _handle_done(fn):
        with lock:
          self.nodes[node].completed.set()
          
          if not fn.exception():
            self.nodes[node].result = fn.result()
            for c in self.nodes[node].children:
              if self.is_dependency_met(c):
                _start(c)

          nonlocal running_nodes
          running_nodes = running_nodes - 1

          if fn.exception():
            if verbose:
              print(f"  error: {node}: {fn.exception()} (still running: {running_nodes})")
            self.nodes[node].exception = fn.exception()
          else:
            if verbose:
              print(f"  finished: {node}, result: {fn.result()} (still running: {running_nodes})")

          if running_nodes == 0:
            all_nodes_finished_event.set()
        
      return _handle_done

    def _start(node):
      with lock:
        if verbose:
          print(f"  starting: {node}")
        
        nonlocal running_nodes
        running_nodes = running_nodes + 1

        fn = executor.submit(node)
        self.nodes[node].future = fn
        fn.add_done_callback(_get_done_callback(node))

    if verbose:
      print("Waiting for all tasks to finish...")
    for n in self.nodes:
      if self.is_dependency_met(n):
        _start(n)

    all_nodes_finished_event.wait()
    if verbose:
      print("All tasks finished, shutting down")
    executor.shutdown()