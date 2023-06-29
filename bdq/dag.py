import threading
import concurrent.futures as CF
from collections.abc import Iterable

__all__ = [ 
  'Node',
  'DAG'
]

class DAG:
  ...

class Node:
  @property
  def state(self):
    if self.exception:
      return "ERROR"
    if self.result == DAG.BREAK:
        return "SKIPPED"
    
    if self.completed.is_set():
      return "SUCCESS"
    elif self.future is not None:
      return "RUNNING"
    
    return "SKIPPED"
  
  @property
  def nodes(self):
    return self.dag.nodes

  def __init__(self, function, dag:DAG=None):
    if function is None or not callable(function):
      raise ValueError("function must be a callable, not may not be None")
    
    self.name = function.__name__
    self.dag = dag
    self.function = function
    self.children: set[callable] = set()
    self.parents: set[callable] = set()
    self.completed = threading.Event()
    self.future: CF.Future = None
    self.exception:Exception = None
    self.result:Any = None

  def _reset(self):
    self.completed = threading.Event()
    self.future = None
    self.exception = None
    self.result = None

  def __repr__(self):
    res = { 
      'state': self.state
      ,'result': self.result
      ,'exception': self.exception
      ,'completed': self.completed.is_set()
    }
    return f"Node({self.function}: {res} )"

class DAG:
  BREAK = threading.Event()

  def __init__(self):
    self.nodes: dict[callable, Node] = {}

  def node(self, *, depends_on=[]):
    depends_on = depends_on or []

    if not isinstance(depends_on, Iterable):
      raise ValueError(f"depends_on must be a list of callables, instead got: {depends_on}")
    
    def _graph_node(fun):
      if fun in self.nodes:
        raise ValueError("function {fun} is already present in a DAG")
      
      self.nodes[fun] = Node(fun, self)

      for p in depends_on:
        if not callable(p):
          raise ValueError(f"{fun} dependency '{p}' is not callable")
        self.nodes[fun].parents.add(p)

      for dep in depends_on:
        self.nodes[dep] = (self.nodes.get(dep) or Node(dep, self))
        self.nodes[dep].children.add(fun)

      return fun
    
    return _graph_node
  
  def add_edge(self, from_node, to_node):
    if from_node not in self.nodes:
      raise ValueError(f"from_node does not exist: {from_node}")
    
    if to_node not in self.nodes:
      raise ValueError(f"to_node does not exist: {to_node}")
    
    self.nodes[from_node].children.add(to_node)
    self.nodes[to_node].parents.add(from_node)
  
  def is_dependency_met(self, node):
    for p in self.nodes[node].parents:
      if not self.nodes[p].completed.is_set() or self.nodes[p].result == DAG.BREAK:
        return False
      
    return True
  
  def is_success(self) -> bool:
    return self.get_error_nodes() == []

  def _get_nodes_with_state(self, state):
    return [
      n
      for n in self.nodes.values()
      if n.state == state
    ]

  def get_error_nodes(self) -> list[Node]:
    return self._get_nodes_with_state("ERROR")

  def get_skipped_nodes(self) -> list[Node]:
    return self._get_nodes_with_state("SKIPPED")

  def get_success_nodes(self) -> list[Node]:
    return self._get_nodes_with_state("SUCCESS")

  def _reset_nodes(self):
    for n in self.nodes.values():
      n._reset()
  
  def execute(self, max_workers, verbose=True):
    lock = threading.RLock()
    running_nodes = 0
    all_nodes_finished_event = threading.Event()
    executor = CF.ThreadPoolExecutor(max_workers=max_workers)

    self._reset_nodes()
    
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
              print(f"  finished: {node} (still running: {running_nodes})")

          if running_nodes == 0:
            all_nodes_finished_event.set()
        
      return _handle_done

    def _start(node):
      with lock:

        
        nonlocal running_nodes
        running_nodes = running_nodes + 1

        fn = executor.submit(node)
        self.nodes[node].future = fn
        
        if verbose:
          print(f"  starting: {node}")
        
        fn.add_done_callback(_get_done_callback(node))

    if verbose:
      print("Waiting for all tasks to finish...")
    
    if not self.nodes:
      all_nodes_finished_event.set()
    else:
      for n in self.nodes:
        if self.is_dependency_met(n):
          _start(n)

    all_nodes_finished_event.wait()
    if verbose:
      print("All tasks finished, shutting down")
    
    executor.shutdown()