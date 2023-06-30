import threading
import concurrent.futures as CF
from collections.abc import Iterable, Callable
from typing import Any

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
    self.children: set[Node] = set()
    self.parents: set[Node] = set()
    self.completed = threading.Event()
    self.future: CF.Future = None
    self.exception:Exception = None
    self.result = None

    self._viz_update_state()

  def _viz_update_state(self):
    viz = self.dag._vizg
    if not viz:
      return
    
    s = self.state

    if s == "ERROR":
      style = "fill: #f77"
    elif s == "SUCCESS":
      style = "fill: #7f7"
    elif s == "RUNNING":
      style = "fill: #77f"
    else:
      style = "fill: #fff"

    viz.setNode(self.name, style=style)
    
  def reset(self):
    self.completed = threading.Event()
    self.exception = None
    self.result = None

    self._viz_update_state()

  def __repr__(self):
    res = { 
      'state': self.state
      ,'result': self.result
      ,'exception': self.exception
      ,'completed': self.completed.is_set()
    }
    return f"Node({self.function}: {res} )"
  
  def __call__(self, *args: Any, **kwds: Any) -> Any:
    self.reset()

    try:
      self.result = self.function(*args, **kwds)
      self.completed.set()
      self._viz_update_state()
    except Exception as e:
      self.exception = e
      self.completed.set()
      self._viz_update_state()
      raise e

class DAG:
  BREAK = threading.Event()

  def __init__(self):
    self.nodes: dict[Node, Callable] = {}
    self.functions: dict[Callable, Node] = {}
    self._vizg = self._vizg_try_init()

  def _vizg_try_init(self):
    try:
      import ipydagred3
      return ipydagred3.Graph()
    except:
      return None

  def node(self, *, depends_on:list[Node]=[]):
    depends_on = depends_on or []

    if not isinstance(depends_on, Iterable):
      raise ValueError(f"depends_on must be a list of Nodes, instead got: {depends_on}")
    
    def _graph_node(fun):
      if fun in self.functions:
        raise ValueError(f"Function {fun} is already called by a graph")
  
      #add new node
      node = Node(fun, self)
      if node in self.nodes:
        raise ValueError(f"Node {node} is already present in a DAG")

      self.nodes[node] = fun
      self.functions[fun] = node

      #validate first
      for dep_node in depends_on:
        if not isinstance(dep_node, Node):
          raise ValueError(f"{node} dependency '{dep_node}' is not as node")

      #add once it's dage
      for dep_node in depends_on:
        self.add_edge(dep_node, node)

      return node
    
    return _graph_node
  
  def add_edge(self, from_node:Node, to_node:Node):
    if from_node not in self.nodes:
      raise ValueError(f"from_node does not exist: {from_node}")
    
    if not isinstance(from_node, Node):
      raise ValueError("from_node is not of Node type")
    
    if to_node not in self.nodes:
      raise ValueError(f"to_node does not exist: {to_node}")
    
    if not isinstance(to_node, Node):
      raise ValueError("to_node is not of Node type")
    
    from_node.children.add(to_node)
    to_node.parents.add(from_node)

    if self._vizg:
      self._vizg.setEdge(from_node.name, to_node.name)
  
  def is_dependency_met(self, node:Node):
    for p in node.parents:
      if not p.completed.is_set() or p.result == DAG.BREAK:
        return False
      
    return True
  
  def is_success(self) -> bool:
    return self.get_error_nodes() == []

  def _get_nodes_with_state(self, state:str):
    return [
      n
      for n in self.nodes
      if n.state == state
    ]

  def get_error_nodes(self) -> list[Node]:
    return self._get_nodes_with_state("ERROR")

  def get_skipped_nodes(self) -> list[Node]:
    return self._get_nodes_with_state("SKIPPED")

  def get_success_nodes(self) -> list[Node]:
    return self._get_nodes_with_state("SUCCESS")

  def reset_nodes(self):
    for n in self.nodes:
      n.reset()

  def visualize(self):
    if not self._vizg:
      print("pip package `ipydagred3` not installed, install and retry to see beautiful live visualization")
      return None
    
    import ipydagred3
    return ipydagred3.DagreD3Widget(graph=self._vizg)
  
  def execute(self, max_workers, verbose=True):
    lock = threading.RLock()
    running_nodes = 0
    all_nodes_finished_event = threading.Event()
    executor = CF.ThreadPoolExecutor(max_workers=max_workers)

    self.reset_nodes()
    
    def _get_done_callback(node: Node):
      def _handle_done(fn: CF.Future):
        with lock:
          if not node.exception:
            for c in node.children:
              if self.is_dependency_met(c):
                _start(c)

          nonlocal running_nodes
          running_nodes = running_nodes - 1

          if node.exception:
            if verbose:
              print(f"  error: {node}: {node.exception} (still running: {running_nodes})")
          else:
            if verbose:
              print(f"  finished: {node} (still running: {running_nodes})")

          if running_nodes == 0:
            all_nodes_finished_event.set()
        
      return _handle_done

    def _start(node: Node):
      with lock:
        nonlocal running_nodes
        running_nodes = running_nodes + 1

        node.future = executor.submit(node)
        node._viz_update_state()
        
        if verbose:
          print(f"  starting: {node}")
        
        node.future.add_done_callback(_get_done_callback(node))

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
