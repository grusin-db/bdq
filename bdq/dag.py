import threading
import concurrent.futures as CF
import traceback
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
  
  @property
  def traceback(self):
    if self.exception:
      return "".join(traceback.format_tb(self.exception.__traceback__))
  
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
    self._running_nodes = 0

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
      if not p.completed.is_set() or p.result == DAG.BREAK or p.exception:
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
      print("pip package `ipydagred3` not installed, `%pip install ipydagred3` and rerun to see beautiful live visualization")
      return None
    
    import ipydagred3
    return ipydagred3.DagreD3Widget(graph=self._vizg)
  
  def execute(self, max_workers, verbose=True):
    lock = threading.RLock()
    self._running_nodes = 0
    all_nodes_finished_event = threading.Event()
    executor = CF.ThreadPoolExecutor(max_workers=max_workers)

    self.reset_nodes()
    
    def _get_done_callback(node: Node):
      def _handle_done(fn: CF.Future):
        with lock:
          self._running_nodes = self._running_nodes - 1

          if node.exception:
            if verbose:
              print(f"  error: {node}: {node.exception} (running: {self._running_nodes})")
          else:
            if verbose:
              print(f"  finished: {node} (running: {self._running_nodes})")

          if not node.exception:
            started_nodes = _start_if_dependenyc_met(node.children)
          else:
            started_nodes = []

          if self._running_nodes == 0:
            all_nodes_finished_event.set()

        _add_done_callback(started_nodes)
        
      return _handle_done

    def _add_done_callback(nodes: list[Node]):
      # must be called outside of lock, otherwise it might execute callback imediately
      # and will lead to deadlock
      for node in nodes:
        node.future.add_done_callback(_get_done_callback(node))

    def _start_if_dependenyc_met(nodes: list[Node]):
      with lock:
        started_nodes:list[Node] = []
        for node in nodes:
          if not self.is_dependency_met(node) or node.future is not None:
            continue

          self._running_nodes = self._running_nodes + 1

          node.future = executor.submit(node)
          node._viz_update_state()
          
          if verbose:
            print(f"  starting: {node} (running: {self._running_nodes})")
        
          started_nodes.append(node)

      return started_nodes

    if verbose:
      print("Waiting for all tasks to finish...")
    
    if self.nodes:
      _add_done_callback(_start_if_dependenyc_met(self.nodes))
    else:
      all_nodes_finished_event.set()

    all_nodes_finished_event.wait()
    if verbose:
      print("All tasks finished, shutting down")
    
    executor.shutdown()

  def __call__(self, max_workers, verbose=True):
    return self.execute(max_workers=max_workers, verbose=verbose)
