from multiprocessing import synchronize
import sys, os, serpent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.DataNodeVitals import DataNodeVitals
from mydfs.utils.lock_decorator import synchronized


class DataNodesConnected:
  def __init__(self):
    self.__data_nodes: dict[DataNodeVitals] = {}

  @synchronized
  def update_data_node_vitals(self, token: str, vitals: dict):
    if token not in self.__data_nodes:
      self.__data_nodes[token] = DataNodeVitals(token, cpu_usage=vitals['cpu_usage'], ram_available=vitals['ram_available'], disk_available=vitals['disk_available'])
    else:
      self.__data_nodes[token].update_vitals(vitals['cpu_usage'], vitals['ram_available'], vitals['disk_available'])

  def __str__(self):
    return serpent.dumps(self.__data_nodes).decode('utf-8')
    