from multiprocessing import synchronize
import sys, os, serpent
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.DataNodeVitals import DataNodeVitals
from mydfs.utils.lock_decorator import synchronized


class DataNodesConnected:
  def __init__(self):
    self.__data_nodes: dict[str, DataNodeVitals] = {}
    self.__LAST_UPDATE_THRESHOLD = 45 # seconds
    self.__CPU_USAGE_THRESHOLD = 70 # percent
    self.__MINIUM_SHARD_TO_STORE = 8

  def __remove_dead_data_nodes(self):
    tokens_to_remove = [token for token, data_node in self.__data_nodes.items() if data_node.time_since_last_update() > self.__LAST_UPDATE_THRESHOLD]
    for token in tokens_to_remove:
      del self.__data_nodes[token]

  @synchronized
  def update_data_node_vitals(self, token: str, vitals: dict):
    if token not in self.__data_nodes:
      self.__data_nodes[token] = DataNodeVitals(token, cpu_usage=vitals['cpu_usage'], ram_available=vitals['ram_available'], disk_available=vitals['disk_available'])
    else:
      self.__data_nodes[token].update_vitals(vitals['cpu_usage'], vitals['ram_available'], vitals['disk_available'])

    self.__remove_dead_data_nodes()

  @synchronized
  def any_data_node_connected(self) -> bool:
    return len(self.__data_nodes) > 0

  @synchronized
  def get_suitable_data_nodes_for_upload(self) -> list[str]:
    if len(self.__data_nodes) == 1:
      return list(self.__data_nodes.keys())
    
    dn = [token for token, data_node in self.__data_nodes.items() if
          data_node.time_since_last_update() < self.__LAST_UPDATE_THRESHOLD
          and data_node.cpu_usage <= self.__CPU_USAGE_THRESHOLD
          and data_node.can_store_n_shards(self.__MINIUM_SHARD_TO_STORE)]
    return dn

  @synchronized
  def data_node_isnt_stressed(self, token: str) -> bool:
    return self.__data_nodes[token].cpu_usage <= self.__CPU_USAGE_THRESHOLD

  @synchronized
  def get_data_nodes(self) -> dict[str, DataNodeVitals]:
    return self.__data_nodes

  def __str__(self):
    return serpent.dumps(self.__data_nodes).decode('utf-8')    