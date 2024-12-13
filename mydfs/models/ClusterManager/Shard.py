import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.utils.lock_decorator import synchronized


class Shard():
  def __init__(self, data_node_owner: list[str] = []):
    self.data_node_owner = data_node_owner
    
  @synchronized
  def add_data_node_owner_if_not_exists(self, data_node_token: str):
    if data_node_token not in self.data_node_owner:
      self.data_node_owner.append(data_node_token)

  @synchronized
  def remove_data_node_owner(self, data_node_token: str):
    self.data_node_owner.remove(data_node_token)

  def get_replica_factor(self):
    return len(self.data_node_owner)  