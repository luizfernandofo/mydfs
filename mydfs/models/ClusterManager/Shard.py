import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.utils.lock_decorator import synchronized


class Shard():
  def __init__(self, data_node_owners: list[str] = []):
    self.data_node_owners = data_node_owners
    self.replications_requested = 0
    
  @synchronized
  def add_data_node_owner_if_not_exists(self, data_node_token: str):
    if data_node_token not in self.data_node_owners:
      self.data_node_owners.append(data_node_token)

  @synchronized
  def remove_data_node_owner(self, data_node_token: str):
    self.data_node_owners.remove(data_node_token)

  @synchronized
  def get_replication_factor(self):
    return len(self.data_node_owners)  
  
  @synchronized
  def has_any_owner(self):
    return len(self.data_node_owners) > 0
  
  @synchronized
  def increase_replications_requested(self):
    self.replications_requested += 1

  @synchronized 
  def decrease_replications_requested(self):
    self.replications_requested -= 1