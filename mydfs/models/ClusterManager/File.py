import sys
import os
from math import ceil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.Shard import Shard
from mydfs.utils.lock_decorator import synchronized
from mydfs.utils.shared import *

class File():
  def __init__(self, name: str, size: int):
    self.name: str = name
    self.size: int = size
    self.upload_finished: bool = False
    self.shards: list[Shard] = [Shard([]) for _ in range(ceil(size / SHARD_SIZE))]

  @synchronized
  def add_shard(self, shard: Shard):
    self.shards.append(shard)

  @synchronized
  def remove_shard(self, shard: Shard):
    self.shards.remove(shard)
  
  def get_shard_by_index(self, index: int) -> Shard:
    return self.shards[index]
  
  @synchronized
  def update_shard_owners(self, shard_index: int, data_node_token: str):
    self.get_shard_by_index(shard_index).add_data_node_owner_if_not_exists(data_node_token)
    