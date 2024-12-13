import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.Shard import Shard
from mydfs.utils.lock_decorator import synchronized

class File():
  def __init__(self, name: str, size: int):
    self.name: str = name
    self.size: int = size
    self.shards: list[Shard] = []

  @synchronized
  def add_shard(self, shard: Shard):
    self.shards.append(shard)

  @synchronized
  def remove_shard(self, shard: Shard):
    self.shards.remove(shard)
  
  def get_shard_by_index(self, index: int) -> Shard:
    return self.shards[index]