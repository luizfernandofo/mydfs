from mydfs.models.Shard import Shard
from typing import List


class File():
  def __init__(self, name: str, size: int):
    self.name = name
    self.size = size
    self.shards: List[Shard] = []
