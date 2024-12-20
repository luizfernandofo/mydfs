import os
import json
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.File import File
from mydfs.utils.lock_decorator import synchronized


class FileSystem:
  def __init__(self):
    if os.path.exists('filesystem.txt'):
      self.__load_from_disk('filesystem.txt')
    else:
      self.files: dict[str, File] = {}

  @synchronized
  def create_file(self, name: str, size: int):
    self.files[name] = File(name, size)

  @synchronized
  def update_shard_owners_by_file_name(self, file_name: str, shard_index: int, data_node_token: str):
    self.files[file_name].update_shard_owners(shard_index, data_node_token)

  @synchronized
  def remove_dead_shard_owners(self, data_node_tokens: list[str]):
    for file in self.files.values():
      for shard in file.get_shards():
        for data_node_token in data_node_tokens:
          shard.remove_data_node_owner(data_node_token)

  @synchronized
  def get_shards_owners_by_file_name(self, file_name: str) -> list[list[str]]:
    return self.files[file_name].get_shards_owners()

  @synchronized
  def decrease_replications_requested(self, file_name: str, shard_index: int):
    self.files[file_name].get_shard_by_index(shard_index).decrease_replications_requested()

  @synchronized
  def file_exists(self, file_name: str) -> bool:
    return file_name in self.files
  
  def get_file_size(self, file_name: str) -> int:
    return self.files[file_name].size

  @synchronized
  def get_alternative_shard_owner(self, shard_name: str, data_node_token: str) -> str:
    return self.files[shard_name.split('-')[0]].get_alternative_shard_owner(int(shard_name.split('-')[1]), data_node_token)

  @synchronized
  def __save_to_disk(self, file_path: str):
    with open(file_path, 'w') as f:
      json.dump(self.files, f, default=lambda o: o.__dict__)
  
  def __load_from_disk(self, file_path: str):
      with open(file_path, 'w') as f:
        self.files = json.load(f.read())

  def __str__(self):
    return json.dumps(self.files, default=lambda o: o.__dict__, indent=2)