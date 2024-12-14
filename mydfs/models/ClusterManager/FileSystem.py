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
  def __save_to_disk(self, file_path: str):
    with open(file_path, 'w') as f:
      json.dump(self.files, f, default=lambda o: o.__dict__)
  
  def __load_from_disk(self, file_path: str):
      with open(file_path, 'w') as f:
        self.files = json.load(f.read())

  def __str__(self):
    return json.dumps(self.files, default=lambda o: o.__dict__, indent=2)