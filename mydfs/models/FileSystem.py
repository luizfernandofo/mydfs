import os
from typing import List
from mydfs.models.File import File

class FileSystem:
  def __init__(self):
    if os.path.exists('filesystem.txt'):
      self.load_from_disk('filesystem.txt')

  def save_to_disk(self, file_path: str):
    with open(file_path, 'w') as f:
      for file in self.files:
        f.write(f"{file}\n")
  
  def load_from_disk(self, file_path: str):
    with open(file_path, 'r') as f:
      self.files = [File.from_string(line.strip()) for line in f]
    