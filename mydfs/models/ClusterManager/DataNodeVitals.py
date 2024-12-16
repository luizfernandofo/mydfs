import time, sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.utils.shared import *

class DataNodeVitals:
  def __init__(self, token: str, cpu_usage: float, ram_available: int, disk_available: int):
    self.__token: str = token
    self.__cpu_usage: float = cpu_usage
    self.__ram_available: int = ram_available
    self.__disk_available: int = disk_available
    self.__last_updated: int = time.time_ns()

  def update_vitals(self, cpu_usage: float, ram_available: int, disk_available: int):
    self.__cpu_usage = cpu_usage
    self.__ram_available = ram_available
    self.__disk_available = disk_available
    self.__last_updated = time.time_ns()

  def time_since_last_update(self) -> int:
    return (time.time_ns() - self.__last_updated) // 1e9
  
  def can_store_n_shards(self, n: int) -> bool:
    return self.__disk_available > n * SHARD_SIZE
  
  @property
  def cpu_usage(self) -> float:
    return self.__cpu_usage
  
  @property
  def ram_available(self) -> int:
    return self.__ram_available
  
  @property
  def disk_available(self) -> int:
    return self.__disk_available