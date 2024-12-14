import time


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