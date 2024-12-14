class Shard:
  def __init__(self, file_name: str, file_path: str, shard_size: int):
    self.__file_name = file_name
    self.__file_path = file_path
    self.__shard_size = shard_size

  @property
  def file_name(self) -> str:
    return self.__file_name
  
  @file_name.setter
  def file_name(self, value: str):
    self.__file_name = value

  @property
  def file_path(self) -> str:
    return self.__file_path
  
  @file_path.setter
  def file_path(self, value: str):
    self.__file_path = value

  @property
  def shard_size(self) -> int:
    return self.__shard_size
  
  @shard_size.setter
  def shard_size(self, value: int):
    self.__shard_size = value

if __name__ == "__main__":
  s = Shard("teste.txt", "/tmp", 10)
  print(s.file_name)
  s.file_name = "teste2.txt"
  print(s.file_name)