class Shard():
  def __init__(self, index: int, replicas: int):
    self.index = index
    self.replicas = replicas
    self.data_nodes_owning = []