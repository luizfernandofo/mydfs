import sys
import os
import Pyro5.api

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.FileSystem import FileSystem
from mydfs.utils.lock_decorator import synchronized

@Pyro5.api.expose
class ClusterManagerService:
  def __init__(self):
    self.__file_system = FileSystem()

  def report_shards(self, token: str, shard_name_list: list[str]):
    shard_tuples = [(shard.split('-')[0], int(shard.split('-')[1])) for shard in shard_name_list]
    for file_name, shard_index in shard_tuples:
      self.__file_system[file_name][shard_index].add_data_node_owner_if_not_exists(token)

if __name__ == "__main__":
  print("Cluster Manager Service started")
  with Pyro5.api.Daemon() as daemon:
    uri = daemon.register(ClusterManagerService(), "cluster.manager")
    try:
      ns = Pyro5.api.locate_ns()
      ns.register("cluster-manager", uri)
    except Exception as e:
      print(f"Failed to locate nameserver: {e}")
      exit(1)
    daemon.requestLoop()
