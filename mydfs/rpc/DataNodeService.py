import os
import sys
import uuid
import threading
import time
import Pyro5.api

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.utils.get_proxy_by_name import get_proxy_by_name
from mydfs.models.DataNode.Shard import Shard

INTERVAL_SHARD_REPORT = 10

if os.path.exists('data_node_config.txt'):
  with open('data_node_config.txt', 'r') as f:
    TOKEN = f.read().strip()
else:
  TOKEN = str(uuid.uuid4())
  with open('data_node_config.txt', 'w') as f:
    f.write(TOKEN)


@Pyro5.api.expose
class DataNodeService:
  def __init__(self):
    print(f"DataNode {TOKEN} started")
    self.TOKEN = TOKEN
    self.__cluster_manager_proxy = get_proxy_by_name("cluster-manager")
    self.__shards : list[Shard] = [Shard(file_name=f"shard_{i}.dat", file_path="/tmp", shard_size=10) for i in range(2)]
      
    try:
      self.__keep_running = True
      self.__report_thread = threading.Thread(target=self.__report_shards_periodically)
      self.__report_thread.start()
    except Exception as e:
      print(f"Failed to connect to cluster manager: {e}")

  def __report_shards_periodically(self):
    cluster_manager_proxy = get_proxy_by_name("cluster-manager")
    while self.__keep_running:
      try:
        cluster_manager_proxy.report_shards(self.TOKEN, [shard.file_name for shard in self.__shards])
      except Exception as e:
        print(f"Failed to report shards: {e}")
      time.sleep(INTERVAL_SHARD_REPORT)


if __name__ == "__main__":
  with Pyro5.api.Daemon() as daemon:
    uri = daemon.register(DataNodeService(), f"dn-{TOKEN}")
    try:
      ns = Pyro5.api.locate_ns()
      ns.register(f"dn-{TOKEN}", uri)
    except Exception as e:
      print(f"Failed to locate nameserver: {e}")
      exit(1)
    print("DataNode Service started")
    daemon.requestLoop()