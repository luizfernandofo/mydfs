import sys
import os
from typing import List
import rpyc

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.File import File
from mydfs.models.FileSystem import FileSystem
from mydfs.utils.lock_decorator import synchronized

class ClusterManagerService(rpyc.Service):
  file_system = FileSystem()
  data_nodes = {}

  @synchronized
  def insert_data_node(self, token: str, ip: str, port: int):
    ClusterManagerService.data_nodes[token] = (ip, port)

  # ====================== RPC ======================

  def on_connect(self, conn):
    self.conn = conn
    print("Connection established")

  def on_disconnect(self, conn):
    print("Connection closed")

  def exposed_register_data_node(self, token: str):
    caller_ip = self.conn._channel.stream.sock.getpeername()[0]
    caller_port = self.conn._channel.stream.sock.getpeername()[1]
    self.insert_data_node(token, caller_ip, caller_port)
    print(f"DataNode registered with token {token} at {caller_ip}:{caller_port}")
    
  def exposed_report_shards(self, token: str, file: List[File]):
    print(f"Report received from DataNode at {token} at {self.data_nodes[token]}")


if __name__ == "__main__":
  from rpyc.utils.server import ThreadedServer
  t = ThreadedServer(ClusterManagerService, port=25565)
  print("Cluster Manager Service started")
  t.start()