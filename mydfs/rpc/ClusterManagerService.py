import sys
import os
import threading
import Pyro5.api
import pika
import serpent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.ClusterManager.FileSystem import FileSystem
from mydfs.utils.lock_decorator import synchronized

BROKER_URL = "localhost"
VITALS_EXCHANGE_NAME = "vitals"


@Pyro5.api.expose
class ClusterManagerService:
  def __init__(self):
    self.__file_system = FileSystem()

    try:
      self.__brokker_connection = pika.BlockingConnection(pika.ConnectionParameters(BROKER_URL))
    except Exception as e:
      print(f"Failed to connect to broker: {e}")
      exit(1)

    try:
      self.__keep_running = True
      self.__vitals_thread = threading.Thread(target=self.__vitals_thread)
      self.__vitals_thread.start()
    except Exception as e:
      print(f"Failed to start vitals thread: {e}")

  def __del__(self):
    self.__keep_running = False
    if self.__vitals_thread.is_alive():
      self.__vitals_thread.join()
    if self.__brokker_connection.is_open:
      self.__brokker_connection.close()

  def __vitals_thread(self):
    try:
      brokker_channel = self.__brokker_connection.channel()
      brokker_channel.exchange_declare(exchange=VITALS_EXCHANGE_NAME, exchange_type='direct')
      queue = brokker_channel.queue_declare(queue='', exclusive=True)
      brokker_channel.queue_bind(exchange=VITALS_EXCHANGE_NAME, queue=queue.method.queue, routing_key='')
      brokker_channel.basic_consume(queue=queue.method.queue, on_message_callback=self.__recevei_vitals_callback, auto_ack=True)
      brokker_channel.start_consuming()
    except Exception as e:
      print(f"Failed to declare exchange or consume: {e}")

  def __recevei_vitals_callback(self, ch, method, properties, body):
    vitals = serpent.loads(body)
    print(vitals)

  def report_shards(self, token: str, shard_name_list: list[str]):
    shard_tuples = [(shard.split('-')[0], int(shard.split('-')[1])) for shard in shard_name_list]
    for file_name, shard_index in shard_tuples:
      self.__file_system[file_name][shard_index].add_data_node_owner_if_not_exists(token)

  def get_file_shards_owners(self, file_name: str):
    return [shard_owners for shard_owners in self.__file_system[file_name].shards.data_node_owners]

  def start_upload(self, file_name: str, file_size: int):
    pass

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
