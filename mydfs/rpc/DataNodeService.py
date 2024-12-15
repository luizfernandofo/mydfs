import os
import sys
import time
import uuid
import threading
import pika
import Pyro5.api
import psutil
import serpent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from mydfs.utils.get_proxy_by_name import get_proxy_by_name
from mydfs.models.DataNode.FileSystem import FileSystem
from mydfs.utils.shared import *


if False and os.path.exists("data_node_config.txt"):
    with open("data_node_config.txt", "r") as f:
        TOKEN = f.read().strip()
else:
    TOKEN = str(uuid.uuid4())
    # with open('data_node_config.txt', 'w') as f:
    #   f.write(TOKEN)


@Pyro5.api.expose
class DataNodeService:
    def __init__(self):
        print(f"DataNode {TOKEN} started")
        self.TOKEN = TOKEN
        self.__cluster_manager_proxy = get_proxy_by_name("cluster-manager")
        self.__file_system = FileSystem()

        try:
            self.__brokker_connection = pika.BlockingConnection(
                pika.ConnectionParameters(BROKER_URL)
            )
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
        print(f"DataNode {self.TOKEN} stopped")

    def __report_shards_to_cluster(self):
        try:
            get_proxy_by_name("cluster-manager").report_shards(
                self.TOKEN, self.__file_system.get_all_files_names()
            )
        except Exception as e:
            print(f"Failed to report shards: {e}")

    def __get_system_info(self):
        cpu_usage = psutil.cpu_percent(interval=1)
        ram_info = psutil.virtual_memory()
        disk_info = psutil.disk_usage("/")
        return {
            "cpu_usage": cpu_usage,
            "ram_available": ram_info.available,
            "disk_available": disk_info.free,
        }

    def __vitals_thread(self):
        try:
            channel = self.__brokker_connection.channel()
            channel.exchange_declare(
                exchange=VITALS_EXCHANGE_NAME, exchange_type="fanout"
            )
            while self.__keep_running:
                vitals = self.__get_system_info()
                vitals.update({"token": self.TOKEN})
                channel.basic_publish(
                    exchange=VITALS_EXCHANGE_NAME,
                    routing_key="",
                    body=serpent.dumps(vitals),
                )
                time.sleep(INTERVAL_VITALS_REPORT)
        except Exception as e:
            print(f"Failed to send vitals: {e}")
            exit(1)

    def upload_shard(self, shard_name: str, shard_data: bytes):
        self.__file_system.insert_shard(shard_name, shard_data)
        self.__report_shards_to_cluster()


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
