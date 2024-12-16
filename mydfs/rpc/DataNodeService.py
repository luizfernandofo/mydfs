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
        self.__keep_running = True
        self.__file_system = FileSystem()

        try:
            self.__vitals_t = threading.Thread(target=self.__vitals_thread)
            self.__vitals_t.start()
        except Exception as e:
            print(f"Failed to start vitals thread: {e}")
            exit(1)

        try:
            self.__replica_t = threading.Thread(target=self.__replica_thread)
            self.__replica_t.start()
        except Exception as e:
            print(f"Failed to start replica thread: {e}")
            exit(1)

    def __del__(self):
        self.__keep_running = False
        if self.__vitals_thread.is_alive():
            self.__vitals_thread.join()
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
            brokker_connection = pika.BlockingConnection(
                pika.ConnectionParameters(BROKER_URL)
            )
            channel = brokker_connection.channel()
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

    def __replica_thread(self):
        def __handle_replica_request_callback(ch, method, properties, body):
            replica = serpent.loads(body)
            shard_name = replica["shard_name"]
            shard_owner = replica["shard_owner"]

            if self.__file_system.get_shard_by_name(shard_name) is not None:
                print(properties.headers)
                if (properties.headers is not None):
                    delivery_count = properties.headers["x-delivery-count"] if "x-delivery-count" in properties.headers else 0
                    print(f"Delivery count {delivery_count} for shard {shard_name}")
                    if delivery_count > 0:
                        time.sleep(min((delivery_count * 2), 30))
                ch.basic_nack(delivery_tag=method.delivery_tag)
                return

            data_node_proxy = get_proxy_by_name(f"dn-{shard_owner}")
            data_node_proxy._pyroSerializer = "marshal"
            try:                
                shard_data = data_node_proxy.download_shard(shard_name)
                data_node_proxy._pyroRelease()
                self.upload_shard(shard_name, shard_data)
                print(f"Shard {shard_name} downloaded and uploaded")
                get_proxy_by_name("cluster-manager").decrease_replications_requested(shard_name)
                print(f"Replication requested decreased for {shard_name}")
            except Exception as e:
                print(f"Failed to download shard: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        try:
            brokker_connection = pika.BlockingConnection(
                pika.ConnectionParameters(BROKER_URL)
            )
            brokker_channel = brokker_connection.channel()
            brokker_channel.exchange_declare(
                exchange=REPLICA_EXCHANGE_NAME, exchange_type="direct"
            )
            brokker_channel.queue_declare(queue=REPLICA_QUEUE_NAME, durable=True, arguments={"x-queue-type": "quorum"})
            brokker_channel.queue_bind(
                exchange=REPLICA_EXCHANGE_NAME, queue=REPLICA_QUEUE_NAME, routing_key=""
            )
        except Exception as e:
            print(f"Failed to declare exchange or bind queue: {e}")

        try:
            brokker_channel.basic_consume(
                queue=REPLICA_QUEUE_NAME,
                on_message_callback=__handle_replica_request_callback,
            )
            brokker_channel.start_consuming()
        except Exception as e:
            print(f"Failed to start consuming: {e}")

    # ============== Exposed methods ==============

    def upload_shard(self, shard_name: str, shard_data: bytes):
        self.__file_system.insert_shard(shard_name, shard_data)
        self.__report_shards_to_cluster()

    def download_shard(self, shard_name: str) -> bytes:
        shard_path = self.__file_system.get_shard_by_name(shard_name).file_path
        with open(shard_path, "rb") as f:
            return f.read()


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
