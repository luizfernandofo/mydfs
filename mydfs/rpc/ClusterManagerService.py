from math import ceil
import sys
import os
import threading
import time
import Pyro5.api
import pika
import serpent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from mydfs.models.ClusterManager.DataNodesConnected import DataNodesConnected
from mydfs.models.ClusterManager.FileSystem import FileSystem
from mydfs.models.ClusterManager.File import File
from mydfs.utils.lock_decorator import synchronized
from mydfs.models.Reponse import Response
from mydfs.utils.shared import *


@Pyro5.api.expose
class ClusterManagerService:
    def __init__(self):
        self.__file_system = FileSystem()
        self.__data_nodes_connected = DataNodesConnected()
        self.__keep_running = True

        try:
            self.__vitals_t = threading.Thread(target=self.__vitals_thread)
            self.__vitals_t.start()
        except Exception as e:
            print(f"Failed to start vitals thread: {e}")

        try:
            self.__integrity_thread = threading.Thread(target=self.__integrity_routine_thread)
            self.__integrity_thread.start()
        except Exception as e:
            print(f"Failed to start integrity routine thread: {e}")

    def __del__(self):
        if self.__vitals_thread.is_alive():
            self.__vitals_thread.join()
        

    def __vitals_thread(self):
        def __receive_vitals_callback(ch, method, properties, body):
            vitals = serpent.loads(body)
            dead_data_nodes = self.__data_nodes_connected.update_data_node_vitals(vitals["token"], vitals)
            self.__file_system.remove_dead_shard_owners(dead_data_nodes)
            
        try:
            brokker_connection = pika.BlockingConnection(
                pika.ConnectionParameters(BROKER_URL)
            )
            brokker_channel = brokker_connection.channel()
            brokker_channel.exchange_declare(
                exchange=VITALS_EXCHANGE_NAME, exchange_type="fanout"
            )
            queue = brokker_channel.queue_declare(queue="", exclusive=True)
            brokker_channel.queue_bind(
                exchange=VITALS_EXCHANGE_NAME, queue=queue.method.queue, routing_key=""
            )
        except Exception as e:
            print(f"Failed to declare exchange or bind queue: {e}")
        
        try:
            brokker_channel.basic_consume(
                queue=queue.method.queue,
                on_message_callback=__receive_vitals_callback,
                auto_ack=True,
            )
            brokker_channel.start_consuming()
        except Exception as e:
            print(f"Failed to start consuming: {e}")
        
    def __integrity_routine_thread(self):
        try:
            brokker_connection = pika.BlockingConnection(
                pika.ConnectionParameters(BROKER_URL)
            )
            channel = brokker_connection.channel()
            channel.exchange_declare(
                exchange=REPLICA_EXCHANGE_NAME, exchange_type="direct"
            )

            while self.__keep_running:
                if len(self.__data_nodes_connected.get_data_nodes()) == 1:
                    time.sleep(INTERVAL_ROUTINE_INTEGRITY)
                    continue
                
                _replication_factor = min(REPLICATION_FACTOR, len(self.__data_nodes_connected.get_data_nodes()))
                for file_name in self.__file_system.files:
                    file: File = self.__file_system.files[file_name]
                    if not file.upload_finished:
                        continue
                    shards = file.get_shards()
                    for shard in shards:
                        if (shard.replications_requested + shard.get_replication_factor()) < _replication_factor:
                            for i in range(_replication_factor - shard.get_replication_factor()):
                                body = {"shard_name": f"{file_name}-{shards.index(shard)}", "shard_owner": shard.data_node_owners[i % len(shard.data_node_owners)]}
                                print(f"Replication requested for {file_name}-{shards.index(shard)} with shard owner {shard.data_node_owners[i % len(shard.data_node_owners)]}")
                                channel.basic_publish(
                                    exchange=REPLICA_EXCHANGE_NAME,
                                    routing_key="",
                                    body=serpent.dumps(body),
                                    properties=pika.BasicProperties(
                                        delivery_mode = pika.DeliveryMode.Persistent
                                    )
                                )
                                shard.increase_replications_requested()                                        
                        print(f"Replicas requested: {shard.replications_requested}")
                
                time.sleep(INTERVAL_ROUTINE_INTEGRITY)
        except Exception as e:
            print(f"Failed to execute integrity routine: {e}")
            exit(1)
            

    # ============== Exposed methods ==============

    def report_shards(self, token: str, shard_name_list: list[str]):
        shard_tuples = [
            (shard.split("-")[0], int(shard.split("-")[1])) for shard in shard_name_list
        ]
        for file_name, shard_index in shard_tuples:
            self.__file_system.update_shard_owners_by_file_name(
                file_name, shard_index, token
            )     
    
    def decrease_replications_requested(self, shard_name: str):
        file_name = shard_name.split("-")[0]
        shard_index = int(shard_name.split("-")[1])
        self.__file_system.decrease_replications_requested(file_name, shard_index)

    def download_file(self, file_name: str) -> Response:
        if not self.__file_system.file_exists(file_name):
            return Response(
                Response.Status.FILE_NOT_FOUND,
                Response.Body("File not found"),
            )

        shards_owners = self.__file_system.get_shards_owners_by_file_name(file_name)
        load_balanced_shard_owners = []
        for s_o in shards_owners:
            if len(s_o) == 1:
                load_balanced_shard_owners.append([s_o[0]])
            else:
                tmp = []                
                for owner in s_o:
                    if len(tmp) == 2:
                        break
                    if self.__data_nodes_connected.data_node_isnt_stressed(owner):
                        tmp.append(owner)
                load_balanced_shard_owners.append(tmp)
        
        for shard in load_balanced_shard_owners:
            if len(shard) == 0:
                return Response(
                    Response.Status.DATA_NODES_ARE_BUSY,
                    Response.Body("Data nodes are busy right now."),
                )
            
        return Response(
            Response.Status.OK,
            Response.Body("Data nodes for download", {"shards_owners": load_balanced_shard_owners, "file_size": self.__file_system.get_file_size(file_name)}),
        )            

    def start_upload(self, file_name: str, file_size: int) -> Response:
        if file_name in self.__file_system.files:
            return Response(
                Response.Status.FILE_ALREADY_EXISTS,
                Response.Body("File already exists"),
            )

        if not self.__data_nodes_connected.any_data_node_connected():
            return Response(
                Response.Status.NO_DATA_NODES, Response.Body("No data nodes connected")
            )

        shard_count = ceil(file_size / SHARD_SIZE)
        suitable_data_nodes = (
            self.__data_nodes_connected.get_suitable_data_nodes_for_upload()
        )
        if len(suitable_data_nodes) == 0:
            return Response(
                Response.Status.DATA_NODES_ARE_BUSY,
                Response.Body("The cluster is busy right now."),
            )

        data_nodes_per_shard = []
        if len(suitable_data_nodes) == 1:
            if self.__data_nodes_connected.get_data_nodes()[suitable_data_nodes[0]].can_store_n_shards(
                shard_count
            ):
                data_nodes_per_shard = [
                    suitable_data_nodes[0] for _ in range(shard_count)
                ]
        elif len(suitable_data_nodes) > 1:
            for i in range(shard_count):
                data_nodes_per_shard.append(
                    suitable_data_nodes[i % len(suitable_data_nodes)]
                )

        self.__file_system.create_file(file_name, file_size)
        return Response(
            Response.Status.OK,
            Response.Body(
                "Data nodes for upload", {"data_nodes_per_shard": data_nodes_per_shard}
            ),
        )

    def get_available_files(self) -> Response:
        available_files = [
            file_name
            for file_name in self.__file_system.files
            if self.__file_system.files[file_name].upload_finished
        ]
        return Response(
            Response.Status.OK,
            Response.Body("Available files", {"files": available_files}),
        )

    def print_file_system(self) -> str:
        return str(self.__file_system)

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
