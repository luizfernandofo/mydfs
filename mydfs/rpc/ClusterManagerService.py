from math import ceil
import sys
import os
import threading
import Pyro5.api
import pika
import serpent

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from mydfs.models.ClusterManager.DataNodesConnected import DataNodesConnected
from mydfs.models.ClusterManager.FileSystem import FileSystem
from mydfs.utils.lock_decorator import synchronized
from mydfs.models.Reponse import Response
from mydfs.utils.shared import *


@Pyro5.api.expose
class ClusterManagerService:
    def __init__(self):
        self.__file_system = FileSystem()
        self.__data_nodes_connected = DataNodesConnected()

        try:
            self.__brokker_connection = pika.BlockingConnection(
                pika.ConnectionParameters(BROKER_URL)
            )
        except Exception as e:
            print(f"Failed to connect to broker: {e}")
            exit(1)

        try:
            self.__vitals_thread = threading.Thread(target=self.__vitals_thread)
            self.__vitals_thread.start()
        except Exception as e:
            print(f"Failed to start vitals thread: {e}")

    def __del__(self):
        if self.__vitals_thread.is_alive():
            self.__vitals_thread.join()
        if self.__brokker_connection.is_open:
            self.__brokker_connection.close()

    def __vitals_thread(self):
        def __recevei_vitals_callback(ch, method, properties, body):
            vitals = serpent.loads(body)
            self.__data_nodes_connected.update_data_node_vitals(vitals["token"], vitals)

        try:
            brokker_channel = self.__brokker_connection.channel()
            brokker_channel.exchange_declare(
                exchange=VITALS_EXCHANGE_NAME, exchange_type="direct"
            )
            queue = brokker_channel.queue_declare(queue="", exclusive=True)
            brokker_channel.queue_bind(
                exchange=VITALS_EXCHANGE_NAME, queue=queue.method.queue, routing_key=""
            )
            brokker_channel.basic_consume(
                queue=queue.method.queue,
                on_message_callback=__recevei_vitals_callback,
                auto_ack=True,
            )
            brokker_channel.start_consuming()
        except Exception as e:
            print(f"Failed to declare exchange or consume: {e}")

    # ============== Exposed methods ==============

    def report_shards(self, token: str, shard_name_list: list[str]):
        shard_tuples = [
            (shard.split("-")[0], int(shard.split("-")[1])) for shard in shard_name_list
        ]
        for file_name, shard_index in shard_tuples:
            self.__file_system[file_name][
                shard_index
            ].add_data_node_owner_if_not_exists(token)

    def get_file_shards_owners(self, file_name: str):
        return [
            shard_owners
            for shard_owners in self.__file_system[file_name].shards.data_node_owners
        ]

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
            if self.__data_nodes_connected[suitable_data_nodes[0]].can_store_n_shards(
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

        self.__file_system.__create_file(file_name, file_size)
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
