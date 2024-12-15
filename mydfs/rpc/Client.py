import sys, os, struct
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from mydfs.utils.get_proxy_by_name import get_proxy_by_name
from mydfs.models.Reponse import Response
from mydfs.utils.shared import *

class Client:
    def __init__(self):
        self.__cluster_manager_proxy = get_proxy_by_name("cluster-manager")
        self.__files_folder_path = os.path.join(os.getcwd(), "files")
        if not os.path.exists(self.__files_folder_path):
            os.makedirs(self.__files_folder_path)

    def get_available_files(self) -> Response:
        return Response.from_dict(self.__cluster_manager_proxy.get_available_files())

    def __start_upload(self, file_name: str, file_size: int) -> Response:
        return Response.from_dict(self.__cluster_manager_proxy.start_upload(file_name, file_size))

    def upload_file(self, file_name: str):
        if file_name.find("-") != -1:
            print("Nome de arquivo inválido")
            return
        
        file_path = os.path.join(self.__files_folder_path, file_name)
        if not os.path.isfile(file_path):
            print(f"File {file_path} does not exist.")
            return

        res = self.__start_upload(file_name, os.path.getsize(file_path))
        file_size = os.path.getsize(file_path)
        data_nodes_per_shard = res.body.data["data_nodes_per_shard"]

        time_start = time.time_ns()
        with open(file_path, "rb") as f:
            for i in range(0, len(data_nodes_per_shard)):
                shard_name = f"{file_name}-{i}"
                shard_data = f.read(SHARD_SIZE)
                if not shard_data:
                    break
                data_node_proxy = get_proxy_by_name(f"dn-{data_nodes_per_shard[i]}")
                data_node_proxy._pyroSerializer = "marshal"
                data_node_proxy.upload_shard(shard_name, shard_data)
                data_node_proxy._pyroRelease()
                print(f"{(i/len(data_nodes_per_shard)) * 100:.2f}%")
        time_end = time.time_ns()
        total_time = (time_end - time_start) / 1e9
        file_size = (file_size / 1e6) #MB
        mbs = file_size / total_time
        print(f"Upload de arquivo {file_name} ({int(file_size)} MB) concluído.")
        print(f"Taxa de transferência: {mbs:.2f} MB/s.")
        print(f"Tempo de upload: {total_time:.2f} segundos.")

if __name__ == "__main__":
    c = Client()
    c.upload_file("4arquivo_grande.gz")
