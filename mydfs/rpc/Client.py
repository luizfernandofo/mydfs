import sys, os, struct
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from mydfs.utils.get_proxy_by_name import get_proxy_by_name
from mydfs.models.Reponse import Response
from mydfs.utils.shared import *
import threading

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
            print("Nome de arquivo inválido. O arquivo não pode conter o caractere '-'")
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

    def download_file(self, file_name: str, download_method: str = "parallel"):
        res: Response = Response.from_dict(self.__cluster_manager_proxy.download_file(file_name))
        if res.status_code != Response.Status.OK:
            print(res.body.msg)
            return
        
        shards_owners_list = res.body.data["shards_owners"]
        file_size = res.body.data["file_size"]

        if download_method == "serial":
            self.__serial_download(file_name, shards_owners_list)
        elif download_method == "parallel":
            self.__threaded_download(file_name, file_size, shards_owners_list)

    def __serial_download(self, file_name: str, shards_owners_list: list[list[str]]):
        file_path = os.path.join(self.__files_folder_path + "/downloads/", file_name)
        time_start = time.time_ns()
        with open(file_path, "wb") as f:
            for i in range(0, len(shards_owners_list)):
                shard_name = f"{file_name}-{i}"
                data_node_proxy = get_proxy_by_name(f"dn-{shards_owners_list[i][0]}")
                data_node_proxy._pyroSerializer = "marshal"
                shard_data = data_node_proxy.download_shard(shard_name)
                data_node_proxy._pyroRelease()
                f.write(shard_data)
                print(f"{(i/len(shards_owners_list)) * 100:.2f}%")
        time_end = time.time_ns()
        total_time = (time_end - time_start) / 1e9
        file_size = (os.path.getsize(file_path) / 1e6)
        mbs = file_size / total_time
        print(f"Download de arquivo {file_name} ({int(file_size)} MB) concluído.")
        print(f"Taxa de transferência: {mbs:.2f} MB/s.")
        print(f"Tempo de download: {total_time:.2f} segundos.")        

    def __threaded_download(self, file_name: str, file_size: int, shards_owners_list: list[list[str]]):
        time_start = time.time_ns()
        file_path = os.path.join(self.__files_folder_path + "/downloads/", file_name)
        num_threads = min(len(shards_owners_list), NUM_THREADS_DOWNLOAD)  # Number of threads to use
        shards_per_thread = len(shards_owners_list) // num_threads

        def download_shards(thread_id: int):
            start_index = thread_id * shards_per_thread
            end_index = start_index + shards_per_thread if thread_id != num_threads - 1 else len(shards_owners_list)
            with open(file_path, "r+b") as f:
                for i in range(start_index, end_index):
                    shard_name = f"{file_name}-{i}"
                    data_node_proxy = get_proxy_by_name(f"dn-{shards_owners_list[i][0]}")
                    data_node_proxy._pyroSerializer = "marshal"
                    shard_data = data_node_proxy.download_shard(shard_name)
                    data_node_proxy._pyroRelease()
                    f.seek(i * SHARD_SIZE)
                    f.write(shard_data)
                    print(f"Thread {thread_id}: {(i/len(shards_owners_list)) * 100:.2f}%")

        # Create the file and set its size
        with open(file_path, "wb") as f:
            f.truncate(file_size)

        threads = []
        for thread_id in range(num_threads):
            thread = threading.Thread(target=download_shards, args=(thread_id,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        time_end = time.time_ns()
        total_time = (time_end - time_start) / 1e9
        file_size = (os.path.getsize(file_path) / 1e6)
        mbs = file_size / total_time
        print(f"Download de arquivo {file_name} ({int(file_size)} MB) de modo PARALELO concluído.")
        print(f"Taxa de transferência: {mbs:.2f} MB/s.")
        print(f"Tempo de download: {total_time:.2f} segundos.")
        

if __name__ == "__main__":
    c = Client()
    #c.upload_file("img.tif")
    c.download_file("ideaIU2024.3.1.tar.gz")