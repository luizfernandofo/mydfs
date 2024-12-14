import os, sys, random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from mydfs.models.DataNode.Shard import Shard
from mydfs.utils.lock_decorator import synchronized

class FileSystem:
    def __init__(self):
        self.__DEFAULT_FOLDER_NAME = f'shards{random.randint(0, 10000)}/'
        if not os.path.exists(self.__DEFAULT_FOLDER_NAME):
            os.mkdir(self.__DEFAULT_FOLDER_NAME)

        if False and os.path.exists('filesystem.txt'):
            pass
        else:
            self.files: list[Shard] = []

    @synchronized
    def __insert_shard(self, shard: Shard):
        try:
            self.files.append(shard)
        except Exception as e:
            print(f"Failed to insert shard: {e}")

    def insert_shard(self, shard_name: str, shard_data: bytes):
        try:
            with open(self.__DEFAULT_FOLDER_NAME + shard_name, 'wb') as f:
                f.write(shard_data)
        except Exception as e:
            print(f"Failed to write shard: {e}")
            
        self.__insert_shard(Shard(shard_name, self.__DEFAULT_FOLDER_NAME + shard_name, len(shard_data)))
       
        
    