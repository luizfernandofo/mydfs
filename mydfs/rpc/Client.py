import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from mydfs.utils.get_proxy_by_name import get_proxy_by_name
from mydfs.models.Reponse import Response


class Client:
    def __init__(self):
        self.__cluster_manager_proxy = get_proxy_by_name("cluster-manager")

    def get_available_files(self) -> Response:
        return Response.from_dict(self.__cluster_manager_proxy.get_available_files())


if __name__ == "__main__":
    c = Client()
