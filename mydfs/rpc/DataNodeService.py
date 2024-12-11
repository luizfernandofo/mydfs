import rpyc


class DataNodeService(rpyc.Service):
    def __init__(self):
        super().__init__()

    def on_connect(self, conn):
        print("Connection established")

    def on_disconnect(self, conn):
        print("Connection closed")


if __name__ == "__main__":
    # connect to the cluster manager
    token = "1234"
    conn = rpyc.connect("localhost", 25565)
    conn.root.register_data_node(token)
    conn.root.report_shards(token)