class Response:

  class Status:
    OK = 200
    FILE_ALREADY_EXISTS = 400
    NO_DATA_NODES = 501
    DATA_NODES_ARE_BUSY = 502

  class Body:
    def __init__(self, msg: str, data: dict = None):
      self.msg = msg
      self.data = data

  def __init__(self, status_code: int, body: Body):
    self.status_code = status_code
    self.body = body