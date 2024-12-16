class Response:

    class Status:
        OK = 200
        FILE_ALREADY_EXISTS = 400
        FILE_NOT_FOUND = 404
        NO_DATA_NODES = 501
        DATA_NODES_ARE_BUSY = 502

    class Body:
        def __init__(self, msg: str, data: dict = None):
            self.msg = msg
            self.data = data

        def __getstate__(self):
            return self.__dict__

        def __setstate__(self, state):
            self.__dict__.update(state)

    def __init__(self, status_code: int, body: Body):
        self.status_code = status_code
        self.body = body

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __str__(self):
        return f"Response(status_code={self.status_code}, body=(msg={self.body.msg}, data={self.body.data}))"

    @classmethod
    def from_dict(cls, d: dict) -> 'Response':
        return cls(d['status_code'], cls.Body(d['body']['msg'], d['body']['data']))
