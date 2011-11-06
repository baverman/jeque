import socket
import time

from cPickle import dumps, loads

class Client(object):
    def __init__(self, socket_path):
        self.socket = None

        if socket_path.find(':') >= 0:
            [host, port] = socket_path.split(':')

            def connect():
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((host, int(port)))

        else:
            def connect():
                self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.socket.connect(socket_path)

        self.connect = connect

    def init_connection(self):
        if not self.socket:
            self.connect()

    def call(self, method, *args):
        self.init_connection()

        data = dumps((method,) + args)
        data = str(len(data)).ljust(10) + data
        self.socket.sendall(data)

        data_len = int(self.socket.recv(10))
        is_ok, result = loads(self.socket.recv(data_len))

        if is_ok:
            return result
        else:
            raise Exception(result)

    def put(self, queue_id, message, message_id=None, priority=None, group=None):
        return self.call('put', queue_id, message, message_id, priority, group)

    def wait_ack(self, queue_id, message_id):
        return self.call('wait_ack', queue_id, message_id)

    def get(self, queue_id, multi=False, block=True):
        return self.call('get', queue_id, multi, block)

    def ack(self, queue_id, message_id, result=None):
        return self.call('ack', queue_id, message_id, result)

    def reput(self, queue_id, message_id, delay=0):
        return self.call('reput', queue_id, message_id, delay)

    def close(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.socket = None

    def dump(self):
        return self.call('dump')

    def get_queue_size(self, queue_id):
        return self.call('size', queue_id)