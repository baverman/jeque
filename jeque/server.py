import asyncore
import socket
from cPickle import dumps, loads

class Task(asyncore.dispatcher):
    def __init__(self, sock):
        asyncore.dispatcher.__init__(self, sock)
        self.result_ready = False

    def writable(self):
        return self.result_ready

    def handle_read(self):
        data_len = self.recv(10)
        if not data_len:
            self.close()
            return

        data = self.recv(int(data_len))
        print 'data recieved', loads(data)
        self.send_result('ok')

    def handle_write(self):
        self.result_ready = False
        self.sendall(self.result)

    def handle_close(self):
        print 'connection closed'

    def send_result(self, result):
        self.result = dumps((True, result))
        self.result = str(len(self.result)).ljust(10) + self.result
        self.result_ready = True

class Server(asyncore.dispatcher):
    def __init__(self, addr):
        asyncore.dispatcher.__init__(self)
        self.addr = addr
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.bind(addr)
        self.listen(5)
        print "Queue server started", self.addr

    def handle_accept(self):
        channel, addr = self.accept()
        Task(channel)

    def run(self):
        asyncore.loop()