import time
import socket

def recvall(sock, size):
    result = ''
    while size > 0:
        try:
            data = sock.recv(min(size, 4096))
        except socket.error as e:
            if e.errno == socket.errno.EAGAIN:
                time.sleep(0.1)
                continue

        if not data:
            return None

        size -= len(data)
        result += data

    return result

def sendall(sock, data):
    while data:
        try:
            sent = sock.send(data)
        except socket.error as e:
            if e.errno == socket.errno.EAGAIN:
                time.sleep(0.1)
                continue

        data = data[sent:]