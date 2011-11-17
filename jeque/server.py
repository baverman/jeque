import os
import time
import asyncore
import socket
import threading
import logging

from cPickle import dumps, loads

from .queue import Queue
from .utils import sendall, recvall

queue_dict = dict()

def get_queue(queue_id):
    try:
        return queue_dict[queue_id]
    except KeyError:
        pass

    q = queue_dict[queue_id] = Queue()
    return q

class DelayedResult(Exception): pass

class Session(asyncore.dispatcher):
    def __init__(self, session_id, sock):
        asyncore.dispatcher.__init__(self, sock)
        self.result_ready = False
        self.session_id = session_id

    def writable(self):
        return self.result_ready

    def handle_read(self):
        data_len = recvall(self, 10)
        if not data_len:
            return

        data = recvall(self, int(data_len))

        args = loads(data)
        method = args[0]
        args = args[1:]
        logging.getLogger(__name__).debug('%s %s %s', self.session_id, method, args)

        try:
            result = getattr(self, 'do_' + method)(*args)
        except DelayedResult:
            logging.getLogger(__name__).debug('Delayed result')
            pass
        except Exception as e:
            logging.getLogger(__name__).exception('Queue:')
            self.send_result(False, str(e))
        else:
            self.send_result(True, result)

    def handle_write(self):
        self.result_ready = False
        sendall(self, self.result)

    def handle_close(self):
        logging.getLogger(__name__).info('Session %s closed', self.session_id)
        for q in queue_dict.itervalues():
            q.session_done(self.session_id)

        self.close()

    def send_result(self, status, result):
        logging.getLogger(__name__).debug('Result: %s', result)
        self.result = dumps((status, result))
        self.result = str(len(self.result)).ljust(10) + self.result
        self.result_ready = True

    def do_put(self, queue_id, message, message_id, priority, group):
        return get_queue(queue_id).put(message, message_id, priority, group)

    def do_wait_ack(self, queue_id, message_id):
        get_queue(queue_id).wait_for_ack(self.session_id, message_id, self.on_ack)
        raise DelayedResult()

    def on_ack(self, result):
        self.send_result(True, result)

    def do_get(self, queue_id, multi, block):
        queue = get_queue(queue_id)
        msg_id, message = queue.get(self.session_id, multi)
        if not msg_id and block:
            queue.wait_for_message(self.session_id, self.on_message)
            raise DelayedResult()

        return msg_id, message

    def on_message(self, msg_id, message):
        self.send_result(True, (msg_id, message))

    def do_ack(self, queue_id, message_id, result):
        get_queue(queue_id).ack(message_id, result)

    def do_reput(self, queue_id, message_id, delay):
        get_queue(queue_id).reput(message_id, delay)

    def do_dump(self):
        now = time.time()
        result = {}

        for queue_id, queue in queue_dict.iteritems():
            messages, pmessages = result[queue_id] = [], queue.pmessages
            for msg_id, message in queue.all_messages():
                messages.append((msg_id, message.message, message.priority,
                    int(now - message.enterdate), message.get))

        return result

    def do_size(self, queue_id):
        return len(get_queue(queue_id))

    def handle_error(self):
        logging.getLogger(__name__).exception('Ahtung')
        self.handle_close()

class Timer(asyncore.file_dispatcher):
    def handle_read(self):
        self.recv(1)
        for q in queue_dict.itervalues():
            q.check_for_new_messages()

def timer_feeder(fd):
    while True:
        time.sleep(10)
        os.write(fd, '0')

class Server(asyncore.dispatcher):
    def __init__(self, addr):
        asyncore.dispatcher.__init__(self)
        self.addr = addr
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.bind(addr)
        self.listen(10)
        self.session_counter = 0
        logging.getLogger(__name__).info("Queue server started %s", self.addr)

    def handle_accept(self):
        channel, addr = self.accept()
        self.session_counter += 1
        Session(self.session_counter, channel)

    def run(self):
        r, w = os.pipe()
        self.timer = Timer(r)
        t = threading.Thread(target=timer_feeder, args=(w,))
        t.daemon = True
        t.start()
        asyncore.loop()

    def writable(self):
        return False