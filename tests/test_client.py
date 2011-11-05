import sys
import time
from subprocess import Popen
from jeque import Client

class Future(object):
    def __init__(self, func, *args, **kwargs):
        from threading import Thread

        def do():
            self.result = func(*args, **kwargs)

        self.t = Thread(target=do)
        self.t.daemon = True
        self.t.start()

    def get(self, timeout=None):
        self.t.join(timeout)
        if self.t.is_alive():
            raise Exception('Future is not finished yet')

        return self.result

def pytest_funcarg__server(request):
    srv = Popen('./bin/jeque /tmp/sock', shell=True)
    time.sleep(1)
    def close():
        srv.terminate()
        srv.wait()

    request.addfinalizer(close)
    return srv

def _test_speed(server):
    cl = Client('/tmp/sock')

    t = time.time()
    i = 0
    while time.time() - t < 3:
        cl.put('test', 'aaa')
        i += 1

    cl.close()
    print i / 3.0, 'mp in second'
    assert False

def test_put_get_and_ack(server):
    cl1 = Client('/tmp/sock')

    cl1.put('test', 'wow1', '1', 100)

    id, message = cl1.get('test', block=False)
    assert id == '1'
    assert message == 'wow1'

    cl1.put('test', 'wow2', '2', 500)

    time.sleep(1.1)

    id, message = cl1.get('test', block=False)
    assert id == '2'
    assert message == 'wow2'

    cl2 = Client('/tmp/sock')
    id, message = cl2.get('test', block=False)
    assert id == '1'
    assert message == 'wow1'

    cl3 = Client('/tmp/sock')
    id, message = cl3.get('test', block=False)
    assert id is None

    cl2.close()

    cl1.get_queue_size('test') == 2

    id, message = cl3.get('test', block=False)
    assert id == '1'
    assert message == 'wow1'

    cl1.ack('test', '2')
    id, message = cl1.get('test', block=False)
    assert id is None

def test_blocked_get(server):
    cl1 = Client('/tmp/sock')
    cl2 = Client('/tmp/sock')

    f = Future(cl1.get, 'test')
    cl2.put('test', 'msg', 'id')
    result = f.get(1)
    assert result == ('id', 'msg')

    result = cl2.get('test', block=False)
    assert result == (None, None)
    f = Future(cl2.get, 'test')
    cl1.reput('test', 'id')
    result = f.get(1)
    assert result == ('id', 'msg')

    result = cl1.get('test', block=False)
    assert result == (None, None)
    f = Future(cl1.get, 'test')
    cl2.close()
    result = f.get(1)
    assert result == ('id', 'msg')

def test_ack_wait(server):
    cl1 = Client('/tmp/sock')
    cl2 = Client('/tmp/sock')

    msg_id = cl1.put('test', 'message')
    f = Future(cl1.wait_ack, 'test', msg_id)
    _ = cl2.get('test', block=False)
    cl2.ack('test', msg_id, 10)
    result = f.get(1)
    assert result == 10