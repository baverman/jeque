from jeque import Client

def test_simple():
    cl = Client('/tmp/sock')

    print cl.call('aaa', 10, True)
    print cl.call('bbb', 40, True)
    print cl.call('aaa', 20, False)
    cl.close()

    assert False