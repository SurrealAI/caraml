from threading import Thread
import pytest
from caraml.zmq import ZmqProxyThread, ZmqSender, ZmqReceiver, ZmqPusher, ZmqPuller


@pytest.mark.timeout(1)
def test_proxyed_send_repceive():
    host = '127.0.0.1'
    port_frontend = 7000
    port_backend = 7001
    N = 6
    msg_send = list(range(N))
    msg_receive = [None] * len(msg_send)

    def send(N):
        sender = ZmqSender(host=host,
                           port=port_frontend,
                           serializer='pickle')
        for i in range(N):
            sender.send(msg_send[i])

    def receive(N):
        receiver = ZmqReceiver(host=host,
                               port=port_backend,
                               bind=False,
                               deserializer='pickle')
        for i in range(N):
            n = receiver.recv()
            msg_receive[i] = n

    server_thread = Thread(target=send, args=[N])
    client_thread = Thread(target=receive, args=[N])
    in_add = 'tcp://{}:{}'.format(host, port_frontend)
    out_add = 'tcp://{}:{}'.format(host, port_backend)
    proxy_thread = ZmqProxyThread(in_add, out_add, pattern='router-dealer')

    client_thread.start()
    server_thread.start()
    proxy_thread.start()
    client_thread.join()
    server_thread.join()

    assert msg_send == msg_receive


@pytest.mark.timeout(1)
def test_proxyed_pull_push():
    host = '127.0.0.1'
    port_frontend = 7002
    port_backend = 7003
    N = 6
    msg_push = list(range(N))
    msg_pull = [None] * len(msg_push)

    def push(N):
        pusher = ZmqPusher(host=host,
                           port=port_frontend,
                           serializer='pickle')
        for i in range(N):
            pusher.push(msg_push[i])

    def pull(N):
        puller = ZmqPuller(host=host,
                           port=port_backend,
                           bind=False,
                           deserializer='pickle')
        for i in range(N):
            n = puller.pull()
            msg_pull[i] = n

    server_thread = Thread(target=push, args=[N])
    client_thread = Thread(target=pull, args=[N])
    in_add = 'tcp://{}:{}'.format(host, port_frontend)
    out_add = 'tcp://{}:{}'.format(host, port_backend)
    proxy_thread = ZmqProxyThread(in_add, out_add, pattern='pull-push')

    client_thread.start()
    server_thread.start()
    proxy_thread.start()
    client_thread.join()
    server_thread.join()

    assert msg_push == msg_pull
