import pytest
import zmq
import time
from caraml.zmq import ZmqSocket, ZmqServer, ZmqClient, ZmqPusher, ZmqPuller, ZmqPub, ZmqSub
from threading import Thread, Lock
from asyncio import Semaphore

def test_malformed_address():
    with pytest.raises(TypeError, match='bind'):
        c = ZmqSocket(host='tcp//tcp://123.123.123.123', port=1234, socket_mode='PUSH')
    with pytest.raises(ValueError, match='Cannot parse address'):
        c = ZmqSocket(host='tcp//tcp://123.123.123.123', port=1234, socket_mode='PUSH', bind=True)

@pytest.mark.timeout(1)
def test_client_server():
    host = '127.0.0.1'
    port = 7000
    N = 20
    data = [None, None]

    def client(N):
        client = ZmqClient(host=host, port=port, serializer='pickle', deserializer='pickle')
        all_responses = []
        for i in range(N):
            all_responses.append(client.request(i))
        data[1] = all_responses

    def server(N):
        server = ZmqServer(host=host, port=port, serializer='pickle', deserializer='pickle')
        all_requests = []
        for i in range(N):
            n = server.recv()
            all_requests.append(n)
            server.send(n + 1)
        data[0] = all_requests

    server_thread = Thread(target=server, args=[N])
    client_thread = Thread(target=client, args=[N])

    client_thread.start()
    server_thread.start()
    client_thread.join()
    server_thread.join()
    
    assert data[0] == [n for n in range(N)]
    assert data[1] == [n + 1 for n in range(N)]

@pytest.mark.timeout(1)
def test_client_server_eventloop():
    host = '127.0.0.1'
    port = 7000
    N = 20
    data = [None, None]

    server = ZmqServer(host=host, port=port, serializer='pickle', deserializer='pickle')
    all_requests = []

    def handler(x):
        if x == N - 1:
            server.stop()
        all_requests.append(x)
        return x + 1

    server_thread = server.start_event_loop(handler, blocking=False)

    client = ZmqClient(host=host, port=port, serializer='pickle', deserializer='pickle')
    all_responses = []
    for i in range(N):
        all_responses.append(client.request(i))
    data[1] = all_responses

    server_thread.join()
    
    assert all_requests == [n for n in range(N)]
    assert all_responses == [n + 1 for n in range(N)]

@pytest.mark.timeout(1)
def test_pull_push():
    host = '127.0.0.1'
    port = 7000
    N = 20
    all_requests = []

    def push(N):
        pusher = ZmqPusher(host=host, port=port, serializer='pickle')
        for i in range(N):
            pusher.push(i)

    def pull(N):
        puller = ZmqPuller(host=host, port=port, deserializer='pickle')
        for i in range(N):
            n = puller.pull()
            all_requests.append(n)

    puller_thread = Thread(target=pull, args=[N])
    pusher_thread = Thread(target=push, args=[N])

    puller_thread.start()
    pusher_thread.start()
    puller_thread.join()
    pusher_thread.join()
    
    assert all_requests == [n for n in range(N)]

@pytest.mark.timeout(1)
def test_pub_sub():
    host = '127.0.0.1'
    port = 7000
    N = 10
    responses = {}
    lock = Lock()
    clients_online = {'topic-1': False, 'topic-2': False}

    def pub(N):
        socket = ZmqPub(host=host, port=port, hwm=1000, serializer='pickle')
        while True:
            socket.pub('topic-1', 'connect')
            socket.pub('topic-2', 'connect')
            with lock:
                all_connected = True
                for val in clients_online.values():
                    all_connected = all_connected and val
                time.sleep(0.01)
                if all_connected:
                    break
        for i in range(N):
            socket.pub('topic-1', i + 1)
            socket.pub('topic-2', i + 2)

    def sub(N, topic):
        with lock:
            responses[topic] = []
        socket = ZmqSub(host=host, port=port, topic=topic, hwm=1000, deserializer='pickle')
        i = 0
        while i < N:
            data = socket.recv()
            if data == 'connect':
                with lock:
                    clients_online[topic] = True
            else:
                with lock:
                    responses[topic].append(data)
                i += 1

    pub_thread = Thread(target=pub, args=[N])
    sub_thread_1 = Thread(target=sub, args=[N, 'topic-1'])
    sub_thread_2 = Thread(target=sub, args=[N, 'topic-2'])
    threads = [sub_thread_1, sub_thread_2, pub_thread]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert responses == {
        'topic-1': [i + 1 for i in range(N)],
        'topic-2': [i + 2 for i in range(N)]
    }