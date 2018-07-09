import pytest
import zmq
from caraml.zmq import ZmqSocket, ZmqServer, ZmqClient, ZmqPusher, ZmqPuller
from threading import Thread

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
        all_responses = []
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
