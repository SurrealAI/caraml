import pytest
import numpy as np
from threading import Thread
from caraml.zmq import DataFetcher
from caraml.zmq.communicator import ZmqClient, ZmqServer


from caraml.utils.serializer import pa_deserialize

@pytest.mark.timeout(1.5)
def test_client_server_timeout():
    host = '127.0.0.1'
    port = 7000
    N = 9
    responses = [None for i in range(N)]
    shp = [1000, 1000]

    def server(N):
        server = ZmqServer(host=host, port=port, 
                           serializer='pyarrow',
                           deserializer='pyarrow')
        for i in range(N):
            req = server.recv()
            arr = np.ones(shp) * req
            server.send(arr)

    def client(N):
        def handler(request, response):
            responses[request] = response.data

        def worker_handler(data):
            data = data ** 2
            return data

        requests = [i for i in range(N)]
        fetcher = DataFetcher(handler=handler,
                              remote_host=host,
                              remote_port=port,
                              requests=requests,
                              worker_comm_port=7001,
                              n_workers=3,
                              worker_handler=worker_handler)
        fetcher.start()
        fetcher.join()

    server_thread = Thread(target=server, args=[N])
    client_thread = Thread(target=client, args=[N])

    client_thread.start()
    server_thread.start()
    client_thread.join()
    print('client exited')
    server_thread.join()
    print('server exited')

    for i in range(N):
        assert np.all(responses[i].mean() == np.ones(shp) * i * i)