"""
    This file implements the necessary classes for data fetcher class
    DataFetcher forks several processes that request data and transfer them
    through memory mapped files
"""
from threading import Thread
from multiprocessing import Process
from caraml.zmq.communicator import ZmqClient, ZmqServer
from caraml.utils.serializer import get_serializer, get_deserializer
from caraml.inmemory import inmem_serialize, inmem_deserialize

_CARAML_TERMINATE_FETCHER = '_CARAML_TERMINATE_FETCHER'


def _get_new_task_message():
    return {
        'type': 'new-task',
    }


def _get_response_message(request, data):
    return {
        'type': 'response',
        'request': request,
        'data': data,
    }


class DataFetcherWorker(Process):
    def __init__(self, master_port, num_threads=1,
                 remote_host=None, remote_port=None,
                 handler=None, serializer=None, deserializer=None,
                 ):
        """
        Override this to setup states in the main process
        """
        Process.__init__(self)
        self.master_port = master_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.handler = handler
        self.serializer = get_serializer(serializer)
        self.deserializer = get_deserializer(deserializer)
        self.num_threads = num_threads

    def run(self):
        threads = []
        for i in range(self.num_threads):
            thread = Thread(target=self.run_thread)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def run_thread(self):
        # You must initilize the transmission channel AFTER you fork off
        remote = ZmqClient(host=self.remote_host, port=self.remote_port,
                           serializer=self.serializer,
                           deserializer=self.deserializer)
        master = ZmqClient(host='127.0.0.1',
                           port=self.master_port,
                           serializer='pickle',
                           deserializer='pickle')
        while True:
            task = master.request(_get_new_task_message())
            if task == _CARAML_TERMINATE_FETCHER:
                return
            else:
                response = remote.request(task)
                if self.handler is not None:
                    response = self.handler(response)
                name = inmem_serialize(response)
                master.request(_get_response_message(task, name))


class DataFetcher(Thread):
    def __init__(self, handler,
                 remote_host,
                 remote_port,
                 requests,
                 worker_comm_port,
                 remote_serializer='pyarrow',
                 remote_deserialzer='pyarrow',
                 n_workers=2,
                 threads_per_worker=2,
                 worker_handler=None,
                 ):
        """
            TODO: automatically find a port
            TODO: drain and exit

            Forks @n_worker processes, send @requests to
            remote_host:remote_port and process responses
            with handler

        Args:
            handler: function(request, reponse) what to do with response in the
                main process
            remote_host, remote_port: host/port for workers to connect to
            requests: A generator of requests to send to remote
            worker_comm_port: local port for communication with workers
            remote_serializer: how to serialize requests to remote
            remote_deserializer: how to deserialize response from remote
            n_workers: number of processes to fetch data
            worker_handler: how to process data on worker processes
                (i.e. compute intensive tasks that still return
                pyarrow serializable data)
        """
        Thread.__init__(self)
        self.handler = handler
        self.n_workers = n_workers
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.remote_serializer = remote_serializer
        self.remote_deserialzer = remote_deserialzer
        self.requests_iter = iter(requests)

        self.worker_comm_port = worker_comm_port
        self.n_workers = n_workers
        self.threads_per_worker = threads_per_worker
        self.worker_handler = worker_handler

    def run(self):
        # Use receiver here to rate-limit the workers, using pull-push causes
        # a lot of messages to be held in system socket cache
        # https://stackoverflow.com/questions/22613737/how-could-i-set-hwm-in-the-push-pull-pattern-of-zmq
        self.server = ZmqServer(host='127.0.0.1',
                                port=self.worker_comm_port,
                                serializer='pickle',
                                deserializer='pickle')
        self.workers = []

        self.active_workers = 0
        for i in range(self.n_workers):
            worker = DataFetcherWorker(master_port=self.worker_comm_port,
                                       num_threads=self.threads_per_worker,
                                       remote_host=self.remote_host,
                                       remote_port=self.remote_port,
                                       handler=self.worker_handler,
                                       serializer=self.remote_serializer,
                                       deserializer=self.remote_deserialzer,
                                       )
            worker.start()
            self.active_workers += self.threads_per_worker
            self.workers.append(worker)

        while True:
            message = self.server.recv()
            if message['type'] == 'new-task':
                try:
                    self.server.send(self.requests_iter.__next__())
                except StopIteration:
                    self.server.send(_CARAML_TERMINATE_FETCHER)
                    self.active_workers -= 1
                    if self.active_workers == 0:
                        break
            elif message['type'] == 'response':
                self.server.send('ack')
                request = message['request']
                data = inmem_deserialize(message['data'])
                self.handler(request, data)

        for worker in self.workers:
            worker.join()
