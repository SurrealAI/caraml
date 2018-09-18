"""
    Defines a proxy that forwards zmq messages
"""
from threading import Thread
from multiprocessing import Process
import zmq
import nanolog as nl


logger = nl.Logger.create_logger(
    __name__,
    stream='stdout',
    time_format='MD HMS',
    show_level=True,
)


class ZmqProxy:
    """
        Creates a proxy that forwards zmq requests.
    """
    def __init__(self, in_add, out_add, context=None, pattern='router-dealer'):
        """
            The initializer collects all necessary information for the proxy
            It does not create any sockets yet
        Args:
            in_add: address that frontend binds to
            out_add: address that backend binds to
            context: Provided context for zmq,
                if None, a private context is created
            pattern: what zmq proxy pattern to use specified by
                <frontend>-<backend> i.e. `router-dealer` or `pull-push`
        """
        self._initialize()
        self.in_add = in_add
        self.out_add = out_add
        self.context = context
        self.pattern = pattern
        if pattern == 'router-dealer':
            self.frontend_protocol = zmq.ROUTER
            self.backend_protocol = zmq.DEALER
        elif pattern == 'pull-push':
            self.frontend_protocol = zmq.PULL
            self.backend_protocol = zmq.PUSH
        else:
            raise ValueError("Unkown zmq proxy patter {}. Please choose "
                             "router-dealer or pull-push".format(pattern))
        if self.context is None:
            self.context = zmq.Context()

    def run(self):
        """
            Run the proxy based on the information provided
        """
        frontend = self.context.socket(self.frontend_protocol)
        frontend.bind(self.in_add)

        # Socket facing services
        backend = self.context.socket(self.backend_protocol)
        backend.bind(self.out_add)

        message = 'Forwarding traffic from {} to {}. Pattern: {}'
        logger.debugfmt(message, self.in_add, self.out_add, self.pattern)
        zmq.proxy(frontend, backend)

        # TODO: termination
        # We never get here…
        frontend.close()
        backend.close()
        self.context.term()

    def _initialize(self):
        """
            Used by subclasses, called in __init__ before everything else
        """
        pass


class ZmqProxyThread(ZmqProxy, Thread):
    """
        Run the proxy in a thread
    """
    def _initialize(self):
        Thread.__init__(self)
        self.daemon = True


class ZmqProxyProcess(ZmqProxy, Process):
    """
        Run the proxy in a process
    """
    def _initialize(self):
        Process.__init__(self)
