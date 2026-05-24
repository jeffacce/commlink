import zmq
import time
import threading
import traceback
from typing import Optional
from commlink.serializer import Serializer


class RPCServer:
    """
    Expose an object's attributes and methods to remote RPCClients.

        server = RPCServer(my_obj, port=5000)
        server.start()        # non-blocking; pass threaded=False to block instead

    Clients can call methods, read/write attributes, and request shutdown
    via stop_server(). Requests are handled one at a time, so a slow method
    serializes other clients. Exceptions raised by the exposed object are
    re-raised on the client as RPCException.
    """

    def __init__(
        self,
        obj,
        port: int = 5000,
        threaded: bool = True,
        compression: Optional[str] = None,
        compression_min_bytes: int = 1024,
    ):
        """
        obj: the object whose methods and attributes to expose.
        port: port to listen on.
        threaded: if True (default), start() launches the server in a
            background thread. If False, start() blocks the calling thread.
        compression: None, 'zstd', or 'lz4'. Applied to responses.
        compression_min_bytes: responses smaller than this skip compression.
            Ignored when compression is None.
        """
        self.obj = obj
        self.context = zmq.Context()
        self.socket: zmq.socket.Socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{port}")
        self.threaded = threaded
        self.thread = None
        self.compression = compression
        self.compression_min_bytes = compression_min_bytes
        self._serializer = Serializer(
            compression=compression,
            compression_min_bytes=compression_min_bytes,
        )
        if threaded:
            self.stop_event = threading.Event()
        else:
            self.stop_event = False
        # stop() can be invoked concurrently from the run thread (via the
        # 'stop' RPC request) and from an external caller (e.g. a test's
        # finally block). Serialize so the cleanup runs exactly once.
        self._stop_lock = threading.Lock()
        self._stopped = False

    def _send_exception(self, e):
        """
        Serialize an exception and send it over the socket.
        Only the exception type, message, and traceback are sent.
        """
        exception = {
            "type": "exception",
            "content": {
                "exception": str(type(e)),
                "message": str(e),
                "traceback": traceback.format_exc(),
            },
        }
        self.socket.send_multipart(self._serializer.serialize("rpc_exception", exception))

    def _send_result(self, result):
        """
        Serialize a result and send it over the socket.
        """
        result = {"type": "result", "content": result}
        self.socket.send_multipart(self._serializer.serialize("rpc_result", result))

    def run(self):
        """
        Run the server.
        """
        try:
            if self.threaded:
                poller = zmq.Poller()
                poller.register(self.socket, zmq.POLLIN)
                while not self.stop_event.is_set():
                    socks = dict(poller.poll(timeout=100))
                    if self.socket in socks:
                        frames = self.socket.recv_multipart()
                        _, message = self._serializer.deserialize(frames)
                        self._handle_message(message)
            else:
                while not self.stop_event:
                    try:
                        frames = self.socket.recv_multipart(flags=zmq.NOBLOCK)
                        _, message = self._serializer.deserialize(frames)
                    except zmq.Again:
                        time.sleep(0.001)
                        continue
                    self._handle_message(message)
        finally:
            # Socket and context are owned by the run thread — closing them
            # from another thread races with recv and can deadlock term().
            self.socket.close()
            self.context.term()

    def _is_callable(self, attr):
        return hasattr(self.obj, attr) and callable(getattr(self.obj, attr))

    def _handle_message(self, message):
        """
        Handles a dictionary of {
            "req": str,  # request type
            "attr": str,
            "args": list,
            "kwargs": dict,
        }
        from the socket.
        If req == "is_callable", return whether the attribute is callable.
        If req == "get", return the attribute.
            If the attribute is not found, return an error message.
            If the attribute is callable, call with args and kwargs.
                If there are any errors in the callable, return the pickled error
                If the callable is found and there are no errors, return the pickled result.
            If the attribute is not callable, return the attribute.
        If req == "set", set the attribute to the value.
        If req == "dir", return a list of attributes.
        If req == "stop", stop the server.
        """
        if message["req"] == "is_callable":
            result = self._is_callable(message["attr"])
            self._send_result(result)
        elif message["req"] == "get":
            try:
                attribute = getattr(self.obj, message["attr"])
                args = message["args"]
                kwargs = message["kwargs"]
                if not callable(attribute):
                    self._send_result(attribute)
                else:
                    result = attribute(*args, **kwargs)
                    self._send_result(result)
            except Exception as e:
                self._send_exception(e)
        elif message["req"] == "set":
            try:
                setattr(self.obj, message["attr"], message["value"])
                self._send_result(None)
            except Exception as e:
                self._send_exception(e)
        elif message["req"] == "dir":
            result = dir(self.obj)
            self._send_result(result)
        elif message["req"] == "stop":
            self._send_result(True)
            self.stop()

    def start(self):
        if self.threaded:
            self.stop_event.clear()
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
        else:
            self.run()

    def stop(self):
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            if self.threaded:
                self.stop_event.set()
                if self.thread is not None and threading.current_thread() is not self.thread:
                    self.thread.join()
                    self.thread = None
            else:
                self.stop_event = True


if __name__ == "__main__":
    import numpy as np
    class HelloWorld:
        def __init__(self):
            self.abc = 123

        def hello(self):
            return np.random.randn(3, 224, 224)

        def bad(self):
            return 1 / 0

    server = RPCServer(HelloWorld(), port=1234)
    server.start()
