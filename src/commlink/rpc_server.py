import zmq
import time
import pickle
import threading
import traceback


class RPCServer:
    def __init__(self, obj, port: int = 5000, threaded: bool = True):
        """
        obj: object with methods to expose
        port: port to listen on
        """
        self.obj = obj
        self.context = zmq.Context()
        self.socket: zmq.socket.Socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{port}")
        self.threaded = threaded
        self.thread = None
        if threaded:
            self.stop_event = threading.Event()
        else:
            self.stop_event = False

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
        self.socket.send(pickle.dumps(exception))

    def _send_result(self, result):
        """
        Serialize a result and send it over the socket.
        """
        result = {"type": "result", "content": result}
        self.socket.send(pickle.dumps(result))

    def run(self):
        """
        Run the server.
        """
        if self.threaded:
            while not self.stop_event.is_set():
                try:
                    message = self.socket.recv()
                    message = pickle.loads(message)
                    self._handle_message(message)
                except zmq.ContextTerminated:
                    break
        else:
            while not self.stop_event:
                try:
                    message = self.socket.recv(flags=zmq.NOBLOCK)
                    message = pickle.loads(message)
                except zmq.Again:
                    time.sleep(0.001)
                    continue
                self._handle_message(message)

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
        self.socket.close()
        self.context.term()
        if self.threaded:
            self.stop_event.set()
            if self.thread and threading.current_thread() is not self.thread:
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
