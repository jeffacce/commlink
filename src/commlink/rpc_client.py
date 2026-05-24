import threading
from typing import Optional
import zmq
from commlink.serializer import Serializer


class RPCException(Exception):
    def __init__(self, exception_type: str, message: str, traceback: str):
        self.exception_type = exception_type
        self.message = message
        self.traceback = traceback

    def __str__(self):
        return f"{self.exception_type}: {self.message}\n{self.traceback}"


class RPCClient:
    def __init__(
        self,
        host: str,
        port: int = 5000,
        compression: Optional[str] = None,
        compression_min_bytes: int = 1024,
    ):
        """
        host: host to connect to
        port: port to connect to
        compression: optional codec for outbound request payloads. One of None, 'zstd', 'lz4'.
            The wire format is self-describing, so the server may use a different setting.
        compression_min_bytes: per-frame size threshold below which compression is
            skipped. Has no effect when compression is None. Defaults to 1024.
        """
        self.__dict__["context"] = zmq.Context()
        self.__dict__["socket"] = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{host}:{port}")
        self.__dict__["_is_callable_cache"] = {}
        self.__dict__["compression"] = compression
        self.__dict__["compression_min_bytes"] = compression_min_bytes
        self.__dict__["_serializer"] = Serializer(
            compression=compression,
            compression_min_bytes=compression_min_bytes,
        )
        # zmq.REQ enforces strict send->recv alternation and zmq sockets are not
        # thread-safe; serialize each request/response pair so concurrent callers
        # don't trip the REQ state machine (EFSM).
        self.__dict__["_lock"] = threading.Lock()

    def __setattr__(self, attr: str, value):
        """
        Set the attribute of the same name.
        Attribute must not be callable on remote.
        """
        if self._is_callable(attr):
            raise AttributeError(f"Overwriting a callable attribute: {attr}")
        self._send_set(attr, value)

    def _send_get(self, attr: str, args: list, kwargs: dict):
        """
        Send a get request over the socket.
        """
        req = {"req": "get", "attr": attr, "args": args, "kwargs": kwargs}
        with self._lock:
            self.socket.send_multipart(self._serializer.serialize("rpc", req))
            return self._recv_result()

    def _send_set(self, attr: str, value):
        """
        Send a set request over the socket.
        """
        req = {"req": "set", "attr": attr, "value": value}
        with self._lock:
            self.socket.send_multipart(self._serializer.serialize("rpc", req))
            return self._recv_result()

    def _recv_result(self):
        """
        Receive a dictionary of {
            "type": str,
            "content": object,
        }
        if type == "exception", content is a dictionary of {
            "exception": str,
            "message": str,
            "traceback": str,
        }; re-raise the exception on the client side
        if type == "result", content is the result
        """
        result = self.socket.recv_multipart()
        _, result = self._serializer.deserialize(result)
        if result["type"] == "exception":
            raise RPCException(
                result["content"]["exception"],
                result["content"]["message"],
                result["content"]["traceback"],
            )
        return result["content"]

    def _is_callable(self, attr: str) -> bool:
        """
        Send a request to check if the attribute is callable.
        Returns False if the attribute is not found.
        """
        if attr not in self._is_callable_cache:
            req = {"req": "is_callable", "attr": attr}
            with self._lock:
                self.socket.send_multipart(self._serializer.serialize("rpc", req))
                result = self._recv_result()
            self._is_callable_cache[attr] = result
        return self._is_callable_cache[attr]

    def __getattr__(self, attr: str):
        """
        Return the attribute of the same name.
        If the attribute is a callable, return a function that sends the call over the socket.
        Else, return the attribute value.
        """
        if self._is_callable(attr):
            return lambda *args, **kwargs: self._send_get(attr, args, kwargs)
        else:
            return self._send_get(attr, [], {})

    def __dir__(self):
        """
        Return a list of attributes.
        """
        req = {"req": "dir"}
        with self._lock:
            self.socket.send_multipart(self._serializer.serialize("rpc", req))
            result = self._recv_result()
        return result + ["stop_server"]

    def stop_server(self) -> bool:
        """
        Send a stop request to the server.
        If the server is stopped, close the socket and terminate the context.
        Returns a bool for success.
        """
        req = {"req": "stop"}
        with self._lock:
            self.socket.send_multipart(self._serializer.serialize("rpc", req))
            stopped = self._recv_result()
            if stopped:
                self.socket.close()
                self.context.term()
            else:
                raise RuntimeError("Could not stop the server.")
        return stopped


if __name__ == "__main__":
    import time

    Hello = RPCClient("localhost", port=1234)
    arr = []
    for i in range(100):
        start = time.time()
        print(Hello.hello())
        arr.append(time.time() - start)
    print("Total time:", sum(arr))
    print(sum(arr) / len(arr))
    print(len(arr) / sum(arr))
    print(Hello.abc)
    Hello.new_attr = 456
    print(Hello.new_attr)
    print(dir(Hello))
    print(Hello.bad())
