import socket
import threading
import time

import pytest

from commlink.rpc_client import RPCClient, RPCException
from commlink.rpc_server import RPCServer


class ExampleService:
    def __init__(self):
        self.value = 0

    def increment(self, amount: int = 1) -> int:
        self.value += amount
        return self.value

    def raise_error(self) -> None:
        raise ValueError("boom")


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_server(service: ExampleService, port: int, threaded: bool):
    server = RPCServer(service, port=port, threaded=threaded)
    if threaded:
        server.start()
        server_thread = server.thread
    else:
        server_thread = threading.Thread(target=server.start, daemon=True)
        server_thread.start()
    time.sleep(0.05)
    return server, server_thread


@pytest.mark.parametrize("threaded", [True, False])
def test_rpc_server_start_stop(threaded):
    port = get_free_port()
    service = ExampleService()
    server, server_thread = start_server(service, port, threaded)
    try:
        if threaded:
            assert server.thread is not None
            assert server.thread.is_alive()
            server.stop()
            assert server.thread is None
        else:
            assert server_thread is not None
            server.stop()
            server_thread.join(timeout=1)
            assert not server_thread.is_alive()
    finally:
        if threaded:
            if server.thread is not None:
                server.stop()
        else:
            if server_thread is not None and server_thread.is_alive():
                server.stop()
                server_thread.join(timeout=1)


def test_rpc_client_connect():
    port = get_free_port()
    service = ExampleService()
    server, server_thread = start_server(service, port, threaded=True)
    try:
        client = RPCClient("127.0.0.1", port=port)
        assert "increment" in dir(client)
    finally:
        if server.thread is not None:
            server.stop()


@pytest.mark.parametrize("threaded", [True, False])
def test_rpc_server_client_integration(threaded):
    port = get_free_port()
    service = ExampleService()
    server, server_thread = start_server(service, port, threaded)
    client = RPCClient("127.0.0.1", port=port)
    try:
        assert client.increment(5) == 5
        assert client.value == 5

        client.value = 42
        assert client.value == 42

        with pytest.raises(RPCException) as exc:
            client.raise_error()
        assert "ValueError" in str(exc.value)

        stop_result = client.stop_server()
        assert stop_result is True
    finally:
        if server_thread is not None:
            server_thread.join(timeout=1)
        if threaded:
            if server.thread is not None:
                server.stop()
        else:
            if server_thread is not None and server_thread.is_alive():
                server.stop()
                server_thread.join(timeout=1)
