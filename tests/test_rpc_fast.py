import socket
import threading
import time
import pytest
import numpy as np
import torch
from commlink.rpc_client import RPCClient
from commlink.rpc_server import RPCServer

class DataService:
    def __init__(self):
        self.data = []

    def echo(self, x):
        return x

    def store(self, x):
        self.data.append(x)
        return len(self.data) - 1

    def get(self, idx):
        return self.data[idx]

def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

def start_server(service, port):
    server = RPCServer(service, port=port, threaded=True)
    server.start()
    time.sleep(0.05)
    return server

def test_fast_rpc_numpy():
    port = get_free_port()
    service = DataService()
    server = start_server(service, port)
    
    try:
        # Fast Client (default now)
        client = RPCClient("127.0.0.1", port=port)
        
        # Test Numpy
        arr = np.random.randn(100, 100)
        res = client.echo(arr)
        assert np.array_equal(res, arr)
        
        # Test large array performance/correctness check
        large_arr = np.zeros((1000, 1000))
        idx = client.store(large_arr)
        retrieved = client.get(idx)
        assert np.array_equal(retrieved, large_arr)

    finally:
        server.stop()

def test_fast_rpc_torch():
    port = get_free_port()
    service = DataService()
    server = start_server(service, port)
    
    try:
        # Fast Client (default now)
        client = RPCClient("127.0.0.1", port=port)
        
        # Test Torch
        t = torch.randn(50, 50)
        res = client.echo(t)
        assert torch.equal(res, t)

    finally:
        server.stop()
