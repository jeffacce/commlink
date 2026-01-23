import time
import numpy as np
from commlink.rpc_client import RPCClient
from commlink.rpc_server import RPCServer
import threading

class BenchmarkService:
    def echo(self, x):
        return x

def start_server(port):
    service = BenchmarkService()
    server = RPCServer(service, port=port, threaded=True)
    server.start()
    return server

def benchmark_roundtrip(size_mb, iterations=20):
    port = 5555
    server = start_server(port)
    
    try:
        # Create data
        elements = int(size_mb * 1024 * 1024 / 8) # float64 = 8 bytes
        data = np.random.randn(elements)
        print(f"Benchmarking with {size_mb} MB array ({elements} elements)")
        
        # Fast Client (default now)
        client_fast = RPCClient("127.0.0.1", port=port)
        # Warmup
        client_fast.echo(data)
        
        start_time = time.time()
        for _ in range(iterations):
            client_fast.echo(data)
        fast_duration = time.time() - start_time
        fast_fps = iterations / fast_duration
        print(f"Fast (Multipart): {fast_fps:.2f} FPS, {fast_duration/iterations:.4f} s/iter")
        
    finally:
        server.stop()

if __name__ == "__main__":
    benchmark_roundtrip(size_mb=10, iterations=50)
    benchmark_roundtrip(size_mb=100, iterations=10)
