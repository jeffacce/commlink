
import time
import pickle
import numpy as np
import threading
import queue
from commlink.publisher import Publisher
from commlink.subscriber import Subscriber

def generate_payloads():
    payloads = {}
    
    # RGB Image: 1280x720x3 uint8
    payloads['rgb_720p'] = np.random.randint(0, 255, (720, 1280, 3), dtype=np.uint8)
    
    # Depth Image: 1280x720 float16
    payloads['depth_720p'] = np.random.rand(720, 1280).astype(np.float16)
    
    # Pose: 4x4 float64
    payloads['pose_4x4'] = np.eye(4, dtype=np.float64)
    
    # Point Cloud: 100k points (N, 3) float32 + Colors (N, 3) uint8
    N = 100000
    points = np.random.rand(N, 3).astype(np.float32)
    colors = np.random.randint(0, 255, (N, 3), dtype=np.uint8)
    payloads['point_cloud'] = {'points': points, 'colors': colors}
    
    return payloads

def benchmark_serialization(payloads):
    print(f"{'Payload':<20} | {'Method':<10} | {'Size (MB)':<10} | {'Ser (ms)':<10} | {'Deser (ms)':<10}")
    print("-" * 75)
    
    for name, data in payloads.items():
        # Pickle
        start = time.time()
        serialized = pickle.dumps(data)
        ser_time = (time.time() - start) * 1000
        
        size_mb = len(serialized) / (1024 * 1024)
        
        start = time.time()
        _ = pickle.loads(serialized)
        deser_time = (time.time() - start) * 1000
        
        print(f"{name:<20} | {'Pickle':<10} | {size_mb:<10.2f} | {ser_time:<10.2f} | {deser_time:<10.2f}")

        # Numpy (if applicable)
        if isinstance(data, np.ndarray):
            start = time.time()
            serialized = data.tobytes()
            ser_time = (time.time() - start) * 1000
             
            size_mb = len(serialized) / (1024 * 1024)
            
            start = time.time()
            _ = np.frombuffer(serialized, dtype=data.dtype).reshape(data.shape)
            deser_time = (time.time() - start) * 1000
            
            print(f"{name:<20} | {'Numpy':<10} | {size_mb:<10.2f} | {ser_time:<10.2f} | {deser_time:<10.2f}")
    print("-" * 75)

def benchmark_e2e_latency(payloads, iterations=100):
    print(f"\nEnd-to-End Latency Benchmark ({iterations} iterations)")
    print(f"{'Payload':<20} | {'Mean Latency (ms)':<20} | {'FPS':<10}")
    print("-" * 60)

    port = 5555
    
    for name, data in payloads.items():
        topic = "bench_topic"
        
        # Setup Subscriber
        rx_queue = queue.Queue()
        sub = Subscriber("localhost", port=port, topics=[topic], buffer=False)
        
        running = True
        def sub_thread_func():
            while running:
                try:
                    t, d = sub.get()
                    rx_queue.put((t, d))
                except Exception:
                    break
        
        t_sub = threading.Thread(target=sub_thread_func)
        t_sub.start()
        
        # Setup Publisher
        pub = Publisher("localhost", port=port)
        time.sleep(0.5) # Let connections establish
        
        latencies = []
        
        for _ in range(iterations):
            start_time = time.time()
            # We pack the start time into the payload for accurate one-way latency?
            # Or just measure round trip if we had a rep-req.
            # Since this is Pub-Sub, we can measure "send start" to "recv end" in the same process
            # but that requires synchronization.
            # Simpler: Measure time from 'publish' return (which is async for ZMQ usually) to 'recv' return.
            # Wait, zmq.PUB send might be async.
            
            # Let's put a timestamp in the message wrapper if possible OR just use wall clock 
            # if we assume separate threads in same process share clock carefully.
            
            # For this single-process test, we can just track:
            pub.publish(topic, (time.time(), data))
            
            # Block wait for receive
            _, (ts_sent, idx) = rx_queue.get()
            latencies.append((time.time() - ts_sent) * 1000)
            
        mean_latency = np.mean(latencies)
        fps = 1000.0 / mean_latency if mean_latency > 0 else 0
        
        print(f"{name:<20} | {mean_latency:<20.2f} | {fps:<10.2f}")
        
        running = False
        sub.stop()
        t_sub.join()
        port += 1 # Increment port for next test to avoid TIME_WAIT issues
        
    print("-" * 60)

if __name__ == "__main__":
    payloads = generate_payloads()
    benchmark_serialization(payloads)
    benchmark_e2e_latency(payloads)
