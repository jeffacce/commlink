import time
import numpy as np
import threading
import queue
from commlink.publisher import Publisher
from commlink.subscriber import Subscriber

def benchmark_integrated():
    # Payload: 720p RGB Image
    data = np.random.randint(0, 255, (720, 1280, 3), dtype=np.uint8)
    topic = "integrated_bench"
    port = 6000
    iterations = 100

    print(f"{'Mode':<10} | {'Mean Latency (ms)':<20} | {'FPS':<10}")
    print("-" * 50)

    for legacy_mode in [True, False]:
        # Setup Subscriber
        rx_queue = queue.Queue()
        # Subscriber auto-detects mode.
        sub = Subscriber("localhost", port=port, topics=[topic], buffer=False)
        
        running = True
        def sub_thread_func():
            while running:
                try:
                    # We send (timestamp, data) to measure latency
                    t, payload = sub.get()
                    rx_queue.put(payload)
                except Exception:
                    break
        
        t_sub = threading.Thread(target=sub_thread_func)
        t_sub.start()
        
        # Setup Publisher
        pub = Publisher("localhost", port=port, legacy_serializer=legacy_mode)
        time.sleep(0.5) 
        
        latencies = []
        
        for _ in range(iterations):
            ts_start = time.time()
            pub.publish(topic, (ts_start, data))
            
            # Block wait for receive
            (ts_sent, _) = rx_queue.get()
            latencies.append((time.time() - ts_sent) * 1000)
            
        mean_latency = np.mean(latencies)
        fps = 1000.0 / mean_latency if mean_latency > 0 else 0
        
        mode_str = "Legacy" if legacy_mode else "Fast"
        print(f"{mode_str:<10} | {mean_latency:<20.2f} | {fps:<10.2f}")
        
        running = False
        sub.stop()
        t_sub.join()
        port += 1

if __name__ == "__main__":
    benchmark_integrated()
