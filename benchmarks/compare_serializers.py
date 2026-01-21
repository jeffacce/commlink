
import time
import pickle
import numpy as np
import torch
import zmq

def generate_payloads():
    payloads = {}
    
    # RGB Image: 1280x720x3 uint8
    payloads['rgb_720p_np'] = np.random.randint(0, 255, (720, 1280, 3), dtype=np.uint8)
    payloads['rgb_720p_torch'] = torch.randint(0, 255, (720, 1280, 3), dtype=torch.uint8)
    
    # Pose: 4x4 float64
    payloads['pose_4x4_np'] = np.eye(4, dtype=np.float64)
    payloads['pose_4x4_torch'] = torch.eye(4, dtype=torch.float64)
    
    # Point Cloud: 100k points (N, 3) float32
    N = 100000
    payloads['point_cloud_np'] = np.random.rand(N, 3).astype(np.float32)
    payloads['point_cloud_torch'] = torch.rand(N, 3, dtype=torch.float32)

    return payloads

def benchmark_serializers(payloads, iterations=50):
    print(f"{'Payload':<20} | {'Method':<25} | {'Ser (ms)':<10} | {'Deser (ms)':<10} | {'Copy?':<5}")
    print("-" * 80)
    
    for name, data in payloads.items():
        # 1. Baseline: Pickle Default + Concat (Current Commlink)
        # ----------------------------------------------------------------
        topic = b"test_topic"
        
        start = time.time()
        for _ in range(iterations):
            serialized = pickle.dumps(data)
            # Simulate Commlink's current wire format construction
            msg = topic + b" " + serialized 
        ser_time = (time.time() - start) / iterations * 1000

        start = time.time()
        for _ in range(iterations):
            # Simulate split and load
            t = msg.split(b" ")[0]
            d = msg[len(t)+1:]
            _ = pickle.loads(d)
        deser_time = (time.time() - start) / iterations * 1000
        
        print(f"{name:<20} | {'Pickle Default + Concat':<25} | {ser_time:<10.3f} | {deser_time:<10.3f} | {'Yes':<5}")

        # 2. Pickle Protocol 5 + Concat
        # ----------------------------------------------------------------
        start = time.time()
        for _ in range(iterations):
            serialized = pickle.dumps(data, protocol=5)
            msg = topic + b" " + serialized 
        ser_time = (time.time() - start) / iterations * 1000

        start = time.time()
        for _ in range(iterations):
            t = msg.split(b" ")[0]
            d = msg[len(t)+1:]
            _ = pickle.loads(d)
        deser_time = (time.time() - start) / iterations * 1000
        
        print(f"{name:<20} | {'Pickle 5 + Concat':<25} | {ser_time:<10.3f} | {deser_time:<10.3f} | {'Yes':<5}")

        # 3. Pickle 5 + Multipart (Simulated)
        # ----------------------------------------------------------------
        # Pickle 5 can return out-of-band buffers. 
        # But even just avoiding the 'concat' step is a win.
        # We will simulate zmq.send_multipart by NOT doing the concatenation.
        
        start = time.time()
        for _ in range(iterations):
            # In a real ZMQ multipart scenario with Pickle 5, we'd use:
            # buffers = []
            # serialized = pickle.dumps(data, protocol=5, buffer_callback=buffers.append)
            # msg_parts = [topic, serialized] + buffers
            
            buffers = []
            serialized = pickle.dumps(data, protocol=5, buffer_callback=buffers.append)
            msg_parts = [topic, serialized] + buffers
            
        ser_time = (time.time() - start) / iterations * 1000

        # For deserialization, we simulate receiving parts
        start = time.time()
        for _ in range(iterations):
            # Reconstruct
            # The first part is topic (skipped here as we know it)
            # The second is the main pickle stream
            # The rest are buffers
            main_stream = msg_parts[1]
            buffers = msg_parts[2:]
            _ = pickle.loads(main_stream, buffers=buffers)
            
        deser_time = (time.time() - start) / iterations * 1000
        
        print(f"{name:<20} | {'Pickle 5 + Multipart':<25} | {ser_time:<10.3f} | {deser_time:<10.3f} | {'No':<5}")
        print("-" * 80)

if __name__ == "__main__":
    payloads = generate_payloads()
    benchmark_serializers(payloads)
