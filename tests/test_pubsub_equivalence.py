
import pytest
import numpy as np
import torch
import time
import queue
import threading
from commlink.publisher import Publisher
from commlink.subscriber import Subscriber

# Define payloads to test
def get_payloads():
    payloads = []
    
    # 1. Simple primitives
    payloads.append(("simple_int", 42))
    payloads.append(("simple_float", 3.14159))
    payloads.append(("simple_str", "hello world"))
    payloads.append(("simple_list", [1, 2, 3]))
    payloads.append(("simple_dict", {"a": 1, "b": 2}))
    
    # 2. Numpy Arrays
    payloads.append(("numpy_1d", np.array([1, 2, 3], dtype=np.float32)))
    payloads.append(("numpy_2d", np.random.rand(10, 10)))
    payloads.append(("numpy_large", np.zeros((100, 100, 3), dtype=np.uint8)))
    
    # 3. Torch Tensors
    payloads.append(("torch_1d", torch.tensor([1, 2, 3], dtype=torch.float32)))
    payloads.append(("torch_2d", torch.randn(10, 10)))
    
    # 4. Complex / Nested
    payloads.append(("nested_dict", {
        "meta": "data",
        "counts": [1, 2, 3],
        "image": np.random.randint(0, 255, (32, 32, 3), dtype=np.uint8),
        "embedding": torch.randn(128)
    }))
    
    payloads.append(("list_of_arrays", [
        np.zeros(5),
        np.ones(5),
        torch.zeros(5)
    ]))
    
    return payloads

def assert_data_equal(a, b):
    """
    Recursive equality check that handles numpy arrays and torch tensors.
    """
    if isinstance(a, (np.ndarray, np.generic)):
        assert isinstance(b, (np.ndarray, np.generic))
        np.testing.assert_array_equal(a, b)
    elif torch.is_tensor(a):
        assert torch.is_tensor(b)
        assert torch.equal(a, b)
    elif isinstance(a, dict):
        assert isinstance(b, dict)
        assert a.keys() == b.keys()
        for k in a:
            assert_data_equal(a[k], b[k])
    elif isinstance(a, (list, tuple)):
        assert isinstance(b, (list, tuple))
        assert len(a) == len(b)
        for i in range(len(a)):
            assert_data_equal(a[i], b[i])
    else:
        assert a == b

def run_pubsub_exchange(port, fast_mode, topic, payload):
    """
    Spin up a publisher and subscriber, send one message, return the received message.
    """
    recv_queue = queue.Queue()
    
    # Subscriber always auto-detects, so config is same
    sub = Subscriber("localhost", port=port, topics=[topic], buffer=True)
    
    def listener():
        try:
            # Wait up to 2 seconds
            t, d = sub.get()
            recv_queue.put(d)
        except Exception as e:
            recv_queue.put(e)

    t = threading.Thread(target=listener)
    t.start()
    
    # fast_mode=True means legacy_serializer=False
    # fast_mode=False means legacy_serializer=True
    pub = Publisher("localhost", port=port, legacy_serializer=not fast_mode)
    time.sleep(0.2) # Allow connection
    
    pub.publish(topic, payload)
    
    t.join(timeout=3)
    sub.stop()
    
    if recv_queue.empty():
        pytest.fail(f"Did not receive message for {topic} (Fast={fast_mode})")
        
    result = recv_queue.get()
    if isinstance(result, Exception):
        raise result
        
    return result

@pytest.mark.parametrize("name,payload", get_payloads())
def test_pubsub_equivalence(name, payload):
    # Unique ports to avoid conflicts during parallel testing (though pytest defaults to output capture not parallel)
    # We'll use a base port + hash or similar, or just sequential if running single thread.
    # For safety let's pick a random port or rely on OS to free quickly.
    # Actually, simpler: define port based on test case hash
    port_base = 10000 + hash(name) % 5000
    
    # 1. Run Legacy
    legacy_res = run_pubsub_exchange(port_base, False, "test_legacy", payload)
    assert_data_equal(payload, legacy_res)
    
    # 2. Run Fast
    fast_res = run_pubsub_exchange(port_base + 1, True, "test_fast", payload)
    assert_data_equal(payload, fast_res)
    
    # 3. Explicit Comparison
    assert_data_equal(legacy_res, fast_res)

if __name__ == "__main__":
    # Allow running manually
    for name, payload in get_payloads():
        print(f"Testing {name}...")
        test_pubsub_equivalence(name, payload)
        print("PASS")
