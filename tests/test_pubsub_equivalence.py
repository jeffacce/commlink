
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

def run_pubsub_exchange(port, topic, payload):
    """
    Spin up a publisher and subscriber, send one message, return the received message.
    """
    recv_queue = queue.Queue()

    sub = Subscriber("localhost", port=port, topics=[topic], buffer=True)

    def listener():
        try:
            t, d = sub.get()
            recv_queue.put(d)
        except Exception as e:
            recv_queue.put(e)

    t = threading.Thread(target=listener)
    t.start()

    pub = Publisher("localhost", port=port)
    time.sleep(0.2) # Allow connection

    pub.publish(topic, payload)

    t.join(timeout=3)
    sub.stop()

    if recv_queue.empty():
        pytest.fail(f"Did not receive message for {topic}")

    result = recv_queue.get()
    if isinstance(result, Exception):
        raise result

    return result

@pytest.mark.parametrize("name,payload", get_payloads())
def test_pubsub_roundtrip(name, payload):
    port = 10000 + hash(name) % 5000
    res = run_pubsub_exchange(port, "test", payload)
    assert_data_equal(payload, res)

if __name__ == "__main__":
    for name, payload in get_payloads():
        print(f"Testing {name}...")
        test_pubsub_roundtrip(name, payload)
        print("PASS")
