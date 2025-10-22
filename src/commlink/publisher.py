import zmq
import pickle
from typing import Optional, Callable, Any


class Publisher:
    def __init__(self, host: str, port: int = 5000):
        """
        host: host to connect to
        port: port to connect to
        """
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(f"tcp://{host}:{port}")
        self.serializer = pickle.dumps

    def set_serializer(self, serializer: Optional[Callable[[Any], bytes]]):
        self.serializer = serializer or pickle.dumps

    def publish(self, topic: str, data: Any):
        """
        Publish a dictionary of {
            "topic": str,
            "data": object,
        }
        where the data is serialized using self.serializer (default: pickle.dumps)
        """
        if " " in topic:
            raise ValueError("topic cannot contain spaces")
        topic = topic.encode("utf-8")
        data = self.serializer(data)
        msg = topic + b" " + data
        self.socket.send(msg)


if __name__ == "__main__":
    # Example usage:
    import numpy as np

    def np_array_serializer(arr):
        return arr.tobytes()

    if __name__ == "__main__":
        pub = Publisher("*", port=1234)
        pub.set_serializer(np_array_serializer)

        while True:
            pub.publish("test", np.random.rand(100, 100))
