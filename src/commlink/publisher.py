import zmq
from typing import Any
from commlink.serializer import serialize

class Publisher:
    def __init__(self, host: str, port: int = 5000, legacy_serializer: bool = False):
        """
        host: host to connect to
        port: port to connect to
        legacy_serializer: if True, use standard pickle.dumps (compatible with older commlinks).
                           if False (default), use ZMQ multipart messages and Pickle Protocol 5 for faster serialization.
        """
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(f"tcp://{host}:{port}")
        self.legacy_serializer = legacy_serializer

    def publish(self, topic: str, data: Any):
        """
        Publish a dictionary of {
            "topic": str,
            "data": object,
        }
        """
        if " " in topic:
            raise ValueError("topic cannot contain spaces")
        frames = serialize(topic, data, legacy=self.legacy_serializer)
        self.socket.send_multipart(frames)

    def __setitem__(self, topic: str, data: Any):
        """
        Allow dict-style publishing via publisher[topic] = data.
        """
        self.publish(topic, data)


if __name__ == "__main__":
    # Example usage:
    import numpy as np

    pub = Publisher("*", port=1234)

    while True:
        pub["test"] = np.random.rand(100, 100)
