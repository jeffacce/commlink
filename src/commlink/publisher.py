import zmq
from typing import Any, Optional
from commlink.serializer import Serializer

class Publisher:
    def __init__(
        self,
        host: str,
        port: int = 5000,
        legacy_serializer: bool = False,
        compression: Optional[str] = None,
        queue_size: Optional[int] = 10,
    ):
        """
        host: host to connect to
        port: port to connect to
        legacy_serializer: if True, use standard pickle.dumps (compatible with older commlinks).
                           if False (default), use ZMQ multipart messages and Pickle Protocol 5 for faster serialization.
        compression: optional codec for payload compression. One of None, 'zstd', 'lz4'.
                     Cannot be combined with legacy_serializer=True. Subscribers do not need
                     matching configuration; the wire format is self-describing.
        queue_size: max number of pending messages to keep buffered before the
                    publisher starts silently dropping new ones. Default 10, tuned
                    for real-time streaming -- keeps latency low and bounds memory
                    while tolerating brief receiver hiccups. Pass a larger value
                    (e.g. 1000) for buffered/event use cases where every message
                    matters more than freshness, or None to use ZMQ's built-in
                    (1000). (Sets ZMQ_SNDHWM under the hood.)
        """
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        # ZMQ_SNDHWM must be set BEFORE bind/connect to take effect.
        if queue_size is not None:
            self.socket.setsockopt(zmq.SNDHWM, queue_size)
        self.socket.bind(f"tcp://{host}:{port}")
        self.legacy_serializer = legacy_serializer
        self.compression = compression
        self.queue_size = queue_size
        self._serializer = Serializer(compression=compression, legacy=legacy_serializer)

    def publish(self, topic: str, data: Any):
        """
        Publish a dictionary of {
            "topic": str,
            "data": object,
        }
        """
        if " " in topic:
            raise ValueError("topic cannot contain spaces")
        frames = self._serializer.serialize(topic, data)
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
