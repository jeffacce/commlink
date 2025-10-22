import zmq
import pickle
from typing import Iterable, Optional, Callable, Any


class Subscriber:
    def __init__(
        self,
        host: str,
        port: int = 5000,
        topics: Optional[Iterable[str]] = None,
        keep_old: bool = False,
    ):
        """
        host: host to connect to
        port: port to connect to
        topics: optional iterable of topics to subscribe to.
            If ``None``, subscribe to all topics.
        keep_old: whether to keep old messages in the buffer.
            Default False (only keep the latest message).
        """
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.deserializer = pickle.loads
        if topics is not None:
            for topic in topics:
                if " " in topic:
                    raise ValueError("topic cannot contain spaces")
                self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        if not keep_old:
            self.socket.setsockopt(zmq.CONFLATE, 1)
        self.socket.connect(f"tcp://{host}:{port}")

    def set_deserializer(self, deserializer: Optional[Callable[[bytes], Any]]):
        self.deserializer = deserializer or pickle.loads

    def get(self) -> tuple[str, Any]:
        """
        Get a tuple of (topic, data) where data is deserialized using self.deserializer (default: pickle.loads). If no message is available, this will block until one is available.
        """
        msg = self.socket.recv()
        topic = msg.split(b" ")[0].decode("utf-8")
        data = msg[len(topic) + 1 :]
        data_obj = self.deserializer(data)
        return topic, data_obj

    def stop(self):
        """
        Safely terminate the subscription and clean up the resources.
        """
        self.socket.close()
        self.context.term()


if __name__ == "__main__":
    # Example usage:
    import cv2
    import numpy as np

    def np_array_deserializer(arr):
        return np.frombuffer(arr, dtype=np.float64).reshape((100, 100))

    sub = Subscriber("localhost", port=1234, topics=["test"])
    sub.set_deserializer(np_array_deserializer)

    while True:
        topic, data = sub.get()
        print(topic)
        cv2.imshow("test", data)
        cv2.waitKey(1)
