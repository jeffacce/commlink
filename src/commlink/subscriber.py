import pickle
from typing import Iterable, Optional, Callable, Any
import warnings

import zmq


class Subscriber:
    def __init__(
        self,
        host: str,
        port: int = 5000,
        topics: Optional[Iterable[str]] = [],
        buffer: bool = False,
    ):
        """
        host: host to connect to
        port: port to connect to
        topics: optional list of topics to subscribe to.
            If not supplied, subscribe to all topics.
        buffer: whether to keep old messages in the buffer (no conflation).
            Default False (only keep latest for each topic).
        """
        self.buffer = buffer
        self.context = zmq.Context()
        self.deserializer = pickle.loads
        self._endpoint = f"tcp://{host}:{port}"
        self._topic_sockets: dict[Optional[str], zmq.Socket] = {}

        if isinstance(topics, str):
            raise TypeError("topics must be an iterable of strings, not a single string")
        else:
            topics = list(topics)
            if any(not isinstance(t, str) for t in topics):
                raise TypeError("topics must be an iterable of strings")
        if not topics and not buffer:
            warnings.warn(
                "Subscribing to all topics with buffer=False keeps only the latest message across all topics."
                "Specify topics or set buffer=True to avoid this warning.",
                RuntimeWarning,
                stacklevel=2,
            )

        self._global_socket = self._new_socket()
        self._global_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self._global_socket.connect(self._endpoint)
        self._topic_sockets[None] = self._global_socket

        for topic in topics:
            self._topic_sockets[topic] = self._create_topic_socket(topic)

    def set_deserializer(self, deserializer: Optional[Callable[[bytes], Any]] = None):
        self.deserializer = deserializer or pickle.loads

    def get(self, topic: Optional[str] = None) -> Any:
        """
        Get data for a topic.
        - sub.get() returns (topic, data) from the global subscriber.
        - sub.get(topic) returns the deserialized data from that topic's dedicated subscriber.
        """
        if topic not in self._topic_sockets:
            raise KeyError(f"Topic '{topic}' was not subscribed.")
        msg = self._topic_sockets[topic].recv()
        topic_str, data_obj = self._deserialize(msg)
        return (topic_str, data_obj) if topic is None else data_obj

    def __getitem__(self, topic: str) -> Any:
        """
        Retrieve data for a specific topic via sub[topic].
        """
        return self.get(topic)

    def stop(self):
        """
        Safely terminate the subscription and clean up the resources.
        """
        for socket in self._topic_sockets.values():
            socket.close()
        self.context.term()

    def _new_socket(self) -> zmq.Socket:
        socket = self.context.socket(zmq.SUB)
        if not self.buffer:
            socket.setsockopt(zmq.CONFLATE, 1)
        return socket

    def _create_topic_socket(self, topic: str) -> zmq.Socket:
        self._validate_topic(topic)
        socket = self._new_socket()
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        socket.connect(self._endpoint)
        return socket

    def _validate_topic(self, topic: str):
        if " " in topic:
            raise ValueError("topic cannot contain spaces")

    def _deserialize(self, msg: bytes) -> tuple[str, Any]:
        topic = msg.split(b" ")[0].decode("utf-8")
        data = msg[len(topic) + 1 :]
        data_obj = self.deserializer(data)
        return topic, data_obj


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
