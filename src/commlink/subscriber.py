import pickle
from typing import Iterable, Optional, Callable, Any

import zmq


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
        self.keep_old = keep_old
        self.context = zmq.Context()
        self.deserializer = pickle.loads
        self._endpoint = f"tcp://{host}:{port}"
        self._topic_sockets: dict[str, zmq.Socket] = {}

        provided_topics = list(topics) if topics is not None else None
        if provided_topics:
            provided_topics = self._deduplicate_topics(provided_topics)

        self._global_socket = self._new_socket()
        if provided_topics is None:
            self._global_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        else:
            for topic in provided_topics:
                self._validate_topic(topic)
                self._global_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        self._global_socket.connect(self._endpoint)

        if provided_topics:
            for topic in provided_topics:
                self._topic_sockets[topic] = self._create_topic_socket(topic)

    def set_deserializer(self, deserializer: Optional[Callable[[bytes], Any]]):
        self.deserializer = deserializer or pickle.loads

    def get(self, topic: Optional[str] = None) -> tuple[str, Any]:
        """
        Get a tuple of (topic, data) where data is deserialized using self.deserializer (default: pickle.loads).
        If a topic is provided, the message will be read from that topic's dedicated subscriber.
        If no topic is provided, the first available message from any subscribed topic is returned.
        """
        socket = self._global_socket if topic is None else self._get_topic_socket(topic)
        msg = socket.recv()
        return self._deserialize(msg)

    def __getitem__(self, topic: str) -> tuple[str, Any]:
        """
        Retrieve a message for a specific topic via sub[topic].
        """
        return self.get(topic)

    def stop(self):
        """
        Safely terminate the subscription and clean up the resources.
        """
        for socket in self._topic_sockets.values():
            socket.close()
        if self._global_socket:
            self._global_socket.close()
        self.context.term()

    def _new_socket(self) -> zmq.Socket:
        socket = self.context.socket(zmq.SUB)
        if not self.keep_old:
            socket.setsockopt(zmq.CONFLATE, 1)
        return socket

    def _create_topic_socket(self, topic: str) -> zmq.Socket:
        self._validate_topic(topic)
        socket = self._new_socket()
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        socket.connect(self._endpoint)
        return socket

    def _get_topic_socket(self, topic: str) -> zmq.Socket:
        if topic not in self._topic_sockets:
            self._topic_sockets[topic] = self._create_topic_socket(topic)
        return self._topic_sockets[topic]

    def _deduplicate_topics(self, topics: list[str]) -> list[str]:
        seen = set()
        unique_topics = []
        for topic in topics:
            if topic not in seen:
                seen.add(topic)
                unique_topics.append(topic)
        return unique_topics

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
