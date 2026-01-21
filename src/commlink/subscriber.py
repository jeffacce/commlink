from typing import Iterable, Optional, Any
import warnings
import zmq

from commlink.serializer import deserialize

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
            If False, it also maintains a cache of the last received message.
            This means get() will return the last known value if no new data is available.
        """
        self.buffer = buffer
        self.context = zmq.Context()
        self._endpoint = f"tcp://{host}:{port}"
        self._topic_sockets: dict[Optional[str], zmq.Socket] = {}
        self._cache: dict[Optional[str], Any] = {}

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

    def get(self, topic: Optional[str] = None) -> Any:
        """
        Get data for a topic.
        - sub.get() returns (topic, data) from the global subscriber.
        - sub.get(topic) returns the deserialized data from that topic's dedicated subscriber.
        """
        if topic not in self._topic_sockets:
            raise KeyError(f"Topic '{topic}' was not subscribed.")

        socket = self._topic_sockets[topic]

        # Helper to receive one message
        def recv_one(flags=0):
            frames = socket.recv_multipart(flags=flags)
            return deserialize(frames)

        # If not buffering, we want the LATEST message (conflation) AND we persist the last value.
        # Since ZMQ_CONFLATE doesn't support multipart, we manually drain the queue.
        if not self.buffer:
            # Try to read all pending messages
            last_msg = None
            while True:
                try:
                    last_msg = recv_one(flags=zmq.NOBLOCK)
                except zmq.Again:
                    break
            
            if last_msg is not None:
                topic_str, data_obj = last_msg
                if topic is None:
                    self._cache[None] = (topic_str, data_obj)
                else:
                    self._cache[topic] = data_obj
                return (topic_str, data_obj) if topic is None else data_obj
            
            # If we drained nothing (queue was empty start), return cached value if exists
            if topic in self._cache:
                return self._cache[topic]
            elif topic is None and None in self._cache:
                return self._cache[None]
            
            # If no new data and no cache, fall through to blocking wait?
            # User said "persist=True when buffer=False".
            # Usually persist implies "if I have nothing, give me last".
            # If I have NOTHING (start up), I must wait for something.
            # So falling through to blocking receive is correct.

        # Blocking receive (fallback or default behavior for buffer=True)
        # Also reached if buffer=False but no cache and no new data.
        topic_str, data_obj = recv_one()
        
        # If we are in non-buffered mode, we update cache even on blocking receive
        if not self.buffer:
            if topic is None:
                self._cache[None] = (topic_str, data_obj)
            else:
                self._cache[topic] = data_obj

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



if __name__ == "__main__":
    # Example usage:
    import cv2

    sub = Subscriber("localhost", port=1234, topics=["test"])

    while True:
        topic, data = sub.get()
        print(topic)
        cv2.imshow("test", data)
        cv2.waitKey(1)
