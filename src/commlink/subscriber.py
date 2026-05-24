import collections
import threading
import time
import uuid
import warnings
from typing import Any, Iterable, Optional

import zmq

from commlink.serializer import Serializer

OP_PULL = b"PULL"
OP_SUB = b"SUB"
OP_PUSH = b"PUSH"
OP_OK = b"OK"

# Subscriber-side first-call poll cadence: how often to re-PULL while waiting for
# a topic the publisher hasn't published yet. 10 ms keeps startup snappy without
# burning CPU.
_FIRST_CALL_POLL_INTERVAL = 0.01


class Subscriber:
    def __init__(
        self,
        host: str,
        port: int = 5000,
        topics: Optional[Iterable[str]] = None,
        buffer: bool = False,
        compression: Optional[str] = None,
        queue_size: int = 30,
    ):
        """
        host, port: publisher endpoint.
        topics: optional filter. None = no filter (all topics). In pull mode (buffer=False)
            the filter is informational only; each sub[k] still issues a fresh PULL.
            In push mode (buffer=True) the filter is sent to the publisher so it can
            skip irrelevant topics in fanout.
        buffer: False (default) = pull mode. sub[k] does one PULL round trip and returns
            the latest cached payload (or blocks on first call until the publisher has
            data for that topic). True = push mode. Subscriber receives every published
            frame for matching topics into per-topic bounded queues.
        compression: deprecated, silently accepted. The wire format is self-describing,
            so the subscriber decodes any codec automatically.
        queue_size: only used in push mode. Per-topic deque maxlen and ZMQ_RCVHWM.
            Default 30 (~1s of 30 Hz jitter). Ignored in pull mode.
        """
        if isinstance(topics, str):
            raise TypeError("topics must be an iterable of strings, not a single string")
        if topics is not None:
            topics = list(topics)
            if any(not isinstance(t, str) for t in topics):
                raise TypeError("topics must be an iterable of strings")
            for t in topics:
                _validate_topic(t)
        if compression is not None:
            warnings.warn(
                "compression= on Subscriber is deprecated and has no effect; "
                "the wire format is self-describing.",
                DeprecationWarning,
                stacklevel=2,
            )

        self.buffer = buffer
        self.compression = compression
        self.queue_size = queue_size
        self._topics_filter: Optional[list[str]] = topics  # None or list
        self._serializer = Serializer()
        self.context = zmq.Context.instance()
        self._endpoint = f"tcp://{host}:{port}"

        self._stop_lock = threading.Lock()
        self._stopped = False

        if buffer:
            self._init_push_mode()
        else:
            self._init_pull_mode()

    # ------------------------------------------------------------------ pull mode

    def _init_pull_mode(self):
        # One DEALER per topic, created lazily on first access. Each is guarded by
        # its own lock since DEALER is full-duplex but we want send/recv pairing
        # per PULL request.
        self._req_sockets: dict[str, zmq.Socket] = {}
        self._req_locks: dict[str, threading.Lock] = {}
        self._last_version: dict[str, int] = {}
        self._cache: dict[str, Any] = {}
        # Guards the three dicts above (creation race), not the per-topic I/O.
        self._pull_setup_lock = threading.Lock()

    def _get_req_socket(self, topic: str):
        # Fast path without lock.
        sock = self._req_sockets.get(topic)
        if sock is not None:
            return sock, self._req_locks[topic]
        with self._pull_setup_lock:
            sock = self._req_sockets.get(topic)
            if sock is not None:
                return sock, self._req_locks[topic]
            s = self.context.socket(zmq.DEALER)
            s.connect(self._endpoint)
            self._req_sockets[topic] = s
            self._req_locks[topic] = threading.Lock()
            self._last_version.setdefault(topic, 0)
            return s, self._req_locks[topic]

    def _pull_once(self, topic: str):
        """Issue one PULL round trip. Returns (had_payload, data_or_None)."""
        sock, lock = self._get_req_socket(topic)
        topic_bytes = topic.encode("utf-8")
        with lock:
            last_version = self._last_version.get(topic, 0)
            sock.send_multipart(
                [OP_PULL, topic_bytes, last_version.to_bytes(8, "big")]
            )
            frames = sock.recv_multipart()
        if not frames or frames[0] != OP_PULL:
            raise RuntimeError(f"unexpected reply opcode: {frames[:1]!r}")
        version = int.from_bytes(frames[1], "big")
        # Reply shapes:
        #   no change / unknown: [OP_PULL, version, b""]   -> len 3
        #   new payload:         [OP_PULL, version, topic_bytes, *frames]
        if len(frames) == 3 and not frames[2]:
            return False, None
        topic_str, data = self._serializer.deserialize(frames[2:])
        self._last_version[topic] = version
        self._cache[topic] = data
        return True, data

    def _pull_get(self, topic: str) -> Any:
        had_payload, data = self._pull_once(topic)
        if had_payload:
            return data
        if topic in self._cache:
            return self._cache[topic]
        # First call for a topic the publisher hasn't published yet. Block (poll)
        # until something shows up.
        while True:
            time.sleep(_FIRST_CALL_POLL_INTERVAL)
            had_payload, data = self._pull_once(topic)
            if had_payload:
                return data
            if topic in self._cache:
                return self._cache[topic]

    # ------------------------------------------------------------------ push mode

    def _init_push_mode(self):
        self._dealer = self.context.socket(zmq.DEALER)
        if self.queue_size is not None:
            self._dealer.setsockopt(zmq.RCVHWM, self.queue_size)
        self._dealer.connect(self._endpoint)

        # Handshake: register with publisher and wait for OK so the caller knows
        # SUB is in effect before publish() can fanout.
        filter_bytes = (
            b""
            if self._topics_filter is None
            else b",".join(t.encode("utf-8") for t in self._topics_filter)
        )
        # Fire-and-forget SUB. ZMQ queues the message and delivers it whenever
        # the publisher comes up, so subscribers and publishers can spin up in
        # any order. The recv loop silently drops the SUB OK ack when it
        # arrives.
        self._dealer.send_multipart([OP_SUB, filter_bytes])

        self._queues: dict[str, collections.deque] = {}
        self._global_queue: collections.deque = collections.deque(maxlen=self.queue_size)
        self._cv = threading.Condition()

        ctrl_endpoint = f"inproc://commlink-sub-ctrl-{uuid.uuid4().hex}"
        self._ctrl_recv = self.context.socket(zmq.PAIR)
        self._ctrl_recv.bind(ctrl_endpoint)
        self._ctrl_send = self.context.socket(zmq.PAIR)
        self._ctrl_send.connect(ctrl_endpoint)

        self._recv_thread = threading.Thread(
            target=self._push_recv_loop, name="commlink-subscriber", daemon=True
        )
        self._recv_thread.start()

    def _push_recv_loop(self):
        poller = zmq.Poller()
        poller.register(self._dealer, zmq.POLLIN)
        poller.register(self._ctrl_recv, zmq.POLLIN)
        while True:
            try:
                events = dict(poller.poll())
            except zmq.ContextTerminated:
                return
            except zmq.ZMQError:
                if self._stopped:
                    return
                raise
            if self._ctrl_recv in events:
                try:
                    self._ctrl_recv.recv()
                except zmq.ZMQError:
                    pass
                return
            if self._dealer not in events:
                continue
            try:
                frames = self._dealer.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.Again:
                continue
            except zmq.ZMQError:
                if self._stopped:
                    return
                raise
            if not frames:
                continue
            if frames[0] == OP_SUB:
                # SUB OK ack from publisher; nothing to do here.
                continue
            if frames[0] != OP_PUSH:
                continue
            # [OP_PUSH, version_bytes, topic_bytes, *payload_frames]
            if len(frames) < 4:
                continue
            payload = frames[2:]  # [topic_bytes, *frames]
            try:
                topic_str, data = self._serializer.deserialize(payload)
            except Exception:
                continue
            with self._cv:
                q = self._queues.get(topic_str)
                if q is None:
                    q = collections.deque(maxlen=self.queue_size)
                    self._queues[topic_str] = q
                q.append(data)
                self._global_queue.append((topic_str, data))
                self._cv.notify_all()

    def _push_get(self, topic: Optional[str]) -> Any:
        with self._cv:
            if topic is None:
                while not self._global_queue:
                    if self._stopped:
                        raise RuntimeError("Subscriber stopped")
                    self._cv.wait()
                return self._global_queue.popleft()
            q = self._queues.get(topic)
            while not q:
                if self._stopped:
                    raise RuntimeError("Subscriber stopped")
                self._cv.wait()
                q = self._queues.get(topic)
            return q.popleft()

    # ------------------------------------------------------------------ public API

    def get(self, topic: Optional[str] = None) -> Any:
        if self.buffer:
            return self._push_get(topic)
        if topic is None:
            raise TypeError(
                "get() requires a topic in buffer=False mode; each pull is per-topic."
            )
        return self._pull_get(topic)

    def __getitem__(self, topic: str) -> Any:
        if self.buffer:
            return self._push_get(topic)
        return self._pull_get(topic)

    def stop(self):
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            if self.buffer:
                try:
                    self._ctrl_send.send(b"x")
                except zmq.ZMQError:
                    pass
                # Wake any waiters in _push_get.
                with self._cv:
                    self._cv.notify_all()
                if threading.current_thread() is not self._recv_thread:
                    self._recv_thread.join(timeout=2.0)
                for s in (self._dealer, self._ctrl_send, self._ctrl_recv):
                    try:
                        s.close(linger=0)
                    except zmq.ZMQError:
                        pass
            else:
                for s in self._req_sockets.values():
                    try:
                        s.close(linger=0)
                    except zmq.ZMQError:
                        pass

    def __del__(self):
        try:
            self.stop()
        except Exception:
            pass


def _validate_topic(topic: str):
    if " " in topic:
        raise ValueError("topic cannot contain spaces")


if __name__ == "__main__":
    import cv2

    sub = Subscriber("localhost", port=1234, topics=["test"])
    while True:
        data = sub["test"]
        cv2.imshow("test", data)
        cv2.waitKey(1)
