import threading
import time
import uuid
from typing import Any, Optional

import zmq

from commlink.serializer import Serializer


# Opcodes on the first frame of any subscriber<->publisher message.
OP_PULL = b"PULL"
OP_SUB = b"SUB"
OP_PUSH = b"PUSH"
OP_OK = b"OK"


class Publisher:
    def __init__(
        self,
        host: str,
        port: int = 5000,
        compression: Optional[str] = None,
        compression_min_bytes: int = 1024,
        queue_size: Optional[int] = 10,
    ):
        """
        host: bind address for the ROUTER socket (e.g. "*" for all interfaces).
        port: bind port.
        compression: optional codec for payload compression. One of None, 'zstd', 'lz4'.
                     The wire format is self-describing, so subscribers do not need a
                     matching setting.
        compression_min_bytes: per-frame size threshold below which compression is
                     skipped. Each pickle main frame and out-of-band buffer is
                     evaluated independently, so a small joint-state message pays
                     no compression cost while images in the same publisher are
                     still compressed. Has no effect when compression is None.
                     Defaults to 1024.
        queue_size: ZMQ_SNDHWM for the ROUTER socket. Bounds how many outbound frames
                    may queue per subscriber identity before sends start dropping.
                    Default 10. None falls through to ZMQ's built-in (1000).
        """
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.ROUTER)
        if queue_size is not None:
            self.socket.setsockopt(zmq.SNDHWM, queue_size)
        self.socket.bind(f"tcp://{host}:{port}")

        self.compression = compression
        self.compression_min_bytes = compression_min_bytes
        self.queue_size = queue_size
        self._serializer = Serializer(
            compression=compression,
            compression_min_bytes=compression_min_bytes,
        )

        # Cache: topic -> (version_ns, [serialized frames]). Both the publish thread
        # (writer) and the service thread (PULL reader) touch this, so all access is
        # under _lock. ROUTER socket sends/recvs are also serialized by _lock since
        # pyzmq sockets are not thread-safe on their own.
        self._cache: dict[str, tuple[int, list[bytes]]] = {}
        # identity_bytes -> Optional[set[str]] of subscribed topics; None means "all".
        self._push_subs: dict[bytes, Optional[set[str]]] = {}
        self._lock = threading.Lock()

        # Inproc PAIR pipe used to wake the service thread out of poll() on stop().
        ctrl_endpoint = f"inproc://commlink-pub-ctrl-{uuid.uuid4().hex}"
        self._ctrl_recv = self.context.socket(zmq.PAIR)
        self._ctrl_recv.bind(ctrl_endpoint)
        self._ctrl_send = self.context.socket(zmq.PAIR)
        self._ctrl_send.connect(ctrl_endpoint)

        self._stop_lock = threading.Lock()
        self._stopped = False
        self._thread = threading.Thread(
            target=self._service_loop, name="commlink-publisher", daemon=True
        )
        self._thread.start()

    def publish(self, topic: str, data: Any):
        if " " in topic:
            raise ValueError("topic cannot contain spaces")
        frames = self._serializer.serialize(topic, data)
        version = time.time_ns()
        version_bytes = version.to_bytes(8, "big")
        with self._lock:
            self._cache[topic] = (version, frames)
            for identity, topic_filter in self._push_subs.items():
                if topic_filter is not None and topic not in topic_filter:
                    continue
                try:
                    # frames[0] is topic_bytes from the serializer; no need to
                    # send the topic separately.
                    self.socket.send_multipart(
                        [identity, OP_PUSH, version_bytes, *frames],
                        flags=zmq.NOBLOCK,
                    )
                except zmq.Again:
                    # Subscriber's queue full at the ROUTER. Drop silently;
                    # buffer=True semantics tolerate drops under sustained
                    # subscriber-slower-than-publisher conditions.
                    pass

    def __setitem__(self, topic: str, data: Any):
        self.publish(topic, data)

    # ------------------------------------------------------------------ service thread

    def _service_loop(self):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
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
            if self.socket not in events:
                continue
            with self._lock:
                try:
                    frames = self.socket.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    continue
                except zmq.ZMQError:
                    if self._stopped:
                        return
                    raise
                self._handle_request(frames)

    def _handle_request(self, frames: list):
        """
        Frames from a DEALER-on-the-subscriber arrive as:
            [identity, opcode, ...payload]
        Reply via [identity, opcode, ...reply]. Called with self._lock held.
        """
        if len(frames) < 2:
            return
        identity = frames[0]
        opcode = frames[1]
        if opcode == OP_PULL:
            # [identity, "PULL", topic, last_version_be]
            if len(frames) < 4:
                return
            topic = frames[2].decode("utf-8")
            last_version = int.from_bytes(frames[3], "big")
            entry = self._cache.get(topic)
            if entry is None:
                self.socket.send_multipart(
                    [identity, OP_PULL, (0).to_bytes(8, "big"), b""]
                )
                return
            version, payload_frames = entry
            if version <= last_version:
                self.socket.send_multipart(
                    [identity, OP_PULL, version.to_bytes(8, "big"), b""]
                )
            else:
                self.socket.send_multipart(
                    [identity, OP_PULL, version.to_bytes(8, "big"), *payload_frames]
                )
        elif opcode == OP_SUB:
            if len(frames) < 3:
                return
            filter_bytes = frames[2]
            if not filter_bytes:
                topic_filter: Optional[set[str]] = None
            else:
                topic_filter = set(
                    t.decode("utf-8") for t in filter_bytes.split(b",") if t
                )
            self._push_subs[identity] = topic_filter
            self.socket.send_multipart([identity, OP_SUB, OP_OK])

    # ------------------------------------------------------------------ shutdown

    def stop(self):
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            try:
                self._ctrl_send.send(b"x")
            except zmq.ZMQError:
                pass
            if threading.current_thread() is not self._thread:
                self._thread.join(timeout=2.0)
            for s in (self.socket, self._ctrl_send, self._ctrl_recv):
                try:
                    s.close(linger=0)
                except zmq.ZMQError:
                    pass

    def __del__(self):
        try:
            self.stop()
        except Exception:
            pass


if __name__ == "__main__":
    import numpy as np

    pub = Publisher("*", port=1234)
    while True:
        pub["test"] = np.random.rand(100, 100)
        time.sleep(0.033)
