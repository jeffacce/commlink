"""
Integration tests for the compression option on Serializer, Publisher/Subscriber,
and RPCServer/RPCClient.
"""
import socket
import time

import numpy as np
import pytest
import zmq

from commlink import Publisher, RPCClient, RPCServer, Subscriber
from commlink.serializer import (
    CODEC_LZ4,
    CODEC_ZSTD,
    PICKLE_PROTO_MARKER,
    Serializer,
)


CODECS = [None, "zstd", "lz4"]


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def set_receive_timeouts(sub: Subscriber, timeout_ms: int = 1500) -> None:
    sub._global_socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    for s in sub._topic_sockets.values():
        s.setsockopt(zmq.RCVTIMEO, timeout_ms)


# -------------------- Serializer-level --------------------

@pytest.mark.parametrize("codec", CODECS)
def test_serializer_roundtrip_dict(codec):
    ser = Serializer(compression=codec)
    data = {"a": 1, "b": "hello", "c": [1, 2, 3]}
    frames = ser.serialize("topic", data)
    topic, recovered = ser.deserialize(frames)
    assert topic == "topic"
    assert recovered == data


@pytest.mark.parametrize("codec", CODECS)
def test_serializer_roundtrip_numpy(codec):
    ser = Serializer(compression=codec)
    img = np.random.randint(0, 255, (240, 320, 3), dtype=np.uint8)
    topic_in = "image"
    frames = ser.serialize(topic_in, {"img": img})
    topic, recovered = ser.deserialize(frames)
    assert topic == topic_in
    assert np.array_equal(recovered["img"], img)


@pytest.mark.parametrize("codec", ["zstd", "lz4"])
def test_compressed_frames_carry_codec_tag(codec):
    """The first byte of the main and buffer frames must be the codec tag."""
    ser = Serializer(compression=codec)
    img = np.zeros((128, 128, 3), dtype=np.uint8)  # zero-filled => high compression ratio
    frames = ser.serialize("t", {"img": img})

    expected_tag = {"zstd": CODEC_ZSTD, "lz4": CODEC_LZ4}[codec]
    assert frames[1][0] == expected_tag
    for buf in frames[2:]:
        assert buf[0] == expected_tag


def test_uncompressed_frames_start_with_pickle_marker():
    ser = Serializer()
    frames = ser.serialize("t", {"x": 42})
    assert frames[1][0] == PICKLE_PROTO_MARKER


@pytest.mark.parametrize("send_codec", CODECS)
@pytest.mark.parametrize("recv_codec", CODECS)
def test_cross_codec_auto_detect(send_codec, recv_codec):
    """A receiver should decode any codec, regardless of how it was configured."""
    sender = Serializer(compression=send_codec)
    receiver = Serializer(compression=recv_codec)
    payload = {"img": np.random.randn(64, 64).astype(np.float32), "n": 7}
    frames = sender.serialize("topic", payload)
    topic, out = receiver.deserialize(frames)
    assert topic == "topic"
    assert out["n"] == 7
    assert np.array_equal(out["img"], payload["img"])


def test_compressed_payload_smaller_for_compressible_data():
    """A zero-filled buffer should compress dramatically with zstd."""
    plain = Serializer()
    compressed = Serializer(compression="zstd")
    img = np.zeros((720, 1280, 3), dtype=np.uint8)
    plain_frames = plain.serialize("t", {"img": img})
    comp_frames = compressed.serialize("t", {"img": img})

    def frame_len(f):
        # Default frames may contain pickle.PickleBuffer (memoryview-like) objects.
        if isinstance(f, (bytes, bytearray)):
            return len(f)
        return memoryview(f).nbytes

    plain_size = sum(frame_len(f) for f in plain_frames)
    comp_size = sum(frame_len(f) for f in comp_frames)
    # Should be at least 10x smaller for an all-zeros image.
    assert comp_size * 10 < plain_size, (plain_size, comp_size)


def test_legacy_and_compression_are_mutually_exclusive():
    with pytest.raises(ValueError):
        Serializer(compression="zstd", legacy=True)


def test_unknown_codec_rejected():
    with pytest.raises(ValueError):
        Serializer(compression="snappy")


# -------------------- Publisher / Subscriber --------------------

@pytest.mark.parametrize("codec", CODECS)
def test_pubsub_roundtrip(codec):
    port = get_free_port()
    pub = Publisher("*", port=port, compression=codec)
    sub = Subscriber("127.0.0.1", port=port, topics=["frame"], buffer=False, compression=codec)
    set_receive_timeouts(sub)

    time.sleep(0.05)
    img = np.random.randint(0, 255, (180, 320, 3), dtype=np.uint8)
    pub["frame"] = {"img": img, "ts": 1.0}
    time.sleep(0.05)

    out = sub["frame"]
    assert np.array_equal(out["img"], img)
    assert out["ts"] == 1.0
    sub.stop()


def test_queue_size_options_applied():
    """Verify queue_size translates to the underlying ZMQ_SNDHWM / ZMQ_RCVHWM."""
    port = get_free_port()
    pub = Publisher("*", port=port, queue_size=2)
    sub = Subscriber("127.0.0.1", port=port, topics=["x"], buffer=True, queue_size=4)
    try:
        assert pub.socket.getsockopt(zmq.SNDHWM) == 2
        assert sub._global_socket.getsockopt(zmq.RCVHWM) == 4
        assert sub._topic_sockets["x"].getsockopt(zmq.RCVHWM) == 4
    finally:
        sub.stop()


def test_queue_size_drops_stale_frames():
    """With queue_size=1 + buffer=False, a burst of 50 should drop most frames."""
    port = get_free_port()
    pub = Publisher("*", port=port, queue_size=1)
    sub = Subscriber("127.0.0.1", port=port, topics=["x"], buffer=False, queue_size=1)
    set_receive_timeouts(sub)
    time.sleep(0.05)

    # Publish a burst; with HWM=1 most are dropped on the wire.
    for i in range(50):
        pub.publish("x", i)
    time.sleep(0.1)

    # buffer=False conflates by draining; we expect to see *some* value <= 49,
    # but typically the latest few only -- never all 50.
    received = []
    while True:
        try:
            received.append(sub._topic_sockets["x"].recv_multipart(flags=zmq.NOBLOCK))
        except zmq.Again:
            break
    sub.stop()

    assert len(received) < 50, f"queue_size=1 should drop frames; got all {len(received)}"


@pytest.mark.parametrize("send_codec", CODECS)
@pytest.mark.parametrize("recv_codec", CODECS)
def test_pubsub_cross_codec(send_codec, recv_codec):
    """Subscribers should auto-decode regardless of their compression setting."""
    port = get_free_port()
    pub = Publisher("*", port=port, compression=send_codec)
    sub = Subscriber(
        "127.0.0.1", port=port, topics=["frame"], buffer=False, compression=recv_codec
    )
    set_receive_timeouts(sub)

    time.sleep(0.05)
    payload = {"arr": np.arange(1000, dtype=np.int32), "label": "ok"}
    pub["frame"] = payload
    time.sleep(0.05)

    out = sub["frame"]
    assert np.array_equal(out["arr"], payload["arr"])
    assert out["label"] == "ok"
    sub.stop()


# -------------------- RPC --------------------

class _Echo:
    def __init__(self):
        self.value = 0

    def echo(self, x):
        return x

    def big_image(self, h, w):
        return np.random.randint(0, 255, (h, w, 3), dtype=np.uint8)


@pytest.mark.parametrize("codec", CODECS)
def test_rpc_roundtrip(codec):
    port = get_free_port()
    server = RPCServer(_Echo(), port=port, compression=codec)
    server.start()
    try:
        client = RPCClient("127.0.0.1", port=port, compression=codec)
        assert client.echo({"k": "v"}) == {"k": "v"}
        img = client.big_image(120, 160)
        assert img.shape == (120, 160, 3)
        client.value = 42
        assert client.value == 42
        client.stop_server()
    finally:
        # If stop_server failed, ensure the server thread doesn't leak.
        if server.thread is not None and server.thread.is_alive():
            server.stop()


@pytest.mark.parametrize("client_codec", CODECS)
@pytest.mark.parametrize("server_codec", CODECS)
def test_rpc_cross_codec(client_codec, server_codec):
    """The two ends may run with different codecs and still talk to each other."""
    port = get_free_port()
    server = RPCServer(_Echo(), port=port, compression=server_codec)
    server.start()
    try:
        client = RPCClient("127.0.0.1", port=port, compression=client_codec)
        out = client.echo({"img": np.ones((32, 32), dtype=np.float32)})
        assert np.array_equal(out["img"], np.ones((32, 32), dtype=np.float32))
        client.stop_server()
    finally:
        if server.thread is not None and server.thread.is_alive():
            server.stop()
