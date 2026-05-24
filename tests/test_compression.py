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
    CODEC_NONE,
    CODEC_ZSTD,
    PICKLE_PROTO_MARKER,
    Serializer,
)


CODECS = [None, "zstd", "lz4"]


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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
    """The first byte of every frame must be the codec tag when no
    per-frame threshold filters it out."""
    ser = Serializer(compression=codec, compression_min_bytes=0)
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


def test_unknown_codec_rejected():
    with pytest.raises(ValueError):
        Serializer(compression="snappy")


# -------------------- compression_min_bytes --------------------


@pytest.mark.parametrize("codec", ["zstd", "lz4"])
def test_threshold_skips_compression_for_small_frames(codec):
    """A small payload below the threshold is emitted with CODEC_NONE, not the
    configured codec, even when compression is enabled."""
    ser = Serializer(compression=codec, compression_min_bytes=1024)
    frames = ser.serialize("joint_state", {"q": np.zeros(7, dtype=np.float32)})

    # Main pickle frame is well under 1 KB → uncompressed tag.
    assert frames[1][0] == CODEC_NONE
    # Single 28-byte out-of-band buffer → also uncompressed tag.
    assert frames[2][0] == CODEC_NONE

    topic, recovered = ser.deserialize(frames)
    assert topic == "joint_state"
    assert np.array_equal(recovered["q"], np.zeros(7, dtype=np.float32))


@pytest.mark.parametrize("codec", ["zstd", "lz4"])
def test_threshold_compresses_large_frames(codec):
    """A payload above the threshold is compressed with the configured codec."""
    ser = Serializer(compression=codec, compression_min_bytes=1024)
    img = np.zeros((256, 256, 3), dtype=np.uint8)  # ~196 KB buffer
    frames = ser.serialize("img", {"img": img})

    expected_tag = {"zstd": CODEC_ZSTD, "lz4": CODEC_LZ4}[codec]
    # Main pickle frame is tiny → uncompressed; buffer is large → compressed.
    assert frames[1][0] == CODEC_NONE
    assert frames[2][0] == expected_tag

    topic, recovered = ser.deserialize(frames)
    assert topic == "img"
    assert np.array_equal(recovered["img"], img)


def test_threshold_mixed_per_frame_within_one_message():
    """In one wire message a small main + large buffer should produce mixed tags."""
    ser = Serializer(compression="zstd", compression_min_bytes=1024)
    img = np.zeros((720, 1280, 3), dtype=np.uint8)
    tiny = np.zeros(3, dtype=np.float32)
    frames = ser.serialize("t", {"img": img, "tiny": tiny})

    assert frames[1][0] == CODEC_NONE       # main pickle ~150 B
    tags = [f[0] for f in frames[2:]]
    assert CODEC_NONE in tags                # 12-byte tiny buffer
    assert CODEC_ZSTD in tags                # 2.7 MB image buffer

    topic, recovered = ser.deserialize(frames)
    assert topic == "t"
    assert np.array_equal(recovered["img"], img)
    assert np.array_equal(recovered["tiny"], tiny)


def test_threshold_zero_always_compresses():
    """compression_min_bytes=0 disables the threshold; every frame uses the codec."""
    ser = Serializer(compression="zstd", compression_min_bytes=0)
    frames = ser.serialize("t", {"q": np.zeros(7, dtype=np.float32)})
    assert frames[1][0] == CODEC_ZSTD
    assert frames[2][0] == CODEC_ZSTD


def test_threshold_huge_never_compresses():
    """A threshold above any real frame size means nothing ever gets compressed."""
    ser = Serializer(compression="zstd", compression_min_bytes=10**9)
    img = np.zeros((256, 256, 3), dtype=np.uint8)
    frames = ser.serialize("t", {"img": img})
    for f in frames[1:]:
        assert f[0] == CODEC_NONE


def test_threshold_no_effect_without_compression():
    """compression_min_bytes is irrelevant when compression is None; the wire
    format stays the back-compat 0x80 pickle marker, no codec tag."""
    ser = Serializer(compression=None, compression_min_bytes=0)
    frames = ser.serialize("t", {"x": 1})
    assert frames[1][0] == PICKLE_PROTO_MARKER


def test_threshold_negative_rejected():
    with pytest.raises(ValueError):
        Serializer(compression="zstd", compression_min_bytes=-1)


# -------------------- Publisher / Subscriber --------------------

@pytest.mark.parametrize("codec", CODECS)
def test_pubsub_roundtrip(codec):
    """End-to-end with a publisher-side codec. The subscriber decodes
    automatically from the wire format's self-describing tag."""
    port = get_free_port()
    pub = Publisher("*", port=port, compression=codec)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        img = np.random.randint(0, 255, (180, 320, 3), dtype=np.uint8)
        pub["frame"] = {"img": img, "ts": 1.0}
        time.sleep(0.05)

        out = sub["frame"]
        assert np.array_equal(out["img"], img)
        assert out["ts"] == 1.0
    finally:
        sub.stop()
        pub.stop()


def test_queue_size_options_applied():
    """queue_size sets ZMQ_SNDHWM on the publisher ROUTER and ZMQ_RCVHWM on the
    push-mode subscriber DEALER."""
    port = get_free_port()
    pub = Publisher("*", port=port, queue_size=2)
    sub = Subscriber("127.0.0.1", port=port, buffer=True, queue_size=4)
    try:
        assert pub.socket.getsockopt(zmq.SNDHWM) == 2
        assert sub._dealer.getsockopt(zmq.RCVHWM) == 4
    finally:
        sub.stop()
        pub.stop()


@pytest.mark.parametrize("send_codec", CODECS)
def test_pubsub_decodes_any_publisher_codec(send_codec):
    """Subscribers should auto-decode regardless of the publisher's codec."""
    port = get_free_port()
    pub = Publisher("*", port=port, compression=send_codec)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        payload = {"arr": np.arange(1000, dtype=np.int32), "label": "ok"}
        pub["frame"] = payload
        time.sleep(0.05)

        out = sub["frame"]
        assert np.array_equal(out["arr"], payload["arr"])
        assert out["label"] == "ok"
    finally:
        sub.stop()
        pub.stop()


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
