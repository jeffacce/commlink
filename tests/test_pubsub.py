import socket
import threading
import time

import pytest

from commlink.publisher import Publisher
from commlink.subscriber import Subscriber


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# ---------------------------------------------------------------- pull mode (buffer=False)


def test_pull_returns_latest_value():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        pub.publish("alpha", "first")
        pub.publish("alpha", "second")
        time.sleep(0.05)
        assert sub["alpha"] == "second"
        assert sub.get("alpha") == "second"
    finally:
        sub.stop()
        pub.stop()


def test_pull_independent_per_topic():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        pub.publish("red", "R")
        pub.publish("blue", "B")
        time.sleep(0.05)
        assert sub["red"] == "R"
        assert sub["blue"] == "B"
    finally:
        sub.stop()
        pub.stop()


def test_pull_first_call_blocks_until_publish():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        result = {}

        def reader():
            result["v"] = sub["late"]

        t = threading.Thread(target=reader)
        t.start()
        # Reader should be blocked since no one has published "late" yet.
        t.join(timeout=0.2)
        assert t.is_alive(), "expected reader to block on unknown topic"
        pub.publish("late", "arrived")
        t.join(timeout=2.0)
        assert not t.is_alive()
        assert result["v"] == "arrived"
    finally:
        sub.stop()
        pub.stop()


def test_pull_returns_cache_when_no_new_publish():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        pub.publish("k", "v1")
        time.sleep(0.05)
        assert sub["k"] == "v1"
        # No new publish: still returns cached, doesn't block.
        assert sub["k"] == "v1"
        assert sub["k"] == "v1"
    finally:
        sub.stop()
        pub.stop()


def test_pull_get_without_topic_raises():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        with pytest.raises(TypeError):
            sub.get()
    finally:
        sub.stop()
        pub.stop()


def test_pull_setitem_publishes():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        pub["greet"] = "hello"
        time.sleep(0.05)
        assert sub["greet"] == "hello"
    finally:
        sub.stop()
        pub.stop()


def test_pull_survives_publisher_restart():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        pub.publish("x", "before")
        time.sleep(0.05)
        assert sub["x"] == "before"

        pub.stop()
        time.sleep(0.05)
        pub = Publisher("*", port=port)
        time.sleep(0.05)
        pub.publish("x", "after")
        time.sleep(0.05)
        assert sub["x"] == "after"
    finally:
        sub.stop()
        pub.stop()


# ---------------------------------------------------------------- push mode (buffer=True)


def test_push_global_get_arrival_order():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port, buffer=True)
    try:
        time.sleep(0.05)
        pub.publish("one", 1)
        pub.publish("two", 2)
        topics = sorted([sub.get()[0], sub.get()[0]])
        assert topics == ["one", "two"]
    finally:
        sub.stop()
        pub.stop()


def test_push_per_topic_order():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port, buffer=True)
    try:
        time.sleep(0.05)
        pub.publish("seq", 1)
        pub.publish("seq", 2)
        pub.publish("seq", 3)
        assert sub["seq"] == 1
        assert sub["seq"] == 2
        assert sub["seq"] == 3
    finally:
        sub.stop()
        pub.stop()


def test_push_filter_skips_other_topics():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port, topics=["wanted"], buffer=True)
    try:
        time.sleep(0.05)
        pub.publish("ignored", "x")
        pub.publish("wanted", "y")
        pub.publish("ignored", "z")
        topic, data = sub.get()
        assert topic == "wanted"
        assert data == "y"
    finally:
        sub.stop()
        pub.stop()


def test_push_drops_when_queue_full():
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port, buffer=True, queue_size=3)
    try:
        time.sleep(0.05)
        for i in range(20):
            pub.publish("flood", i)
        time.sleep(0.1)
        # Per-topic deque is bounded; collected items can't exceed queue_size.
        collected = [sub["flood"] for _ in range(3)]
        assert all(isinstance(v, int) for v in collected)
        assert len(collected) == 3

        # A 4th get should block since the deque only holds 3 and nothing new
        # has been published. Verify by polling in a thread with a deadline.
        done = threading.Event()

        def reader():
            try:
                sub["flood"]
            except Exception:
                pass
            done.set()

        t = threading.Thread(target=reader, daemon=True)
        t.start()
        # Expect it to still be blocked after a short wait.
        assert not done.wait(timeout=0.2), "expected get() to block on empty deque"
    finally:
        sub.stop()
        pub.stop()


# ---------------------------------------------------------------- validation


def test_topic_with_space_rejected_publisher():
    port = get_free_port()
    pub = Publisher("*", port=port)
    try:
        with pytest.raises(ValueError):
            pub.publish("bad topic", "x")
    finally:
        pub.stop()


def test_topic_with_space_rejected_subscriber_filter():
    port = get_free_port()
    with pytest.raises(ValueError):
        Subscriber("127.0.0.1", port=port, topics=["bad topic"], buffer=True)


def test_topics_str_rejected():
    port = get_free_port()
    with pytest.raises(TypeError):
        Subscriber("127.0.0.1", port=port, topics="solo")


def test_compression_kwarg_deprecated():
    port = get_free_port()
    pub = Publisher("*", port=port)
    try:
        with pytest.warns(DeprecationWarning):
            sub = Subscriber("127.0.0.1", port=port, compression="zstd")
        sub.stop()
    finally:
        pub.stop()


# ---------------------------------------------------------------- pull cost / freshness


def test_pull_no_change_reply_is_cheap():
    """Hammering sub['k'] when nothing new is published should keep returning the cached
    value without ever appearing to stall, even at high call rate."""
    port = get_free_port()
    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port)
    try:
        time.sleep(0.05)
        pub.publish("k", "value")
        time.sleep(0.05)
        assert sub["k"] == "value"
        t0 = time.monotonic()
        for _ in range(200):
            assert sub["k"] == "value"
        elapsed = time.monotonic() - t0
        # 200 round-trip PULLs on loopback should comfortably finish under a second.
        assert elapsed < 2.0, f"200 no-change pulls took {elapsed:.2f}s"
    finally:
        sub.stop()
        pub.stop()
