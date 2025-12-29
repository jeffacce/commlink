import socket
import time

import pytest
import zmq

from commlink.publisher import Publisher
from commlink.subscriber import Subscriber


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def set_receive_timeouts(subscriber: Subscriber, timeout_ms: int = 1000) -> None:
    subscriber._global_socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    for socket in subscriber._topic_sockets.values():
        socket.setsockopt(zmq.RCVTIMEO, timeout_ms)


def test_multi_topic_specific_sockets_keep_latest_message():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["alpha", "beta"], buffer=False)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("alpha", "first-alpha")
    publisher.publish("beta", "first-beta")
    publisher.publish("alpha", "second-alpha")
    publisher.publish("beta", "second-beta")
    time.sleep(0.05)

    data_a = subscriber.get("alpha")
    assert data_a == "second-alpha"

    data_b = subscriber.get("beta")
    assert data_b == "second-beta"

    subscriber.stop()


def test_global_get_receives_messages_from_all_topics():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["one", "two"], buffer=True)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("one", 1)
    publisher.publish("two", 2)

    received_topics = []
    for _ in range(2):
        topic, data = subscriber.get()
        received_topics.append(topic)
        assert data in (1, 2)

    assert set(received_topics) == {"one", "two"}

    subscriber.stop()


def test_getitem_reads_specific_topic_socket():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["red", "blue"], buffer=False)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("blue", "other")
    publisher.publish("red", "stale")
    publisher.publish("red", "fresh")
    time.sleep(0.05)

    data = subscriber["red"]
    assert data == "fresh"

    subscriber.stop()


def test_setitem_publishes():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["alpha"], buffer=False)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher["alpha"] = "published via setitem"
    time.sleep(0.05)

    assert subscriber["alpha"] == "published via setitem"

    subscriber.stop()


def test_get_raises_for_unsubscribed_topic():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["alpha"], buffer=False)
    set_receive_timeouts(subscriber)
    time.sleep(0.05)

    with pytest.raises(KeyError):
        subscriber.get("beta")

    publisher.publish("alpha", "value")
    time.sleep(0.05)
    assert subscriber.get("alpha") == "value"

    subscriber.stop()


def test_global_subscription_with_empty_topics_list():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=[], buffer=True)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("x", "first")
    publisher.publish("y", "second")

    received = {subscriber.get()[0], subscriber.get()[0]}
    assert received == {"x", "y"}

    subscriber.stop()


def test_topic_validation_rejects_spaces():
    port = get_free_port()
    with pytest.raises(ValueError):
        Subscriber("127.0.0.1", port=port, topics=["bad topic"], buffer=True)


def test_buffer_true_preserves_order_on_topic_socket():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["seq"], buffer=True)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("seq", 1)
    publisher.publish("seq", 2)
    time.sleep(0.05)

    first = subscriber.get("seq")
    second = subscriber.get("seq")
    assert first == 1
    assert second == 2

    subscriber.stop()


def test_conflate_global_subscription_rejected():
    port = get_free_port()
    with pytest.warns(RuntimeWarning):
        sub_empty = Subscriber("127.0.0.1", port=port, topics=[], buffer=False)
        sub_empty.stop()


def test_topics_str_is_normalized_to_list():
    port = get_free_port()
    with pytest.raises(TypeError):
        Subscriber("127.0.0.1", port=port, topics="solo", buffer=False)
