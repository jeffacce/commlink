import socket
import time

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
    subscriber = Subscriber("127.0.0.1", port=port, topics=["alpha", "beta"], keep_old=False)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("alpha", "first-alpha")
    publisher.publish("beta", "first-beta")
    publisher.publish("alpha", "second-alpha")
    publisher.publish("beta", "second-beta")
    time.sleep(0.05)

    topic_a, data_a = subscriber.get("alpha")
    assert topic_a == "alpha"
    assert data_a == "second-alpha"

    topic_b, data_b = subscriber.get("beta")
    assert topic_b == "beta"
    assert data_b == "second-beta"

    subscriber.stop()


def test_global_get_receives_messages_from_all_topics():
    port = get_free_port()
    publisher = Publisher("*", port=port)
    subscriber = Subscriber("127.0.0.1", port=port, topics=["one", "two"], keep_old=True)
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
    subscriber = Subscriber("127.0.0.1", port=port, topics=["red", "blue"], keep_old=False)
    set_receive_timeouts(subscriber)

    time.sleep(0.05)
    publisher.publish("blue", "other")
    publisher.publish("red", "stale")
    publisher.publish("red", "fresh")
    time.sleep(0.05)

    topic, data = subscriber["red"]
    assert topic == "red"
    assert data == "fresh"

    subscriber.stop()
