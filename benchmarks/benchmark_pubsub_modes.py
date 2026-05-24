"""
Bench the redesigned pub/sub against realistic robotics traffic shapes.

Topics:
  camera : 720p RGB uint8 (~2.76 MB / msg) @ 30 Hz
  joints : 32 float32 (~128 B / msg) @ 1000 Hz

Scenarios:
  push_keepup        buffer=True,  sub drains at full pub rate
  push_slow_camera   buffer=True,  sub drains camera at 10 Hz, joints at 1000 Hz
  pull_obs_rate      buffer=False, sub pulls camera at 5 Hz, joints at 100 Hz
  pull_matched       buffer=False, sub pulls camera at 30 Hz, joints at 1000 Hz

Per-topic numbers:
  recv / drops / recvHz  : delivered count, gap count, achieved sub rate
  p50/p95/p99 latency    : (recv_time - publish_time), in ms
  MB/s                   : payload bytes received per second
  no_change (pull only)  : how often the publisher had nothing new for us
"""
import argparse
import socket
import threading
import time
from dataclasses import dataclass, field

import numpy as np

from commlink import Publisher, Subscriber


CAMERA_SHAPE = (720, 1280, 3)
JOINTS_SHAPE = (32,)


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# Pre-allocate once per topic. A real camera or proprioceptive driver hands us
# a frame; the publisher's loop shouldn't be modeling np.random.randint cost
# (which is ~4ms for a 720p uint8 frame and would swamp every other measurement
# via GIL contention).
_CAMERA_FRAME = np.random.randint(0, 255, CAMERA_SHAPE, dtype=np.uint8)
_JOINTS_FRAME = np.random.randn(*JOINTS_SHAPE).astype(np.float32)


def make_camera_frame() -> np.ndarray:
    return _CAMERA_FRAME


def make_joint_frame() -> np.ndarray:
    return _JOINTS_FRAME


@dataclass
class Stats:
    topic: str
    received: int = 0
    drops: int = 0
    no_change_pulls: int = 0     # pull mode only
    bytes_received: int = 0
    first_seq: int = -1
    last_seq: int = -1
    latencies_ns: list = field(default_factory=list)

    def record(self, seq: int, publish_ns: int, payload_nbytes: int, recv_ns: int):
        if self.last_seq < 0:
            self.first_seq = seq
        else:
            gap = seq - self.last_seq - 1
            if gap > 0:
                self.drops += gap
        self.last_seq = seq
        self.received += 1
        self.bytes_received += payload_nbytes
        self.latencies_ns.append(recv_ns - publish_ns)


def publisher_loop(pub: Publisher, topic: str, hz: float, payload_factory,
                   stop_evt: threading.Event):
    period = 1.0 / hz
    seq = 0
    next_t = time.perf_counter()
    while not stop_evt.is_set():
        payload = {
            "seq": seq,
            "publish_ns": time.perf_counter_ns(),
            "data": payload_factory(),
        }
        pub.publish(topic, payload)
        seq += 1
        next_t += period
        sleep = next_t - time.perf_counter()
        if sleep > 0:
            stop_evt.wait(timeout=sleep)
        else:
            # Behind schedule; reset so we don't try to catch up with a burst.
            next_t = time.perf_counter()


def push_consumer_loop(sub: Subscriber, topic: str, stats: Stats,
                       stop_evt: threading.Event, rate_hz):
    """Drain topic queue as fast as possible (rate_hz=None) or at rate_hz Hz."""
    period = (1.0 / rate_hz) if rate_hz else None
    next_t = time.perf_counter() if period else None
    while not stop_evt.is_set():
        try:
            msg = sub[topic]
        except Exception:
            return
        recv_ns = time.perf_counter_ns()
        stats.record(msg["seq"], msg["publish_ns"], msg["data"].nbytes, recv_ns)
        if period is not None:
            next_t += period
            sleep = next_t - time.perf_counter()
            if sleep > 0:
                stop_evt.wait(timeout=sleep)
            else:
                next_t = time.perf_counter()


def pull_consumer_loop(sub: Subscriber, topic: str, stats: Stats,
                       stop_evt: threading.Event, rate_hz: float):
    """Issue one PULL per period at rate_hz. Count duplicates (same seq returned twice)
    as no-change replies — that's what the publisher would have answered with an empty
    REP, but our subscriber surfaces it as the cached value."""
    period = 1.0 / rate_hz
    next_t = time.perf_counter()
    last_seen_seq = -1
    while not stop_evt.is_set():
        try:
            msg = sub[topic]
        except Exception:
            return
        recv_ns = time.perf_counter_ns()
        if msg["seq"] != last_seen_seq:
            stats.record(msg["seq"], msg["publish_ns"], msg["data"].nbytes, recv_ns)
            last_seen_seq = msg["seq"]
        else:
            stats.no_change_pulls += 1
        next_t += period
        sleep = next_t - time.perf_counter()
        if sleep > 0:
            stop_evt.wait(timeout=sleep)
        else:
            next_t = time.perf_counter()


def print_report(name: str, mode: str, duration: float,
                 pub_rates: dict, sub_rates: dict, stats: dict):
    print(f"\n=== {name}  (mode={mode}, duration={duration:.1f}s) ===")
    # In push mode the "gaps" column counts dropped frames — every gap is loss.
    # In pull mode every gap is an intentional skip: pull only ever returns the
    # latest cached version, so any frame published between two PULLs is skipped
    # by design. Different label keeps that distinction visible.
    gap_label = "drops" if mode == "push" else "skips"
    header = (
        f"{'topic':<8} {'pubHz':>6} {'subHz':>6} {'recv':>7} {gap_label:>6} "
        f"{'recvHz':>7} | {'p50ms':>7} {'p95ms':>7} {'p99ms':>7} | {'MB/s':>7}"
    )
    print(header)
    print("-" * len(header))
    for topic in pub_rates:
        s = stats[topic]
        if s.latencies_ns:
            lat_ms = np.array(s.latencies_ns) / 1e6
            p50, p95, p99 = np.percentile(lat_ms, [50, 95, 99])
        else:
            p50 = p95 = p99 = float("nan")
        recv_hz = s.received / duration
        mbps = s.bytes_received / duration / (1024 * 1024)
        sub_hz = sub_rates[topic] if sub_rates[topic] is not None else pub_rates[topic]
        print(
            f"{topic:<8} {pub_rates[topic]:>6} {sub_hz:>6} {s.received:>7} "
            f"{s.drops:>6} {recv_hz:>7.1f} | {p50:>7.2f} {p95:>7.2f} {p99:>7.2f} "
            f"| {mbps:>7.2f}"
        )
        if mode == "pull" and s.no_change_pulls:
            ratio = s.no_change_pulls / (s.no_change_pulls + s.received)
            print(
                f"           no-change pulls: {s.no_change_pulls} "
                f"({ratio*100:.0f}% of pulls — sub asked faster than pub published)"
            )


def run_scenario(name: str, mode: str, pub_rates: dict, sub_rates: dict,
                 duration: float):
    """
    mode: 'push' or 'pull'.
    pub_rates: {'camera': 30, 'joints': 1000}
    sub_rates: {'camera': hz_or_None, 'joints': hz_or_None}.
               For push, None means "drain as fast as possible".
               For pull, must be a number.
    """
    port = get_free_port()
    stop_evt = threading.Event()
    factories = {"camera": make_camera_frame, "joints": make_joint_frame}

    pub = Publisher("*", port=port)
    sub = Subscriber("127.0.0.1", port=port, buffer=(mode == "push"))
    # Give the SUB registration / DEALER connect time to flush.
    time.sleep(0.2)

    stats = {t: Stats(t) for t in pub_rates}

    pub_threads = []
    for topic, hz in pub_rates.items():
        t = threading.Thread(
            target=publisher_loop,
            args=(pub, topic, hz, factories[topic], stop_evt),
            daemon=True,
            name=f"pub-{topic}",
        )
        pub_threads.append(t)

    sub_threads = []
    consumer = push_consumer_loop if mode == "push" else pull_consumer_loop
    for topic in pub_rates:
        t = threading.Thread(
            target=consumer,
            args=(sub, topic, stats[topic], stop_evt, sub_rates[topic]),
            daemon=True,
            name=f"sub-{topic}",
        )
        sub_threads.append(t)

    for t in pub_threads:
        t.start()
    for t in sub_threads:
        t.start()

    t0 = time.perf_counter()
    time.sleep(duration)
    elapsed = time.perf_counter() - t0
    stop_evt.set()

    # Tear down subscriber first so any consumer blocked in sub[topic] gets
    # bumped out (either Condition.wait wakes via stop, or DEALER close raises).
    for t in pub_threads:
        t.join(timeout=2.0)
    sub.stop()
    for t in sub_threads:
        t.join(timeout=2.0)
    pub.stop()

    print_report(name, mode, elapsed, pub_rates, sub_rates, stats)


SCENARIOS = [
    # (name, mode, pub_rates, sub_rates)
    (
        "push_keepup",
        "push",
        {"camera": 30, "joints": 1000},
        {"camera": None, "joints": None},   # drain as fast as possible
    ),
    (
        "push_slow_camera",
        "push",
        {"camera": 30, "joints": 1000},
        {"camera": 10, "joints": None},     # camera consumer is slower than pub
    ),
    (
        "pull_obs_rate",
        "pull",
        {"camera": 30, "joints": 1000},
        {"camera": 5, "joints": 100},
    ),
    (
        "pull_matched",
        "pull",
        {"camera": 30, "joints": 1000},
        {"camera": 30, "joints": 1000},
    ),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=5.0,
                        help="seconds per scenario")
    parser.add_argument("--only", default=None,
                        help="comma-separated scenario names to run")
    args = parser.parse_args()

    selected = set(args.only.split(",")) if args.only else None
    print(f"camera frame: {CAMERA_SHAPE} uint8 = "
          f"{np.prod(CAMERA_SHAPE)/1024/1024:.2f} MB")
    print(f"joints frame: {JOINTS_SHAPE} float32 = "
          f"{np.prod(JOINTS_SHAPE)*4} B")
    for name, mode, pub_rates, sub_rates in SCENARIOS:
        if selected and name not in selected:
            continue
        run_scenario(name, mode, pub_rates, sub_rates, args.duration)


if __name__ == "__main__":
    main()
