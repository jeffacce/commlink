"""
Benchmark compression on Commlink pub/sub for streaming workloads.

Measures, per (payload, codec):

  * Wire size  -- bytes on the wire per message (post-compression)
  * Ser ms     -- median serialize time (compress + pickle)
  * Deser ms   -- median deserialize time (decompress + unpickle)
  * Latency    -- end-to-end pub -> sub time, median + p95
  * Jitter     -- standard deviation of latency
  * Bandwidth  -- effective MB/s of *uncompressed* payload delivered

Run with:
    python benchmarks/benchmark_compression.py
    python benchmarks/benchmark_compression.py --iters 200 --codecs none zstd lz4
"""
from __future__ import annotations

import argparse
import gc
import socket
import statistics
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import zmq

from commlink import Publisher, Subscriber
from commlink.serializer import Serializer

CODECS = [None, "zstd", "lz4"]
CODEC_LABELS = {None: "none", "zstd": "zstd", "lz4": "lz4"}


# ---------------------- payloads ----------------------


def _frame_image(rng: np.random.Generator, h: int, w: int) -> np.ndarray:
    """
    Build a 720p-ish RGB image that resembles a real camera frame: smooth
    gradient + low-frequency texture + a small amount of noise. This compresses
    much better than uniform random noise (which is the worst case for any
    general-purpose codec) while still being non-trivial.
    """
    yy, xx = np.meshgrid(np.linspace(0, 1, h, dtype=np.float32),
                         np.linspace(0, 1, w, dtype=np.float32),
                         indexing="ij")
    base = np.stack([
        128 + 80 * np.sin(6 * xx + 1.0),
        128 + 80 * np.sin(4 * yy + 2.0),
        128 + 80 * np.sin(3 * (xx + yy)),
    ], axis=-1)
    noise = rng.integers(-12, 13, size=(h, w, 3), dtype=np.int16)
    return np.clip(base + noise, 0, 255).astype(np.uint8)


def _low_rank_image(rng: np.random.Generator, h: int, w: int, rank: int) -> np.ndarray:
    """
    Build an RGB image whose per-channel matrix has SVD rank `rank`. With
    rank << min(h, w), the matrix has a lot of structure (entries are linear
    combinations of `rank` basis vectors), so a general-purpose entropy coder
    should compress it well -- but the bytes are not literally repeated like a
    zero-fill would be, making it a realistic mid-difficulty test case.

    Strategy:
      * Sample U (h, rank) and V (w, rank) from a normal distribution.
      * Compute the rank-`rank` outer product U @ V.T per channel.
      * Rescale each channel to [0, 255] and quantize to uint8. The quantization
        slightly perturbs the rank, but the data still concentrates in a small
        subspace.
    """
    img = np.empty((h, w, 3), dtype=np.uint8)
    for c in range(3):
        U = rng.standard_normal((h, rank)).astype(np.float32)
        V = rng.standard_normal((w, rank)).astype(np.float32)
        m = U @ V.T  # rank `rank`
        m -= m.min()
        peak = m.max()
        if peak > 0:
            m *= 255.0 / peak
        img[..., c] = m.astype(np.uint8)
    return img


def build_payloads(seed: int = 0) -> Dict[str, Any]:
    rng = np.random.default_rng(seed)
    payloads: Dict[str, Any] = {}

    # 720p RGB camera frame.
    payloads["rgb_720p"] = _frame_image(rng, 720, 1280)

    # 720p depth frame as float16 (smooth surface + noise).
    yy, xx = np.meshgrid(np.linspace(0, 1, 720, dtype=np.float32),
                         np.linspace(0, 1, 1280, dtype=np.float32),
                         indexing="ij")
    depth = (1.5 + 0.4 * np.sin(8 * xx) * np.cos(6 * yy)
             + rng.standard_normal((720, 1280)).astype(np.float32) * 0.01)
    payloads["depth_720p_f16"] = depth.astype(np.float16)

    # Worst case: uniform random uint8, 720p (poorly compressible; codecs may add overhead).
    payloads["rgb_720p_random"] = rng.integers(0, 256, (720, 1280, 3), dtype=np.uint8)

    # Realistically compressible: 720p RGB with a low-rank structure. Each channel
    # is a rank-8 matrix, so the data lives in a 8-dimensional subspace per channel
    # and a good entropy coder should see meaningful redundancy.
    payloads["rgb_720p_lowrank8"] = _low_rank_image(rng, 720, 1280, rank=8)
    payloads["rgb_720p_lowrank32"] = _low_rank_image(rng, 720, 1280, rank=32)

    # Tiny pose (small messages dominated by overhead, not payload).
    payloads["pose_4x4"] = np.eye(4, dtype=np.float64)

    # Mixed point-cloud dict (xyz float32 + rgb uint8).
    n = 100_000
    payloads["point_cloud_100k"] = {
        "xyz": rng.standard_normal((n, 3)).astype(np.float32),
        "rgb": rng.integers(0, 256, (n, 3), dtype=np.uint8),
    }
    return payloads


# ---------------------- helpers ----------------------


@dataclass
class SerStats:
    wire_bytes: int
    ser_ms: float
    deser_ms: float


@dataclass
class StreamStats:
    latency_med_ms: float
    latency_p95_ms: float
    jitter_ms: float
    bandwidth_mb_s: float
    msgs_received: int


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def frame_len(frame) -> int:
    if isinstance(frame, (bytes, bytearray)):
        return len(frame)
    return memoryview(frame).nbytes


def payload_uncompressed_bytes(data: Any) -> int:
    """Reference 'raw' size of payload (sum of array nbytes for nested structures)."""
    if isinstance(data, np.ndarray):
        return int(data.nbytes)
    if isinstance(data, dict):
        return sum(payload_uncompressed_bytes(v) for v in data.values())
    if isinstance(data, (list, tuple)):
        return sum(payload_uncompressed_bytes(v) for v in data)
    return sys.getsizeof(data)


def median(xs: List[float]) -> float:
    return statistics.median(xs) if xs else float("nan")


def stdev(xs: List[float]) -> float:
    return statistics.stdev(xs) if len(xs) >= 2 else 0.0


def percentile(xs: List[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs_sorted = sorted(xs)
    k = (len(xs_sorted) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(xs_sorted) - 1)
    return xs_sorted[lo] + (xs_sorted[hi] - xs_sorted[lo]) * (k - lo)


# ---------------------- serializer-only timing ----------------------


def measure_serialize(
    payload: Any,
    codec: Optional[str],
    iters: int,
) -> SerStats:
    ser = Serializer(compression=codec)
    # Warmup
    for _ in range(3):
        frames = ser.serialize("t", payload)
        ser.deserialize([f if isinstance(f, (bytes, bytearray)) else bytes(memoryview(f))
                         for f in frames])

    ser_times: List[float] = []
    deser_times: List[float] = []
    wire_bytes_sample = 0

    for i in range(iters):
        gc.collect()
        t0 = time.perf_counter()
        frames = ser.serialize("t", payload)
        t1 = time.perf_counter()
        ser_times.append((t1 - t0) * 1000)

        # ZMQ would copy the buffers across the socket; mimic that for a fair deserialize timing.
        wire_frames = [f if isinstance(f, (bytes, bytearray)) else bytes(memoryview(f))
                       for f in frames]
        if i == 0:
            wire_bytes_sample = sum(len(f) for f in wire_frames)

        t0 = time.perf_counter()
        ser.deserialize(wire_frames)
        t1 = time.perf_counter()
        deser_times.append((t1 - t0) * 1000)

    return SerStats(
        wire_bytes=wire_bytes_sample,
        ser_ms=median(ser_times),
        deser_ms=median(deser_times),
    )


# ---------------------- end-to-end pub/sub timing ----------------------


def measure_stream(
    payload: Any,
    codec: Optional[str],
    iters: int,
    warmup: int = 5,
    queue_size: Optional[int] = None,
    publish_fps: Optional[float] = None,
) -> StreamStats:
    port = get_free_port()
    pub = Publisher("127.0.0.1", port=port, compression=codec, queue_size=queue_size)
    sub = Subscriber("127.0.0.1", port=port, topics=["bench"], buffer=True,
                     compression=codec, queue_size=queue_size)
    sub._global_socket.setsockopt(zmq.RCVTIMEO, 5000)
    for s in sub._topic_sockets.values():
        s.setsockopt(zmq.RCVTIMEO, 5000)

    # Give PUB/SUB time to connect; otherwise early messages are dropped silently.
    time.sleep(0.3)

    latencies_ms: List[float] = []
    raw_bytes = payload_uncompressed_bytes(payload)
    received = 0

    received_lock = threading.Lock()
    stop = threading.Event()

    def receiver():
        nonlocal received
        while not stop.is_set():
            try:
                frames = sub._topic_sockets["bench"].recv_multipart()
            except zmq.error.Again:
                continue
            except zmq.error.ZMQError:
                break
            t_recv = time.perf_counter()
            _, msg = sub._serializer.deserialize(frames)
            t_send = msg["t"]
            with received_lock:
                latencies_ms.append((t_recv - t_send) * 1000)
                received += 1

    t = threading.Thread(target=receiver, daemon=True)
    t.start()

    # Warmup messages (drop their latencies after capture).
    for _ in range(warmup):
        pub.publish("bench", {"t": time.perf_counter(), "data": payload})
        time.sleep(0.005)
    # Drain warmup latencies.
    deadline = time.time() + 1.0
    while time.time() < deadline:
        with received_lock:
            if received >= warmup:
                latencies_ms.clear()
                received = 0
                break
        time.sleep(0.005)

    # Measurement.
    send_period = 1.0 / publish_fps if publish_fps else 0.0
    t_start = time.perf_counter()
    next_t = time.perf_counter()
    for _ in range(iters):
        if send_period:
            now = time.perf_counter()
            if now < next_t:
                time.sleep(next_t - now)
            next_t += send_period
        pub.publish("bench", {"t": time.perf_counter(), "data": payload})

    # Wait for the receiver to drain. With small HWM many messages are dropped, so
    # we can't wait for all `iters` to arrive -- instead poll until receive count
    # has stabilized (no new arrivals for `quiet` seconds) or deadline hits.
    quiet = 0.3
    deadline = time.time() + 10.0
    last_count = -1
    last_change = time.time()
    while time.time() < deadline:
        with received_lock:
            count = received
        if count >= iters:
            break
        if count != last_count:
            last_count = count
            last_change = time.time()
        elif time.time() - last_change > quiet:
            break
        time.sleep(0.01)
    t_end = time.perf_counter()

    stop.set()
    sub.stop()
    t.join(timeout=1.0)

    elapsed = max(t_end - t_start, 1e-9)
    bandwidth = (raw_bytes * received) / elapsed / (1024 * 1024)

    return StreamStats(
        latency_med_ms=median(latencies_ms),
        latency_p95_ms=percentile(latencies_ms, 0.95),
        jitter_ms=stdev(latencies_ms),
        bandwidth_mb_s=bandwidth,
        msgs_received=received,
    )


# ---------------------- main ----------------------


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:7.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def run(payloads: Dict[str, Any], codecs: List[Optional[str]], iters: int,
        queue_size: Optional[int] = None, publish_fps: Optional[float] = None) -> None:
    raw_sizes = {name: payload_uncompressed_bytes(p) for name, p in payloads.items()}

    print("=" * 96)
    print("Serialize / deserialize (single-process, no socket)")
    print("=" * 96)
    print(f"{'payload':<22} {'codec':<6} {'wire':>11} {'ratio':>7} "
          f"{'ser(ms)':>9} {'deser(ms)':>10}")
    print("-" * 96)
    for name, data in payloads.items():
        raw = raw_sizes[name]
        for codec in codecs:
            stats = measure_serialize(data, codec, iters=max(20, iters // 2))
            ratio = stats.wire_bytes / raw if raw else float("nan")
            print(f"{name:<22} {CODEC_LABELS[codec]:<6} {fmt_bytes(stats.wire_bytes):>11} "
                  f"{ratio:>6.2f}x {stats.ser_ms:>9.2f} {stats.deser_ms:>10.2f}")
        print()

    qs_label = "ZMQ default (1000)" if queue_size is None else str(queue_size)
    fps_label = f"{publish_fps:g} fps" if publish_fps else "unthrottled"
    print("=" * 96)
    print(f"End-to-end pub/sub streaming  (iters={iters} per cell, "
          f"queue_size={qs_label}, publish={fps_label})")
    print("=" * 96)
    print(f"{'payload':<22} {'codec':<6} {'lat med':>9} {'lat p95':>9} "
          f"{'jitter':>8} {'bw MB/s':>9} {'recv':>5} {'drop%':>6}")
    print("-" * 96)
    for name, data in payloads.items():
        for codec in codecs:
            stats = measure_stream(data, codec, iters=iters, queue_size=queue_size,
                                   publish_fps=publish_fps)
            drop_pct = 100.0 * (1.0 - stats.msgs_received / iters) if iters else 0.0
            print(f"{name:<22} {CODEC_LABELS[codec]:<6} "
                  f"{stats.latency_med_ms:>8.2f}  "
                  f"{stats.latency_p95_ms:>8.2f}  "
                  f"{stats.jitter_ms:>7.2f}  "
                  f"{stats.bandwidth_mb_s:>8.1f}  "
                  f"{stats.msgs_received:>5} "
                  f"{drop_pct:>5.1f}%")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iters", type=int, default=100,
                        help="Iterations per (payload, codec) cell. Default: 100.")
    parser.add_argument("--codecs", nargs="+", default=["none", "zstd", "lz4"],
                        choices=["none", "zstd", "lz4"],
                        help="Subset of codecs to benchmark.")
    parser.add_argument("--payloads", nargs="+", default=None,
                        help="Subset of payload names to benchmark.")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--queue-size", type=int, default=None,
                        help="Max pending messages on both publisher and subscriber. "
                             "Default uses ZMQ's built-in (1000). Use a small value "
                             "(e.g. 5-10) for streaming where stale frames should be "
                             "dropped before they pile up.")
    parser.add_argument("--fps", type=float, default=None,
                        help="Throttle publishing rate (e.g. 30 for a 30fps camera). "
                             "Default: unthrottled (back-to-back, worst case).")
    args = parser.parse_args()

    codec_map = {"none": None, "zstd": "zstd", "lz4": "lz4"}
    codecs = [codec_map[c] for c in args.codecs]

    all_payloads = build_payloads(seed=args.seed)
    if args.payloads:
        payloads = {k: all_payloads[k] for k in args.payloads if k in all_payloads}
        missing = set(args.payloads) - set(all_payloads)
        if missing:
            print(f"warning: unknown payloads ignored: {sorted(missing)}", file=sys.stderr)
    else:
        payloads = all_payloads

    run(payloads, codecs, args.iters, queue_size=args.queue_size,
        publish_fps=args.fps)


if __name__ == "__main__":
    main()
