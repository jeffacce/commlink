import pickle
from typing import Any, Callable, List, Optional, Tuple

# Codec tag bytes (first byte of main / buffer frames in the new wire format).
# Pickle protocol >= 2 streams begin with 0x80, so any leading byte that is NOT
# 0x80 unambiguously identifies a tagged-and-possibly-compressed frame.
CODEC_NONE = 0x01
CODEC_ZSTD = 0x02
CODEC_LZ4 = 0x03

PICKLE_PROTO_MARKER = 0x80

_NAME_TO_TAG = {None: None, "none": CODEC_NONE, "zstd": CODEC_ZSTD, "lz4": CODEC_LZ4}
_TAG_TO_NAME = {CODEC_NONE: "none", CODEC_ZSTD: "zstd", CODEC_LZ4: "lz4"}

_NONE_TAG_BYTE = bytes([CODEC_NONE])


def _load_codec(name: str) -> Tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]]:
    """Return (compress, decompress) callables for the named codec."""
    if name == "none":
        def identity(b: bytes) -> bytes:
            return b if isinstance(b, bytes) else bytes(b)
        return identity, identity
    if name == "zstd":
        import zstandard as zstd
        compressor = zstd.ZstdCompressor()
        decompressor = zstd.ZstdDecompressor()
        return compressor.compress, decompressor.decompress
    if name == "lz4":
        import lz4.frame as lz4f
        return lz4f.compress, lz4f.decompress
    raise ValueError(
        f"Unknown compression {name!r}. Supported: None, 'zstd', 'lz4'."
    )


class Serializer:
    """
    Stateful serializer/deserializer for pubsub and RPC frames.

    The codec is resolved once at construction time so the hot path on every
    serialize/deserialize call is a straight-line bound function call instead
    of a branch. Decompressors for codecs other than the configured one are
    loaded lazily the first time a frame tagged with that codec is observed,
    so a Subscriber configured with compression=None can still decode incoming
    zstd-compressed messages (and vice versa).

    Wire formats
    ------------
    Inbound-only single-frame (kept for back-compat with very old senders):
        b"<topic> <pickle_bytes>"

    Inbound-only pre-protocol-5 multipart (kept for back-compat):
        [topic_bytes, pickle_bytes]
        - main frame begins with 0x80 (pickle proto marker)

    Default (compression=None) multipart:
        [topic_bytes, pickle_main, *out_of_band_buffers]
        - pickle_main begins with 0x80

    Compressed multipart (compression in {'zstd','lz4'}):
        [topic_bytes, tag||payload_main, *(tag||payload_buffer)]
        - tag is a single byte from CODEC_* constants (never 0x80)
        - frames below compression_min_bytes carry tag=CODEC_NONE (0x01) and
          uncompressed payload, so one wire message may mix codecs across frames
    """

    def __init__(
        self,
        compression: Optional[str] = None,
        compression_min_bytes: int = 1024,
    ):
        if compression not in _NAME_TO_TAG:
            raise ValueError(
                f"Unknown compression {compression!r}. Supported: None, 'zstd', 'lz4'."
            )
        if compression_min_bytes < 0:
            raise ValueError("compression_min_bytes must be >= 0")

        self.compression = compression
        self.compression_min_bytes = compression_min_bytes

        # Configure the outbound (serialize) hot path once.
        if compression is None:
            self._tag_byte: Optional[bytes] = None
            self._compress: Optional[Callable[[bytes], bytes]] = None
        else:
            self._tag_byte = bytes([_NAME_TO_TAG[compression]])
            self._compress, _ = _load_codec(compression)

        # Decompressor cache for inbound auto-detect, keyed by tag byte int.
        self._decompressors: dict = {CODEC_NONE: (lambda b: b)}
        if compression is not None:
            _, decomp = _load_codec(compression)
            self._decompressors[_NAME_TO_TAG[compression]] = decomp

    def serialize(self, topic: str, data: Any) -> List[bytes]:
        topic_bytes = topic.encode("utf-8")

        buffers: List[Any] = []
        main = pickle.dumps(data, protocol=5, buffer_callback=buffers.append)

        if self._compress is None:
            # Backward-compatible default format: no codec tag.
            return [topic_bytes, main, *buffers]

        tag = self._tag_byte
        compress = self._compress
        threshold = self.compression_min_bytes

        if len(main) < threshold:
            out: List[bytes] = [topic_bytes, _NONE_TAG_BYTE + main]
        else:
            out = [topic_bytes, tag + compress(main)]
        for buf in buffers:
            b = bytes(buf)
            if len(b) < threshold:
                out.append(_NONE_TAG_BYTE + b)
            else:
                out.append(tag + compress(b))
        return out

    def deserialize(self, frames: List[bytes]) -> Tuple[str, Any]:
        # 1. Single-frame legacy (very old senders).
        if len(frames) == 1:
            msg = frames[0]
            topic_str, data_bytes = msg.split(b" ", 1)
            return topic_str.decode("utf-8"), pickle.loads(data_bytes)

        topic = frames[0].decode("utf-8")
        main_frame = frames[1]
        if not main_frame:
            raise ValueError("empty main frame")

        first = main_frame[0]

        # Uncompressed: legacy multipart or default-protocol-5 multipart.
        if first == PICKLE_PROTO_MARKER:
            return topic, pickle.loads(main_frame, buffers=frames[2:])

        # Tagged (possibly compressed) format.
        decompress = self._get_decompressor(first)
        main = decompress(main_frame[1:])

        bufs: List[bytes] = []
        for f in frames[2:]:
            if not f:
                raise ValueError("empty buffer frame")
            d = self._get_decompressor(f[0])
            bufs.append(d(f[1:]))

        return topic, pickle.loads(main, buffers=bufs)

    def _get_decompressor(self, tag: int) -> Callable[[bytes], bytes]:
        decomp = self._decompressors.get(tag)
        if decomp is not None:
            return decomp
        name = _TAG_TO_NAME.get(tag)
        if name is None:
            raise ValueError(
                f"Unknown codec tag 0x{tag:02x} in incoming frame"
            )
        _, decomp = _load_codec(name)
        self._decompressors[tag] = decomp
        return decomp


# A module-level default serializer keeps the historical free-function API working
# without paying constructor cost on every call.
_default_serializer = Serializer()


def serialize(
    topic: str,
    data: Any,
    compression: Optional[str] = None,
    compression_min_bytes: int = 1024,
) -> List[bytes]:
    """
    Serialize data for transport.

    Args:
        topic: The topic string.
        data: The object to serialize.
        compression: Optional codec name. One of None, 'zstd', 'lz4'.
        compression_min_bytes: Per-frame size below which compression is skipped
            (the frame is emitted with the CODEC_NONE tag instead). Has no effect
            when compression is None. Defaults to 1024.
    """
    if compression is None:
        return _default_serializer.serialize(topic, data)
    return Serializer(
        compression=compression,
        compression_min_bytes=compression_min_bytes,
    ).serialize(topic, data)


def deserialize(frames: List[bytes]) -> Tuple[str, Any]:
    """Deserialize wire frames. Auto-detects codec from the per-frame tag byte."""
    return _default_serializer.deserialize(frames)
