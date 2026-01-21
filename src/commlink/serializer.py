import pickle
from typing import Any, List, Tuple

def serialize(topic: str, data: Any, legacy: bool = False) -> List[bytes]:
    """
    Serialize data.
    
    Args:
        topic: The topic string.
        data: The object to serialize.
        legacy: If True, uses standard pickle.dumps and returns [topic_bytes, pickle_bytes].
                If False (default), uses Pickle Protocol 5 + ZMQ Multipart [topic, main, *buffers].
    """
    topic_bytes = topic.encode("utf-8")
    
    if legacy:
        # Legacy wire format as multipart: [topic, pickle_bytes]
        data_bytes = pickle.dumps(data)
        return [topic_bytes, data_bytes]

    buffers = []
    # Protocol 5 allows us to extract buffers to avoid copying data into the pickle stream
    main_stream = pickle.dumps(data, protocol=5, buffer_callback=buffers.append)
    
    # Wire format: [topic, main_pickle_stream, buffer1, buffer2...]
    frames = [topic_bytes, main_stream] + buffers
    return frames


def deserialize(frames: List[bytes]) -> Tuple[str, Any]:
    """
    Deserialize data from the wire format.
    Handles:
    1. Single-frame legacy: "topic data" (concatenated bytes)
    2. Multi-frame legacy: [topic, pickle_bytes]
    3. Multi-frame default: [topic, main_stream, *buffers]
    """
    # 1. Single-frame legacy (backward compatibility for old external publishers)
    if len(frames) == 1:
        msg = frames[0]
        topic_str, data_bytes = msg.split(b" ", 1)
        topic = topic_str.decode("utf-8")
        data = pickle.loads(data_bytes)
        return topic, data

    # Multi-frame (Legacy or Unified)
    # Frame 0: Topic
    topic = frames[0].decode("utf-8")
    
    # Frame 1: Main pickle stream (or simple pickle bytes for legacy)
    main_stream = frames[1]
    
    # Frames 2+: Out-of-band buffers (only for default protocol 5)
    buffers = frames[2:]
    
    data = pickle.loads(main_stream, buffers=buffers)
    return topic, data
