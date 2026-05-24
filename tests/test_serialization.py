import pytest
import numpy as np
import torch
from commlink.serializer import serialize, deserialize
import pickle

def test_deserialize_accepts_pre_protocol5_multipart():
    """Inbound back-compat: frames from old senders that emitted
    [topic_bytes, pickle.dumps(data)] must still deserialize."""
    data = {"key": "value", "num": 123}
    topic = "test"
    frames = [topic.encode("utf-8"), pickle.dumps(data)]

    recovered_topic, recovered_data = deserialize(frames)
    assert recovered_topic == topic
    assert recovered_data == data


def test_deserialize_accepts_single_frame_legacy():
    """Inbound back-compat: very old senders emitted b'<topic> <pickle>' as one frame."""
    data = {"key": "value", "num": 123}
    topic = "test"
    old_style_msg = topic.encode("utf-8") + b" " + pickle.dumps(data)

    recovered_topic, recovered_data = deserialize([old_style_msg])
    assert recovered_topic == topic
    assert recovered_data == data

def test_default_equivalence_numpy():
    """Test that default serialization preserves numpy arrays exactly."""
    data = {"img": np.random.rand(100, 100).astype(np.float32)}
    topic = "test"
    
    frames = serialize(topic, data)
    assert len(frames) > 2  # Topic + Pickle + Buffer(s)
    
    recovered_topic, recovered_data = deserialize(frames)
    assert recovered_topic == topic
    assert np.array_equal(recovered_data['img'], data['img'])

def test_default_equivalence_torch():
    """Test that default serialization preserves torch tensors exactly."""
    data = {"tens": torch.randn(10, 10)}
    topic = "test"
    
    frames = serialize(topic, data)
    
    recovered_topic, recovered_data = deserialize(frames)
    assert recovered_topic == topic
    assert torch.equal(recovered_data['tens'], data['tens'])

