import pytest
import numpy as np
import torch
from commlink.serializer import serialize, deserialize
import pickle

def test_legacy_equivalence():
    """Test that legacy serialization works as expected (now returns list of frames)."""
    data = {"key": "value", "num": 123}
    topic = "test"
    
    # Legacy now returns [topic_bytes, pickle_bytes]
    frames = serialize(topic, data, legacy=True)
    assert isinstance(frames, list)
    assert len(frames) == 2
    assert frames[0] == b"test"
    assert frames[1] == pickle.dumps(data)
    
    # Test that we can deserialize this using the unified deserializer
    recovered_topic, recovered_data = deserialize(frames)
    assert recovered_topic == topic
    assert recovered_data == data

    # Test backward compatibility: Manually construct old single-frame message
    old_style_msg = frames[0] + b" " + frames[1]
    # Pass as list of one frame, which is what zmq recv_multipart returns for single message
    recovered_topic_old, recovered_data_old = deserialize([old_style_msg])
    assert recovered_topic_old == topic
    assert recovered_data_old == data

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

def test_unified_deserialization_handles_legacy_frames():
    """
    Test that the new unified deserialize() function can handle 
    the list of frames produced by serialize(..., legacy=True).
    """
    data = {"foo": "bar"}
    topic = "legacy_test"
    legacy_frames = serialize(topic, data, legacy=True)
    
    # Unified deserialize should handle [topic, pickle_data] just fine
    recovered_topic, recovered_data = deserialize(legacy_frames)
    assert recovered_topic == topic
    assert recovered_data == data
