import string
import random

from lsmkv.bloom_filter import BloomFilter


def test_no_false_negatives():
    bloom = BloomFilter(expected_items=100, false_positive_rate=0.01)
    keys = [f"key-{i}" for i in range(100)]
    for key in keys:
        bloom.add(key)
    for key in keys:
        assert bloom.might_contain(key) is True


def test_absent_keys_mostly_rejected():
    random.seed(42)
    bloom = BloomFilter(expected_items=100, false_positive_rate=0.01)
    present = [f"present-{i}" for i in range(100)]
    for key in present:
        bloom.add(key)

    absent = [f"absent-{i}" for i in range(1000)]
    false_positives = sum(1 for key in absent if bloom.might_contain(key))
    false_positive_rate = false_positives / len(absent)
    assert false_positive_rate < 0.05  # well under the configured 1% target with margin


def test_serialization_round_trip():
    bloom = BloomFilter(expected_items=50)
    bloom.add("hello")
    restored = BloomFilter.from_dict(bloom.to_dict())
    assert restored.might_contain("hello") is True
