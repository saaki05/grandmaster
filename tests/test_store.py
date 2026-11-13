import shutil
import tempfile

import pytest

from lsmkv.store import KVStore


@pytest.fixture
def data_dir():
    tmp = tempfile.mkdtemp()
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


def test_put_and_get(data_dir):
    store = KVStore(data_dir)
    store.put("a", "1")
    assert store.get("a") == "1"
    store.close()


def test_delete(data_dir):
    store = KVStore(data_dir)
    store.put("a", "1")
    store.delete("a")
    assert store.get("a") is None
    store.close()


def test_get_missing_key_returns_none(data_dir):
    store = KVStore(data_dir)
    assert store.get("nope") is None
    store.close()


def test_flush_moves_data_to_sstable(data_dir):
    store = KVStore(data_dir, memtable_limit=1000)
    store.put("a", "1")
    store.flush()
    assert len(store.memtable) == 0
    assert len(store.sstables) == 1
    assert store.get("a") == "1"
    store.close()


def test_auto_flush_on_memtable_limit(data_dir):
    store = KVStore(data_dir, memtable_limit=3)
    for i in range(5):
        store.put(f"key{i}", str(i))
    assert len(store.sstables) >= 1
    for i in range(5):
        assert store.get(f"key{i}") == str(i)
    store.close()


def test_newer_sstable_wins_on_conflict(data_dir):
    store = KVStore(data_dir, memtable_limit=1)
    store.put("a", "v1")  # flushed immediately (limit=1)
    store.put("a", "v2")  # flushed immediately, newer
    assert store.get("a") == "v2"
    store.close()


def test_crash_recovery_replays_wal(data_dir):
    store = KVStore(data_dir, memtable_limit=1000)
    store.put("a", "1")
    store.put("b", "2")
    store.close()  # simulate crash: memtable never flushed to an sstable

    recovered = KVStore(data_dir, memtable_limit=1000)
    assert recovered.get("a") == "1"
    assert recovered.get("b") == "2"
    recovered.close()


def test_compact_reduces_sstable_count(data_dir):
    store = KVStore(data_dir, memtable_limit=2)
    for i in range(10):
        store.put(f"key{i}", str(i))
    num_before = len(store.sstables)
    store.compact()
    assert len(store.sstables) == 1
    assert num_before > 1
    for i in range(10):
        assert store.get(f"key{i}") == str(i)
    store.close()


def test_delete_persists_after_compaction(data_dir):
    store = KVStore(data_dir, memtable_limit=1)
    store.put("a", "1")
    store.delete("a")
    store.compact()
    assert store.get("a") is None
    store.close()


def test_data_survives_reopen_after_flush(data_dir):
    store = KVStore(data_dir, memtable_limit=1000)
    store.put("a", "1")
    store.flush()
    store.close()

    reopened = KVStore(data_dir, memtable_limit=1000)
    assert reopened.get("a") == "1"
    reopened.close()
