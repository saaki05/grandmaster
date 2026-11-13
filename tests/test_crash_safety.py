"""Crash-safety tests: each one leaves the data directory in a state that a
real crash can produce, then reopens the store and checks nothing acknowledged
was lost and nothing corrupt is served."""
import json
import os
import random

import pytest

from lsmkv.store import KVStore


def _fill(store, n, prefix="k"):
    for i in range(n):
        store.put(f"{prefix}{i:04d}", f"v{i}")


def test_torn_wal_tail_is_ignored(tmp_path):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    _fill(store, 10)
    store.close()
    # a crash in the middle of an append leaves a partial final record
    with open(tmp_path / "wal.log", "ab") as f:
        f.write(b'0badc0de {"type": "put", "key": "torn", "va')

    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert reopened.get("k0009") == "v9"
    assert reopened.get("torn") is None
    # the store must stay writable and the new write must survive another reopen
    reopened.put("after", "crash")
    reopened.close()
    assert KVStore(str(tmp_path), memtable_limit=1000).get("after") == "crash"


def test_corrupted_wal_record_is_not_replayed(tmp_path):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    store.put("a", "good")
    store.put("b", "good")
    store.close()
    raw = (tmp_path / "wal.log").read_bytes()
    lines = raw.splitlines(keepends=True)
    # flip payload bytes of the last record; it is still valid JSON
    lines[-1] = lines[-1].replace(b"good", b"evil")
    (tmp_path / "wal.log").write_bytes(b"".join(lines))

    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert reopened.get("a") == "good"
    assert reopened.get("b") != "evil"


def test_partial_sstable_from_interrupted_flush_is_discarded(tmp_path):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    _fill(store, 20)
    store.flush()
    store.put("live", "1")
    store.close()
    # crash while writing the next table: data half-written, no index or filter
    (tmp_path / "sstable_2.data").write_bytes(b'{"k": "zzz", "v": "half')

    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert reopened.get("k0003") == "v3"
    assert reopened.get("zzz") is None
    assert reopened.get("live") == "1"
    assert not (tmp_path / "sstable_2.data").exists()


def test_crash_after_flush_before_wal_clear_keeps_data(tmp_path, monkeypatch):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    _fill(store, 5)

    def boom():
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(store.wal, "clear", boom)
    with pytest.raises(RuntimeError):
        store.flush()

    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert [reopened.get(f"k{i:04d}") for i in range(5)] == [f"v{i}" for i in range(5)]


def test_crash_during_compaction_before_install(tmp_path, monkeypatch):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    _fill(store, 10)
    store.flush()
    store.delete("k0001")
    store.flush()

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(store.manifest, "save", boom)
    with pytest.raises(RuntimeError):
        store.compact()

    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert reopened.get("k0001") is None  # the delete must not be undone
    assert reopened.get("k0002") == "v2"
    assert len(reopened.sstables) == 2  # the uninstalled merged table is gone


def test_crash_during_compaction_after_install(tmp_path, monkeypatch):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    _fill(store, 10)
    store.flush()
    store.delete("k0001")
    store.flush()
    old = list(store.sstables)

    def boom():
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(old[0], "delete_files", boom)
    with pytest.raises(RuntimeError):
        store.compact()

    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert reopened.get("k0001") is None
    assert reopened.get("k0002") == "v2"
    assert len(reopened.sstables) == 1
    leftovers = [n for n in os.listdir(tmp_path) if n.startswith("sstable_")]
    assert len(leftovers) == 3  # one table = data + index + filter


def test_manifest_is_replaced_atomically(tmp_path):
    store = KVStore(str(tmp_path), memtable_limit=1000)
    _fill(store, 3)
    store.flush()
    store.close()
    # a crash while writing the next manifest leaves only a temp file behind
    (tmp_path / "MANIFEST.tmp").write_text('{"tables": [1, 2')
    reopened = KVStore(str(tmp_path), memtable_limit=1000)
    assert reopened.get("k0000") == "v0"
    assert json.loads((tmp_path / "MANIFEST").read_text())["tables"] == [1]


@pytest.mark.parametrize("seed", range(25))
def test_random_operations_match_a_dict_model(tmp_path, seed):
    """Model-based test: after any interleaving of writes, deletes, flushes,
    compactions and unclean reopens, the store agrees with a plain dict."""
    rng = random.Random(seed)
    model = {}
    store = KVStore(str(tmp_path), memtable_limit=rng.choice([3, 7, 50]))
    keys = [f"key{i:02d}" for i in range(30)]
    for _ in range(400):
        action = rng.random()
        key = rng.choice(keys)
        if action < 0.50:
            value = f"v{rng.randrange(10**6)}"
            store.put(key, value)
            model[key] = value
        elif action < 0.70:
            store.delete(key)
            model.pop(key, None)
        elif action < 0.85:
            assert store.get(key) == model.get(key)
        elif action < 0.90:
            store.flush()
        elif action < 0.94:
            store.compact()
        elif action < 0.97:
            lo, hi = sorted(rng.sample(keys, 2))
            expected = sorted((k, v) for k, v in model.items() if lo <= k < hi)
            assert list(store.scan(lo, hi)) == expected
        else:
            # reopen without close(): everything acknowledged must still be there
            store = KVStore(str(tmp_path), memtable_limit=store.memtable_limit)
    for key in keys:
        assert store.get(key) == model.get(key)
    assert list(store.scan()) == sorted(model.items())
