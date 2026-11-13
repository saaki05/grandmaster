from lsmkv.memtable import MemTable, _MISSING


def test_put_and_get():
    m = MemTable()
    m.put("a", "1")
    assert m.get("a") == "1"


def test_missing_key():
    m = MemTable()
    assert m.get("nope") is _MISSING


def test_delete_creates_tombstone():
    m = MemTable()
    m.put("a", "1")
    m.delete("a")
    assert m.get("a") is None


def test_sorted_items():
    m = MemTable()
    m.put("banana", "1")
    m.put("apple", "2")
    m.put("cherry", "3")
    assert m.sorted_items() == [("apple", "2"), ("banana", "1"), ("cherry", "3")]


def test_clear():
    m = MemTable()
    m.put("a", "1")
    m.clear()
    assert len(m) == 0
