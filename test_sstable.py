import shutil
import tempfile
import os

import pytest

from sstable import SSTable, _MISSING


@pytest.fixture
def base_path():
    tmp = tempfile.mkdtemp()
    yield os.path.join(tmp, "table")
    shutil.rmtree(tmp, ignore_errors=True)


def test_write_and_get(base_path):
    items = [("apple", "1"), ("banana", "2"), ("cherry", "3"), ("date", "4"), ("fig", "5")]
    table = SSTable.write(base_path, items)
    assert table.get("banana") == "2"
    assert table.get("fig") == "5"
    assert table.get("apple") == "1"


def test_get_missing_key(base_path):
    items = [("apple", "1"), ("banana", "2")]
    table = SSTable.write(base_path, items)
    assert table.get("zebra") is _MISSING


def test_tombstone_is_readable_as_none(base_path):
    items = [("apple", "1"), ("banana", None)]
    table = SSTable.write(base_path, items)
    assert table.get("banana") is None


def test_all_items_returns_everything_sorted(base_path):
    items = [("c", "3"), ("a", "1"), ("b", "2")]
    table = SSTable.write(base_path, sorted(items))
    assert list(table.all_items()) == [("a", "1"), ("b", "2"), ("c", "3")]


def test_lookup_with_many_keys_spanning_multiple_index_blocks(base_path):
    items = [(f"k{i:04d}", str(i)) for i in range(200)]
    table = SSTable.write(base_path, items)
    for i in (0, 1, 50, 51, 100, 199):
        assert table.get(f"k{i:04d}") == str(i)


def test_delete_files_removes_all_three(base_path):
    table = SSTable.write(base_path, [("a", "1")])
    assert os.path.exists(table.data_path)
    table.delete_files()
    assert not os.path.exists(table.data_path)
    assert not os.path.exists(table.index_path)
    assert not os.path.exists(table.bloom_path)
