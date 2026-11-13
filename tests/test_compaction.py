import os
import shutil
import tempfile

import pytest

from lsmkv.compaction import compact
from lsmkv.sstable import SSTable


@pytest.fixture
def tmp_dir():
    tmp = tempfile.mkdtemp()
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


def test_compact_merges_and_dedupes_keeping_newest(tmp_dir):
    old = SSTable.write(os.path.join(tmp_dir, "old"), [("a", "old-a"), ("b", "old-b")])
    new = SSTable.write(os.path.join(tmp_dir, "new"), [("a", "new-a"), ("c", "new-c")])

    merged = compact([old, new], os.path.join(tmp_dir, "merged"))
    items = dict(merged.all_items())

    assert items == {"a": "new-a", "b": "old-b", "c": "new-c"}


def test_compact_drops_tombstones_when_requested(tmp_dir):
    old = SSTable.write(os.path.join(tmp_dir, "old"), [("a", "1"), ("b", "2")])
    new = SSTable.write(os.path.join(tmp_dir, "new"), [("a", None)])  # a was deleted

    merged = compact([old, new], os.path.join(tmp_dir, "merged"), drop_tombstones=True)
    items = dict(merged.all_items())

    assert "a" not in items
    assert items == {"b": "2"}


def test_compact_keeps_tombstones_when_not_dropping(tmp_dir):
    old = SSTable.write(os.path.join(tmp_dir, "old"), [("a", "1")])
    new = SSTable.write(os.path.join(tmp_dir, "new"), [("a", None)])

    merged = compact([old, new], os.path.join(tmp_dir, "merged"), drop_tombstones=False)
    items = dict(merged.all_items())

    assert items["a"] is None
