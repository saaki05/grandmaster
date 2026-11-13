import os
import re

from .wal import WriteAheadLog
from .memtable import MemTable, _MISSING
from .sstable import SSTable
from .compaction import compact

_SSTABLE_NAME_RE = re.compile(r"^sstable_(\d+)$")


class KVStore:
    """A persistent, crash-recoverable key-value store built as a simplified
    LSM-tree (the design used by LevelDB, RocksDB, and Cassandra's storage layer):

      write -> WAL (durable) -> MemTable (fast, in-memory)
      MemTable full -> flushed to an immutable sorted SSTable on disk
      many SSTables -> periodically compacted into fewer, larger ones

    Reads check the MemTable first, then SSTables from newest to oldest,
    each guarded by a Bloom filter so a miss never has to scan disk.
    """

    def __init__(self, data_dir: str, memtable_limit: int = 100):
        self.data_dir = data_dir
        self.memtable_limit = memtable_limit
        os.makedirs(data_dir, exist_ok=True)

        self.wal = WriteAheadLog(os.path.join(data_dir, "wal.log"))
        self.memtable = MemTable()
        self.sstables = self._discover_sstables()
        self._next_sstable_id = self._max_sstable_id() + 1

        self._recover()

    def _discover_sstables(self):
        ids = sorted(self._max_sstable_id_list())
        return [
            SSTable(os.path.join(self.data_dir, f"sstable_{i}"))
            for i in ids
        ]

    def _max_sstable_id_list(self):
        ids = []
        for name in os.listdir(self.data_dir):
            match = _SSTABLE_NAME_RE.match(name.split(".")[0])
            if match:
                ids.append(int(match.group(1)))
        return sorted(set(ids))

    def _max_sstable_id(self) -> int:
        ids = self._max_sstable_id_list()
        return max(ids) if ids else 0

    def _recover(self):
        for op in self.wal.replay():
            if op["type"] == "put":
                self.memtable.put(op["key"], op["value"])
            elif op["type"] == "delete":
                self.memtable.delete(op["key"])

    def put(self, key: str, value: str):
        self.wal.append({"type": "put", "key": key, "value": value})
        self.memtable.put(key, value)
        self._maybe_flush()

    def delete(self, key: str):
        self.wal.append({"type": "delete", "key": key})
        self.memtable.delete(key)
        self._maybe_flush()

    def get(self, key: str):
        value = self.memtable.get(key)
        if value is not _MISSING:
            return None if value is None else value  # tombstone -> not found

        for table in reversed(self.sstables):
            value = table.get(key)
            if value is not _MISSING:
                return None if value is None else value

        return None

    def _maybe_flush(self):
        if len(self.memtable) >= self.memtable_limit:
            self.flush()

    def flush(self):
        if self.memtable.is_empty():
            return None
        base_path = os.path.join(self.data_dir, f"sstable_{self._next_sstable_id}")
        table = SSTable.write(base_path, self.memtable.sorted_items())
        self.sstables.append(table)
        self._next_sstable_id += 1
        self.memtable.clear()
        self.wal.clear()
        return table

    def compact(self):
        self.flush()
        if len(self.sstables) <= 1:
            return
        base_path = os.path.join(self.data_dir, f"sstable_{self._next_sstable_id}")
        merged = compact(self.sstables, base_path, drop_tombstones=True)
        old_tables = self.sstables
        self.sstables = [merged]
        self._next_sstable_id += 1
        for table in old_tables:
            table.delete_files()

    def close(self):
        self.wal.close()
