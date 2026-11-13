import heapq
import os
import re
from itertools import groupby

from .compaction import compact
from .fsutil import fsync_dir
from .manifest import Manifest
from .memtable import _MISSING, MemTable
from .sstable import SSTable
from .wal import WriteAheadLog

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

        self.manifest = Manifest(data_dir)
        self.wal = WriteAheadLog(os.path.join(data_dir, "wal.log"))
        self.memtable = MemTable()
        self.sstables = self._open_sstables()
        self._next_sstable_id = max(self._table_ids(), default=0) + 1

        self._recover()

    def _path_for(self, table_id: int) -> str:
        return os.path.join(self.data_dir, f"sstable_{table_id}")

    def _ids_on_disk(self) -> list[int]:
        ids = set()
        for name in os.listdir(self.data_dir):
            match = _SSTABLE_NAME_RE.match(name.split(".")[0])
            if match:
                ids.add(int(match.group(1)))
        return sorted(ids)

    def _table_ids(self) -> list[int]:
        return [int(_SSTABLE_NAME_RE.match(os.path.basename(t.base_path)).group(1))
                for t in self.sstables]

    def _open_sstables(self):
        """Load the tables named by the manifest and remove everything else.

        A directory written before the manifest existed is adopted: every table
        with all three files present is kept and a manifest is written.
        """
        on_disk = self._ids_on_disk()
        if self.manifest.exists():
            live = self.manifest.load()
        else:
            live = [i for i in on_disk if SSTable(self._path_for(i)).is_complete()]
            self.manifest.save(live)
        for orphan in set(on_disk) - set(live):
            SSTable(self._path_for(orphan)).delete_files()
        tmp = self.manifest.path + ".tmp"
        if os.path.exists(tmp):
            os.remove(tmp)
        return [SSTable(self._path_for(i)) for i in sorted(live)]

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

    def scan(self, start=None, end=None):
        """Yield (key, value) in key order for start <= key < end.

        Sources are merged newest-first, so for each key only the most recent
        version is seen; tombstones hide older values and are not returned.
        """
        def bounded(items, rank):
            for key, value in items:
                if end is not None and key >= end:
                    return
                yield key, rank, value

        mem = [(k, v) for k, v in self.memtable.sorted_items()
               if start is None or k >= start]
        sources = [bounded(mem, 0)]
        for rank, table in enumerate(reversed(self.sstables), start=1):
            sources.append(bounded(table.iter_from(start), rank))

        merged = heapq.merge(*sources, key=lambda item: (item[0], item[1]))
        for key, versions in groupby(merged, key=lambda item: item[0]):
            _, _, value = next(versions)  # lowest rank = newest
            if value is not None:
                yield key, value

    def flush(self):
        """Write the memtable to a new SSTable.

        Order matters for crash safety: the table is fsynced, then the manifest
        names it, and only then is the log cleared. A crash before the manifest
        swap leaves the log intact; a crash after it replays operations that
        are already in the table, which is harmless.
        """
        if self.memtable.is_empty():
            return None
        table_id = self._next_sstable_id
        table = SSTable.write(self._path_for(table_id), self.memtable.sorted_items())
        fsync_dir(self.data_dir)
        self.manifest.save(self._table_ids() + [table_id])
        self.sstables.append(table)
        self._next_sstable_id += 1
        self.memtable.clear()
        self.wal.clear()
        return table

    def compact(self):
        """Merge every SSTable into one and drop tombstones.

        The merged table becomes visible only when the manifest swap succeeds.
        Old files are removed afterwards; if that is interrupted, the next open
        removes them because the manifest no longer names them.
        """
        self.flush()
        if len(self.sstables) <= 1:
            return
        table_id = self._next_sstable_id
        self._next_sstable_id += 1
        merged = compact(self.sstables, self._path_for(table_id), drop_tombstones=True)
        fsync_dir(self.data_dir)
        self.manifest.save([table_id])
        old_tables = self.sstables
        self.sstables = [merged]
        for table in old_tables:
            table.delete_files()

    def close(self):
        self.wal.close()
