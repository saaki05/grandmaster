import bisect
import json
import os

from .bloom_filter import BloomFilter
from .fsutil import write_durable
from .sentinel import MISSING as _MISSING

INDEX_INTERVAL = 4  # every Nth key gets a sparse index entry, trading memory for lookup speed


class SSTable:
    """An immutable, sorted, on-disk table of key-value pairs.

    Layout on disk (three files sharing a base path):
      <base>.data  - newline-delimited JSON records, sorted by key
      <base>.idx   - sparse index: every INDEX_INTERVAL-th key and its byte offset
      <base>.bloom - a serialized Bloom filter over every key in the table

    All three files are fsynced before `write` returns. The data file is read
    and written in binary mode so byte offsets are exact on every platform.
    """

    SUFFIXES = (".data", ".idx", ".bloom")

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.data_path = base_path + ".data"
        self.index_path = base_path + ".idx"
        self.bloom_path = base_path + ".bloom"
        self._index = None  # list of (key, offset), loaded lazily
        self._index_keys = None
        self._bloom = None

    @classmethod
    def write(cls, base_path: str, sorted_items) -> "SSTable":
        bloom_keys = []
        index = []
        with open(base_path + ".data", "wb") as f:
            offset = 0
            for i, (key, value) in enumerate(sorted_items):
                bloom_keys.append(key)
                if i % INDEX_INTERVAL == 0:
                    index.append((key, offset))
                line = json.dumps({"k": key, "v": value}).encode("utf-8") + b"\n"
                f.write(line)
                offset += len(line)
            f.flush()
            os.fsync(f.fileno())

        bloom = BloomFilter(expected_items=max(len(bloom_keys), 1))
        for key in bloom_keys:
            bloom.add(key)
        write_durable(base_path + ".idx", json.dumps(index).encode("utf-8"))
        write_durable(base_path + ".bloom", json.dumps(bloom.to_dict()).encode("utf-8"))
        return cls(base_path)

    def is_complete(self) -> bool:
        return all(os.path.exists(self.base_path + s) for s in self.SUFFIXES)

    def _load_index(self):
        if self._index is None:
            with open(self.index_path, encoding="utf-8") as f:
                self._index = [tuple(entry) for entry in json.load(f)]
            self._index_keys = [entry[0] for entry in self._index]
        return self._index

    def _load_bloom(self):
        if self._bloom is None:
            with open(self.bloom_path, encoding="utf-8") as f:
                self._bloom = BloomFilter.from_dict(json.load(f))
        return self._bloom

    def _block_start(self, key: str) -> int:
        """Offset of the index block that would contain `key`."""
        index = self._load_index()
        pos = bisect.bisect_right(self._index_keys, key) - 1
        return index[pos][1] if pos >= 0 else 0

    def get(self, key: str):
        if not self._load_bloom().might_contain(key):
            return _MISSING
        with open(self.data_path, "rb") as f:
            f.seek(self._block_start(key))
            for _ in range(INDEX_INTERVAL):
                line = f.readline()
                if not line:
                    break
                record = json.loads(line)
                if record["k"] == key:
                    return record["v"]
                if record["k"] > key:
                    break
        return _MISSING

    def iter_from(self, start_key=None):
        """Yield (key, value) in key order for keys >= start_key. Uses the
        sparse index to seek, so a range scan does not read the whole table."""
        with open(self.data_path, "rb") as f:
            if start_key is not None:
                f.seek(self._block_start(start_key))
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                if start_key is None or record["k"] >= start_key:
                    yield record["k"], record["v"]

    def all_items(self):
        return self.iter_from(None)

    def delete_files(self):
        for path in (self.data_path, self.index_path, self.bloom_path):
            if os.path.exists(path):
                os.remove(path)
