import bisect
import json
import os

from .bloom_filter import BloomFilter
from .sentinel import MISSING as _MISSING

INDEX_INTERVAL = 4  # every Nth key gets a sparse index entry, trading memory for lookup speed


class SSTable:
    """An immutable, sorted, on-disk table of key-value pairs.

    Layout on disk (three files sharing a base path):
      <base>.data  - newline-delimited JSON records, sorted by key
      <base>.idx   - sparse index: every INDEX_INTERVAL-th key and its byte offset
      <base>.bloom - a serialized Bloom filter over every key in the table
    """

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.data_path = base_path + ".data"
        self.index_path = base_path + ".idx"
        self.bloom_path = base_path + ".bloom"
        self._index = None  # list of (key, offset), loaded lazily
        self._bloom = None

    @classmethod
    def write(cls, base_path: str, sorted_items) -> "SSTable":
        sorted_items = list(sorted_items)
        bloom = BloomFilter(expected_items=max(len(sorted_items), 1))
        index = []

        with open(base_path + ".data", "w", encoding="utf-8") as f:
            offset = 0
            for i, (key, value) in enumerate(sorted_items):
                bloom.add(key)
                if i % INDEX_INTERVAL == 0:
                    index.append((key, offset))
                line = json.dumps({"k": key, "v": value})
                f.write(line + "\n")
                offset += len(line.encode("utf-8")) + 1

        with open(base_path + ".idx", "w", encoding="utf-8") as f:
            json.dump(index, f)

        with open(base_path + ".bloom", "w", encoding="utf-8") as f:
            json.dump(bloom.to_dict(), f)

        return cls(base_path)

    def _load_index(self):
        if self._index is None:
            with open(self.index_path, encoding="utf-8") as f:
                self._index = [tuple(entry) for entry in json.load(f)]
        return self._index

    def _load_bloom(self):
        if self._bloom is None:
            with open(self.bloom_path, encoding="utf-8") as f:
                self._bloom = BloomFilter.from_dict(json.load(f))
        return self._bloom

    def get(self, key: str):
        if not self._load_bloom().might_contain(key):
            return _MISSING

        index = self._load_index()
        keys = [entry[0] for entry in index]
        pos = bisect.bisect_right(keys, key) - 1
        start_offset = index[pos][1] if pos >= 0 else 0
        end_offset = index[pos + 1][1] if pos + 1 < len(index) else None

        with open(self.data_path, encoding="utf-8") as f:
            f.seek(start_offset)
            while end_offset is None or f.tell() < end_offset:
                line = f.readline()
                if not line:
                    break
                record = json.loads(line)
                if record["k"] == key:
                    return record["v"]
                if record["k"] > key:
                    break
        return _MISSING

    def all_items(self):
        with open(self.data_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    record = json.loads(line)
                    yield record["k"], record["v"]

    def delete_files(self):
        for path in (self.data_path, self.index_path, self.bloom_path):
            if os.path.exists(path):
                os.remove(path)
