"""Throughput comparison on one machine: this engine against SQLite.

Both are configured for the same durability contract: every write is fsynced
before it is acknowledged (SQLite: WAL journal, synchronous=FULL, one
transaction per write). The numbers are dominated by fsync cost and depend on
the disk; compare the two columns with each other, not with other machines.
"""
import argparse
import os
import random
import shutil
import sqlite3
import string
import tempfile
import time

from lsmkv.store import KVStore


def random_key(rng, n=12):
    return "".join(rng.choices(string.ascii_lowercase + string.digits, k=n))


class SqliteBaseline:
    def __init__(self, data_dir):
        self.db = sqlite3.connect(os.path.join(data_dir, "kv.db"), isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID")

    def put(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO kv VALUES (?, ?)", (key, value))

    def get(self, key):
        row = self.db.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
        return row[0] if row else None

    def scan(self, start, end):
        return self.db.execute("SELECT k, v FROM kv WHERE k >= ? AND k < ? ORDER BY k",
                               (start, end))

    def compact(self):
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def close(self):
        self.db.close()


def bench(name, make_store, num_writes, num_reads, seed):
    rng = random.Random(seed)
    data_dir = tempfile.mkdtemp(prefix="kv_bench_")
    try:
        store = make_store(data_dir)
        keys = [random_key(rng) for _ in range(num_writes)]
        start = time.perf_counter()
        for key in keys:
            store.put(key, f"value-{key}")
        write_s = time.perf_counter() - start
        store.compact()

        sample = rng.sample(keys, min(num_reads, len(keys)))
        start = time.perf_counter()
        for key in sample:
            assert store.get(key) == f"value-{key}"
        hit_s = time.perf_counter() - start

        misses = [random_key(rng, 13) for _ in range(len(sample))]
        start = time.perf_counter()
        for key in misses:
            store.get(key)
        miss_s = time.perf_counter() - start

        start = time.perf_counter()
        rows = 0
        for _ in range(200):
            lo = random_key(rng, 2)
            rows += sum(1 for _ in store.scan(lo, lo + "~"))
        scan_s = time.perf_counter() - start
        store.close()
        print(f"{name:<8} writes {num_writes / write_s:>9,.0f}/s   "
              f"point hits {len(sample) / hit_s:>9,.0f}/s   "
              f"point misses {len(misses) / miss_s:>9,.0f}/s   "
              f"range scans {200 / scan_s:>7,.0f}/s ({rows} rows)")
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writes", type=int, default=20_000)
    parser.add_argument("--reads", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()
    bench("lsmkv", lambda d: KVStore(d, memtable_limit=500), args.writes, args.reads, args.seed)
    bench("sqlite", SqliteBaseline, args.writes, args.reads, args.seed)
