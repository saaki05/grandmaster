import random
import shutil
import string
import tempfile
import time

from store import KVStore


def random_key(n=12):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def run(num_writes: int = 20_000, num_reads: int = 5_000):
    data_dir = tempfile.mkdtemp(prefix="lsm_bench_")
    try:
        store = KVStore(data_dir, memtable_limit=500)
        keys = [random_key() for _ in range(num_writes)]

        start = time.perf_counter()
        for key in keys:
            store.put(key, f"value-{key}")
        write_elapsed = time.perf_counter() - start

        store.flush()
        store.compact()

        sample_keys = random.sample(keys, min(num_reads, len(keys)))
        start = time.perf_counter()
        for key in sample_keys:
            store.get(key)
        read_elapsed = time.perf_counter() - start

        store.close()

        print(f"Writes: {num_writes} in {write_elapsed:.3f}s "
              f"({num_writes / write_elapsed:,.0f} ops/sec)")
        print(f"Reads:  {len(sample_keys)} in {read_elapsed:.3f}s "
              f"({len(sample_keys) / read_elapsed:,.0f} ops/sec)")
        print(f"SSTables after compaction: {len(store.sstables)}")
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    run()
