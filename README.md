# LSM-Tree Key-Value Store

A persistent, crash-recoverable key-value storage engine built from scratch in Python, implementing the **Log-Structured Merge-tree (LSM-tree)** architecture used internally by production databases like LevelDB, RocksDB, and Cassandra.

No database library is used anywhere — every layer (write-ahead log, in-memory table, on-disk sorted tables, bloom filters, compaction) is implemented directly, to demonstrate the underlying mechanics rather than call an existing engine.

## Architecture

```
put(key, value)
      │
      ▼
 ┌─────────────┐   every write is durably logged first, so a crash
 │     WAL     │   before the memtable is flushed can be replayed
 └─────────────┘
      │
      ▼
 ┌─────────────┐   fast in-memory buffer of recent writes
 │  MemTable   │   (a plain sorted dict)
 └─────────────┘
      │  once full, flush()
      ▼
 ┌─────────────┐   immutable, sorted, on-disk table:
 │  SSTable 1  │   .data (sorted records) + .idx (sparse index)
 │  SSTable 2  │   + .bloom (bloom filter, skips disk reads for
 │  SSTable N  │   keys that are definitely absent)
 └─────────────┘
      │  compact()
      ▼
 k-way streaming merge (heapq.merge) → one merged SSTable,
 newest value wins per key, tombstones dropped
```

**Reads** check the MemTable first, then SSTables from newest to oldest. Each SSTable lookup consults its Bloom filter before touching disk — if the filter says "definitely not here," the read skips straight to the next table with zero I/O.

**Writes** never block on a sort: they append to a WAL (durable) and an unsorted-but-indexed-by-Python-dict MemTable (fast). Sorting only happens once, in bulk, when the MemTable is flushed to an SSTable.

**Deletes** don't remove data in place — an LSM-tree is append-only. A delete writes a tombstone (`None`), which shadows older values until compaction physically removes it.

## Why each piece exists

| Component | Problem it solves |
|---|---|
| `wal.py` | Durability: writes are fsync'd immediately, so a crash between writes and the next flush loses nothing. |
| `memtable.py` | Speed: writes are O(1) dict inserts, not disk seeks. |
| `sstable.py` | Efficient reads from disk: a sparse index (`INDEX_INTERVAL`) means looking up any key needs one seek + a short scan, not reading the whole file. |
| `bloom_filter.py` | Avoids disk I/O for keys that don't exist in a given table — critical once there are many SSTables. |
| `compaction.py` | Without it, reads would need to check every SSTable ever created, and deleted data would live forever. A streaming k-way merge (`heapq.merge`) keeps memory use O(1) regardless of how much data is being compacted. |

## Getting started

```bash
pip install -r requirements.txt
pytest -v                 # 29 tests covering WAL recovery, sparse-index lookups,
                           # bloom filter false-positive rate, compaction correctness,
                           # and crash recovery

python cli.py put username saaki05
python cli.py get username
python cli.py delete username
python cli.py stats
python cli.py compact

python benchmark.py       # measure write/read throughput on your machine
```

## Measured performance

On this machine, `benchmark.py` (20,000 writes, 5,000 random reads, then compacted):

```
Writes: 20000 in 15.205s (1,315 ops/sec)
Reads:  5000 in 0.658s (7,604 ops/sec)
```

Write throughput is bottlenecked by `os.fsync()` on every single write — durability is prioritized over raw speed. Real LSM engines batch several writes into one fsync ("group commit") to trade a small durability window for much higher throughput; that's the natural next optimization here (see below).

## Known limitations / what a production engine adds on top
This is a teaching-grade implementation, not a production database, and doesn't try to hide that:
- No group-commit batching on the WAL (see performance note above)
- No leveled/tiered compaction strategy — just a single full compaction
- No concurrency control (single-threaded, single-process)
- No checksums on stored records
- Sparse index and bloom filter are rebuilt fully on every SSTable write, not incrementally

## Tech stack
Python (standard library only — zero external dependencies for the engine itself; `pytest` is test-only)

## License
MIT
