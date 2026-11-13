# LSM-tree key-value store with Raft replication

[![ci](https://github.com/saaki05/grandmaster/actions/workflows/ci.yml/badge.svg)](https://github.com/saaki05/grandmaster/actions/workflows/ci.yml)

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
| `manifest.py` | Crash safety: a table exists only once the atomically replaced manifest names it, so an interrupted flush or compaction leaves nothing half-installed. |
| `compaction.py` | Without it, reads would need to check every SSTable ever created, and deleted data would live forever. A streaming k-way merge (`heapq.merge`) keeps memory use O(1) regardless of how much data is being compacted. |

## Getting started

```bash
pip install -e ".[dev]"
pytest -q                 # 234 tests, about 7 seconds

python cli.py put username saaki05
python cli.py get username
python cli.py delete username
python cli.py stats
python cli.py compact

python benchmark.py       # this engine against SQLite under the same durability setting
```

The engine lives in `lsmkv/`, the replication layer in `lsmkv/raft/`. Both use only the Python standard library.

## Storage engine guarantees

| Situation | What happens | Test |
|---|---|---|
| Crash in the middle of a write-ahead log append | The torn record is dropped and the log truncated there; every acknowledged write is kept | `test_torn_wal_tail_is_ignored` |
| A log record is corrupted on disk | Its CRC32 fails; replay stops before it instead of applying wrong data | `test_corrupted_wal_record_is_not_replayed` |
| Crash while writing an SSTable | The partial files are not in the manifest and are deleted on open; the log still holds the data | `test_partial_sstable_from_interrupted_flush_is_discarded` |
| Crash after a flush, before the log is cleared | The log is replayed over the table it duplicates; no change | `test_crash_after_flush_before_wal_clear_keeps_data` |
| Crash during compaction, before or after the manifest swap | Either the old tables or the merged table are live, never a mixture; deletes are not undone | `test_crash_during_compaction_*` |
| Any mix of puts, deletes, flushes, compactions, scans and unclean reopens | The store agrees with a plain dict (25 seeded runs) | `test_random_operations_match_a_dict_model` |

These tests simulate crashes by leaving the files in the state a crash would produce. They do not cut power to a real disk, and they assume `fsync` does what it says.

Five of these tests were written first and failed against the original engine: a torn log record stopped the store from opening, log records had no checksum, SSTables were never fsynced before the log was cleared, and half-written tables were picked up by a directory scan.

## Replication with Raft

`lsmkv/raft/node.py` implements Raft (Ongaro and Ousterhout, 2014) as a state machine with no I/O: a driver calls `tick()` and `step(message)` and delivers whatever lands in `outbox`. Implemented: leader election, log replication, commitment with the current-term rule, and a no-op entry on election. Term, vote and log are persisted by `FileStorage` with the same checksummed framing as the engine log. Committed commands are applied to the LSM engine by `LSMStateMachine`, which stores the last applied index in the engine so a restarted node does not start from zero.

**Consistency.** Writes and reads both go through the log, so client operations are linearizable. A client that gets no answer cannot tell whether its write took effect; there is no request de-duplication, so a retried write may be applied twice.

**Availability.** A cluster of `n` nodes makes progress while a majority can talk to each other. A leader cut off in a minority accepts commands but never acknowledges them, and discards them when it rejoins.

### How it is tested

`lsmkv/raft/sim.py` runs a whole cluster in one process on a tick counter, with one seeded random generator deciding message delay, loss, duplication, partitions, crashes, restarts and client traffic. A failing seed replays exactly. While it runs, the simulator checks:

- election safety: at most one leader per term
- state machine safety: no two nodes apply different entries at the same index
- log matching, and convergence of every node to the same state once faults stop
- linearizability of the recorded client history (`lsmkv/raft/linearizability.py`, Wing and Gong search with memoisation, checked per key)

150 seeded fault schedules pass. Because a test that cannot fail proves nothing, the same schedules are run against three deliberately broken variants, and the harness must catch them:

| Injected bug | Seeds (of 60) that expose it | Caught by |
|---|---|---|
| Votes granted without the log up-to-date check | 40 | state machine safety |
| A single replica counted as a quorum | 59 | election safety, state machine safety |
| Deposed leader answers reads from local state | 7 | linearizability checker only |

### Not implemented

- A network transport and server process: the cluster currently runs only inside the simulator. This is the next milestone.
- Snapshots and log compaction: the Raft log grows without bound.
- Membership changes, read leases, client sessions for exactly-once writes.
- The simulator's crash model keeps everything that was written to storage; it does not model lost un-synced writes or disk corruption.

## Measured performance

`python benchmark.py`, 20,000 writes then 5,000 point reads, 5,000 misses and 200 range scans, single thread, in a cloud container (seed 0). Both stores fsync every write.

| Store | Writes/s | Point hits/s | Point misses/s | Range scans/s |
|---|---|---|---|---|
| lsmkv | 5,697 | 66,929 | 523,021 | 28,074 |
| SQLite (WAL journal, synchronous=FULL) | 8,169 | 328,591 | 528,961 | 63,912 |

SQLite is faster on every measure, as expected for a C B-tree against JSON records parsed in Python. Misses are close because the Bloom filter answers them without touching disk. The numbers depend heavily on how expensive `fsync` is on the disk in use.

## Known limitations of the engine

- No group commit: one fsync per write
- One full compaction instead of levelled or tiered compaction
- Single-threaded, single-process; no concurrency control
- SSTable records are not checksummed (log records are)
- Keys and values are strings

## License

MIT
