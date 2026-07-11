import heapq
from itertools import groupby
from operator import itemgetter

from sstable import SSTable


def compact(sstables, output_base_path: str, drop_tombstones: bool = True) -> SSTable:
    """Merge multiple SSTables (oldest -> newest) into one, keeping only the
    most recent value per key. A streaming k-way merge (heapq.merge) is used
    so this never loads more than one row per table into memory at a time.

    When drop_tombstones is True (a "full" compaction covering every table),
    deleted keys are dropped entirely instead of carried forward.
    """
    iterators = [table.all_items() for table in sstables]
    merged = heapq.merge(*iterators, key=itemgetter(0))

    def deduped():
        for _key, group in groupby(merged, key=itemgetter(0)):
            *_, last = group  # last entry for this key wins (it came from the newest table)
            key, value = last
            if drop_tombstones and value is None:
                continue
            yield key, value

    return SSTable.write(output_base_path, deduped())
