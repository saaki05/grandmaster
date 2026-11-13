"""Persistent Raft state: current term, vote, and the log.

Indexes are 1-based as in the Raft paper; index 0 is the empty prefix with term 0.
An entry is ``{"term": int, "cmd": <json value or None for a no-op>}``.
"""
import json
import os

from ..fsutil import replace_atomically
from ..wal import WriteAheadLog


class MemoryStorage:
    """Keeps state in memory. In the simulator the object outlives a node
    'crash', so it plays the role of that node's disk."""

    def __init__(self):
        self.term = 0
        self.voted_for = None
        self._log = []

    def save_state(self, term, voted_for):
        self.term = term
        self.voted_for = voted_for

    def last_index(self):
        return len(self._log)

    def term_at(self, index):
        return self._log[index - 1]["term"] if index >= 1 else 0

    def entry(self, index):
        return self._log[index - 1]

    def entries_from(self, index):
        return self._log[index - 1:]

    def append(self, entry):
        self._log.append(entry)

    def truncate_from(self, index):
        del self._log[index - 1:]


class FileStorage(MemoryStorage):
    """Durable variant. Term and vote are replaced atomically; log entries are
    appended as checksummed, fsynced records (the same framing as the engine's
    write-ahead log), so a torn final record is dropped on reopen."""

    def __init__(self, directory):
        super().__init__()
        os.makedirs(directory, exist_ok=True)
        self._state_path = os.path.join(directory, "raft_state.json")
        self._log_path = os.path.join(directory, "raft_log.jsonl")
        if os.path.exists(self._state_path):
            with open(self._state_path, encoding="utf-8") as f:
                state = json.load(f)
            self.term, self.voted_for = state["term"], state["voted_for"]
        self._wal = WriteAheadLog(self._log_path)
        self._log = list(self._wal.replay())

    def save_state(self, term, voted_for):
        payload = json.dumps({"term": term, "voted_for": voted_for}).encode("utf-8")
        replace_atomically(self._state_path, payload)
        super().save_state(term, voted_for)

    def append(self, entry):
        self._wal.append(entry)
        super().append(entry)

    def truncate_from(self, index):
        super().truncate_from(index)
        self._wal.close()
        tmp = WriteAheadLog(self._log_path + ".rewrite")
        for entry in self._log:
            tmp.append(entry)
        tmp.close()
        os.replace(self._log_path + ".rewrite", self._log_path)
        self._wal = WriteAheadLog(self._log_path)

    def close(self):
        self._wal.close()
