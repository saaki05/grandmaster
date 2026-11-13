"""State machines that Raft applies committed commands to.

Commands: {"op": "put", "key", "value"}, {"op": "delete", "key"}, {"op": "get", "key"}.
Reads are log entries too, which makes them linearizable without leases at the
cost of a log write per read.
"""
from ..store import KVStore

_APPLIED_KEY = "\x00raft/last_applied"


class DictStateMachine:
    """Volatile state machine used by the simulator; rebuilt from the log after a crash."""

    def __init__(self):
        self.data = {}

    def apply(self, index, command):
        if command is None:
            return None
        op = command["op"]
        if op == "put":
            self.data[command["key"]] = command["value"]
            return None
        if op == "delete":
            self.data.pop(command["key"], None)
            return None
        if op == "get":
            return self.data.get(command["key"])
        raise ValueError(f"unknown op {op!r}")


class LSMStateMachine:
    """Durable state machine backed by the LSM engine.

    The index of the last applied entry is stored in the engine under a
    reserved key. After a restart Raft re-delivers the log from index 1, and
    entries at or below that index are skipped. The marker is written after the
    command, so a crash in between re-applies one put or delete, which is
    idempotent.
    """

    def __init__(self, data_dir, **store_kwargs):
        self.store = KVStore(data_dir, **store_kwargs)
        self.last_applied = int(self.store.get(_APPLIED_KEY) or 0)

    def apply(self, index, command):
        if index <= self.last_applied:
            return None
        result = None
        if command is not None:
            op = command["op"]
            if op == "put":
                self.store.put(command["key"], command["value"])
            elif op == "delete":
                self.store.delete(command["key"])
            elif op == "get":
                result = self.store.get(command["key"])
            else:
                raise ValueError(f"unknown op {op!r}")
        self.store.put(_APPLIED_KEY, str(index))
        self.last_applied = index
        return result

    def close(self):
        self.store.close()
