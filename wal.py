import json
import os


class WriteAheadLog:
    """Append-only log of every mutation, replayed on startup for crash recovery."""

    def __init__(self, path):
        self.path = path
        self._fh = open(self.path, "a", encoding="utf-8")

    def append(self, op: dict):
        line = json.dumps(op)
        self._fh.write(line + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def replay(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def clear(self):
        self._fh.close()
        open(self.path, "w").close()
        self._fh = open(self.path, "a", encoding="utf-8")

    def close(self):
        self._fh.close()
