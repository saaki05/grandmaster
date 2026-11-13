from .sentinel import MISSING as _MISSING


class MemTable:
    """In-memory buffer for the most recent writes. Flushed to an SSTable on disk once full."""

    def __init__(self):
        self.data = {}

    def put(self, key: str, value: str):
        self.data[key] = value

    def delete(self, key: str):
        self.data[key] = None  # tombstone

    def get(self, key: str):
        return self.data.get(key, _MISSING)

    def __len__(self):
        return len(self.data)

    def is_empty(self) -> bool:
        return len(self.data) == 0

    def sorted_items(self):
        return sorted(self.data.items())

    def clear(self):
        self.data = {}
