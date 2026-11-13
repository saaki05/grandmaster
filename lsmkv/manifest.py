import json
import os

from .fsutil import replace_atomically


class Manifest:
    """The authoritative list of live SSTables.

    A table exists for the store only once its id is in the manifest. Flush and
    compaction first write and fsync the new table, then swap the manifest
    atomically, then clean up. Files on disk that the manifest does not name
    are leftovers of an interrupted flush or compaction and are removed on open.
    """

    FILENAME = "MANIFEST"

    def __init__(self, data_dir: str):
        self.path = os.path.join(data_dir, self.FILENAME)

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def load(self) -> list[int]:
        with open(self.path, encoding="utf-8") as f:
            return list(json.load(f)["tables"])

    def save(self, table_ids: list[int]) -> None:
        payload = json.dumps({"version": 1, "tables": list(table_ids)}).encode("utf-8")
        replace_atomically(self.path, payload)
