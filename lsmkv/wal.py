import json
import os
import zlib


class WriteAheadLog:
    """Append-only log of every mutation, replayed on startup for crash recovery.

    Record format, one per line: ``<crc32 of payload, 8 hex digits> <json payload>``.
    Lines that start with ``{`` are records written before checksums were added
    and are still accepted.

    Recovery policy: replay stops at the first record that is incomplete, fails
    its checksum or does not parse. Everything from that point on is discarded
    and the file is truncated there, so later appends never follow garbage. A
    record is only acknowledged after fsync, so a discarded tail can only hold
    writes that were never acknowledged, unless the disk itself corrupted data.
    """

    def __init__(self, path):
        self.path = path
        self._fh = open(self.path, "ab")

    def append(self, op: dict):
        payload = json.dumps(op).encode("utf-8")
        self._fh.write(b"%08x %s\n" % (zlib.crc32(payload), payload))
        self._fh.flush()
        os.fsync(self._fh.fileno())

    @staticmethod
    def _decode(line: bytes):
        """Return the operation in `line`, or None if the record is not intact."""
        if not line.endswith(b"\n"):
            return None  # torn write
        body = line.rstrip(b"\r\n")
        try:
            if body.startswith(b"{"):
                return json.loads(body)  # legacy record without checksum
            crc_hex, _, payload = body.partition(b" ")
            if len(crc_hex) != 8 or int(crc_hex, 16) != zlib.crc32(payload):
                return None
            return json.loads(payload)
        except ValueError:
            return None

    def replay(self):
        if not os.path.exists(self.path):
            return
        valid_bytes = 0
        damaged = False
        with open(self.path, "rb") as f:
            for line in f:
                if not line.strip():
                    valid_bytes += len(line)
                    continue
                op = self._decode(line)
                if op is None:
                    damaged = True
                    break
                valid_bytes += len(line)
                yield op
        if damaged:
            self._fh.flush()
            os.truncate(self.path, valid_bytes)
            os.fsync(self._fh.fileno())

    def clear(self):
        self._fh.close()
        with open(self.path, "wb") as f:
            f.flush()
            os.fsync(f.fileno())
        self._fh = open(self.path, "ab")

    def close(self):
        self._fh.close()
