import os
import tempfile

from lsmkv.wal import WriteAheadLog


def test_append_and_replay():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    os.remove(path)
    try:
        wal = WriteAheadLog(path)
        wal.append({"type": "put", "key": "a", "value": "1"})
        wal.append({"type": "delete", "key": "b"})
        wal.close()

        wal2 = WriteAheadLog(path)
        ops = list(wal2.replay())
        wal2.close()
        assert ops == [
            {"type": "put", "key": "a", "value": "1"},
            {"type": "delete", "key": "b"},
        ]
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_clear_empties_log():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    os.remove(path)
    try:
        wal = WriteAheadLog(path)
        wal.append({"type": "put", "key": "a", "value": "1"})
        wal.clear()
        assert list(wal.replay()) == []
        wal.close()
    finally:
        if os.path.exists(path):
            os.remove(path)
