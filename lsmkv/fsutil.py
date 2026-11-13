"""Small helpers for durable file writes."""
import os


def fsync_dir(path: str) -> None:
    """Make a rename or file creation inside `path` durable.

    Directory fsync is not supported on Windows; there the call is skipped.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except (PermissionError, OSError):
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_durable(path: str, data: bytes) -> None:
    """Write `data` to `path` and fsync it before returning."""
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def replace_atomically(path: str, data: bytes) -> None:
    """Replace `path` with `data` so a crash leaves either the old or the new
    content, never a mixture."""
    tmp = path + ".tmp"
    write_durable(tmp, data)
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(path) or ".")
