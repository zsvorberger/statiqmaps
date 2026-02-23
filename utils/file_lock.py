from contextlib import contextmanager

try:
    import fcntl
except Exception:  # pragma: no cover - platform specific
    fcntl = None


@contextmanager
def file_lock(path):
    if fcntl is None:
        yield
        return
    lock_file = open(path, "w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()
