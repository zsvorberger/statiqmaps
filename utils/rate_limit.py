import time

_LAST_CALLS = {}


def check_rate_limit(key, window_seconds):
    now = time.time()
    last = _LAST_CALLS.get(key)
    if last is not None and last > now:
        _LAST_CALLS.pop(key, None)
        last = None
    if last is not None and (now - last) < window_seconds:
        return False
    _LAST_CALLS[key] = now
    return True
