import threading
from contextlib import contextmanager

_local = threading.local()


@contextmanager
def request_cache_context():
    cache = {}
    previous = getattr(_local, "cache", None)
    _local.cache = cache
    try:
        yield cache
    finally:
        _local.cache = previous


def get_request_cache():
    return getattr(_local, "cache", None)


def memoize_request(key_builder):
    def decorator(func):
        def wrapper(*args, **kwargs):
            cache = get_request_cache()
            if cache is None:
                return func(*args, **kwargs)

            key = key_builder(*args, **kwargs)
            if key in cache:
                return cache[key]

            value = func(*args, **kwargs)
            cache[key] = value
            return value

        return wrapper

    return decorator
