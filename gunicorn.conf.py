import multiprocessing


bind = "0.0.0.0:8000"

# Use CPU-bound recommendation: (2 x cores) + 1, but never fewer than 3 workers
workers = max(3, multiprocessing.cpu_count() * 2 + 1)

# Keep threads at 1 so each worker runs a single greenlet/event loop
threads = 1

# Allow see-through timeouts for long-running Strava syncs
timeout = 120
graceful_timeout = 30
keepalive = 5

worker_class = "gthread"
worker_connections = 1000

# Avoid preloading so caches stay per-worker and the request cache hooks initialize correctly
preload_app = False

loglevel = "info"
accesslog = "-"
errorlog = "-"

# Forward important environment variables if needed (set via CLI/env when invoking Gunicorn)
raw_env = [
    # "FLASK_ENV=production",
    # "OTHER_ENV=value",
]
