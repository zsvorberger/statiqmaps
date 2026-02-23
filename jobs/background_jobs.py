import threading
import queue
import time
import uuid
from typing import Any, Callable, Dict, List, Optional


class BackgroundJobRunner:
    """Simple in-process background job queue with worker threads."""

    def __init__(self, workers: int = 2):
        self._queue: "queue.Queue[tuple]" = queue.Queue()
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._workers: List[threading.Thread] = []
        self._worker_count = max(1, workers)
        self._start_workers()

    def _start_workers(self) -> None:
        for idx in range(self._worker_count):
            t = threading.Thread(target=self._worker, name=f"job-worker-{idx}", daemon=True)
            t.start()
            self._workers.append(t)

    def _ensure_workers(self) -> None:
        if any(t.is_alive() for t in self._workers):
            return
        self._workers = []
        self._start_workers()

    def enqueue(
        self,
        job_type: str,
        user_id: Optional[str],
        target: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        self._ensure_workers()
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "type": job_type,
            "user_id": user_id,
            "status": "queued",
            "created_at": time.time(),
            "started_at": None,
            "ended_at": None,
            "error": None,
        }
        with self._lock:
            self._jobs[job_id] = job
        self._queue.put((job_id, target, args, kwargs))
        return job

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def list_jobs(self, job_type: Optional[str] = None, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            rows = []
            for job in self._jobs.values():
                if job_type and job["type"] != job_type:
                    continue
                if user_id and job["user_id"] != user_id:
                    continue
                rows.append(dict(job))
            return rows

    def _worker(self) -> None:
        while True:
            job_id, target, args, kwargs = self._queue.get()
            with self._lock:
                job = self._jobs.get(job_id)
                if job:
                    job["status"] = "running"
                    job["started_at"] = time.time()
            try:
                call_kwargs = dict(kwargs)
                call_kwargs.setdefault("job_id", job_id)
                target(*args, **call_kwargs)
                error = None
                status = "completed"
            except Exception as exc:
                error = str(exc)
                status = "error"
            finally:
                with self._lock:
                    job = self._jobs.get(job_id)
                    if job:
                        job["status"] = status
                        job["ended_at"] = time.time()
                        job["error"] = error
                self._queue.task_done()


job_runner = BackgroundJobRunner(workers=2)
