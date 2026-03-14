"""Thread-safe progress tracking and reporting."""

import threading
import time


class ProgressTracker:
    """Track indexing progress and print periodic status lines."""

    def __init__(self, total_docs: int, report_interval: int = 10_000, time_interval: float = 5.0):
        self.total_docs = total_docs
        self.report_interval = report_interval
        self.time_interval = time_interval

        self._lock = threading.Lock()
        self._indexed = 0
        self._failed_batches = 0
        self._start_time: float = 0.0
        self._last_report_time: float = 0.0
        self._last_report_count: int = 0

    def start(self) -> None:
        self._start_time = time.monotonic()
        self._last_report_time = self._start_time

    def add(self, count: int) -> None:
        with self._lock:
            self._indexed += count
            now = time.monotonic()
            docs_since = self._indexed - self._last_report_count
            time_since = now - self._last_report_time
            if docs_since >= self.report_interval or time_since >= self.time_interval:
                self._print_status(now)
                self._last_report_count = self._indexed
                self._last_report_time = now

    def add_failed_batch(self) -> None:
        with self._lock:
            self._failed_batches += 1

    @property
    def indexed(self) -> int:
        with self._lock:
            return self._indexed

    @property
    def failed_batches(self) -> int:
        with self._lock:
            return self._failed_batches

    def _print_status(self, now: float) -> None:
        elapsed = now - self._start_time
        rate = self._indexed / elapsed if elapsed > 0 else 0
        pct = (self._indexed / self.total_docs * 100) if self.total_docs > 0 else 0
        remaining = self.total_docs - self._indexed
        eta = int(remaining / rate) if rate > 0 else 0

        elapsed_hms = time.strftime("%H:%M:%S", time.gmtime(elapsed))
        print(
            f"[{elapsed_hms}] Indexed {self._indexed:,} / {self.total_docs:,} "
            f"({pct:.1f}%) | {rate:,.0f} docs/sec | ETA: {eta}s"
        )

    def print_summary(self) -> None:
        elapsed = time.monotonic() - self._start_time
        rate = self._indexed / elapsed if elapsed > 0 else 0
        elapsed_hms = time.strftime("%H:%M:%S", time.gmtime(elapsed))
        print(f"\n--- Indexing Complete ---")
        print(f"Documents indexed: {self._indexed:,}")
        print(f"Failed batches:    {self._failed_batches}")
        print(f"Total time:        {elapsed_hms}")
        print(f"Average rate:      {rate:,.0f} docs/sec")
