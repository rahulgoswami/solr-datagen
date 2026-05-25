"""Batch assembly, threaded submission with backpressure, and retries."""

from __future__ import annotations

import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, Protocol, runtime_checkable

from solr_datagen.config import COMMIT_WITHIN_MS, MAX_RETRIES
from solr_datagen.data_generator import DataGenerator
from solr_datagen.progress import ProgressTracker
from solr_datagen.solr_client import SolrClient, VersionConflictError

logger = logging.getLogger(__name__)


@runtime_checkable
class DocSource(Protocol):
    """Interface for objects that provide document batches to DocumentIndexer."""
    def total(self) -> int: ...
    def iter_batches(self, stop: threading.Event) -> Iterator[list[dict]]: ...


class GeneratedSource:
    """DocSource that generates synthetic documents via DataGenerator."""

    def __init__(self, generator: DataGenerator, count: int, batch_size: int):
        self._generator = generator
        self._count = count
        self._batch_size = batch_size

    def total(self) -> int:
        return self._count

    def iter_batches(self, stop: threading.Event) -> Iterator[list[dict]]:
        batch: list[dict] = []
        for i in range(self._count):
            if stop.is_set():
                break
            batch.append(self._generator.generate(i))
            if len(batch) >= self._batch_size:
                yield batch
                batch = []
        if batch and not stop.is_set():
            yield batch


class DocumentIndexer:
    """Submit documents to Solr in parallel batches from any DocSource."""

    def __init__(
        self,
        solr_client: SolrClient,
        source: DocSource,
        progress: ProgressTracker,
        commit_within_ms: int = COMMIT_WITHIN_MS,
        max_workers: int = 4,
    ):
        self.client = solr_client
        self.source = source
        self.progress = progress
        self.commit_within_ms = commit_within_ms
        self.max_workers = max_workers
        self._stop = threading.Event()
        self._producer_error: BaseException | None = None

    def run(self) -> None:
        """Index all documents from the source."""
        self.progress.start()

        # Backpressure queue: limits how far ahead the producer gets vs. consumers.
        work_queue: queue.Queue[list[dict] | None] = queue.Queue(
            maxsize=self.max_workers * 2
        )

        producer = threading.Thread(
            target=self._produce, args=(work_queue,), daemon=True
        )
        producer.start()

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while True:
                batch = work_queue.get()
                if batch is None:  # sentinel
                    break
                if self._stop.is_set():
                    break
                pool.submit(self._submit_batch, batch)
            pool.shutdown(wait=True)

        producer.join(timeout=5)

        # Commit whatever was successfully indexed, even on partial runs.
        try:
            self.client.commit()
        except Exception as exc:
            logger.error("Final commit failed: %s", exc)

        if self._producer_error is not None:
            raise RuntimeError("Indexing aborted: producer failed") from self._producer_error

    def stop(self) -> None:
        """Signal the indexer to stop (called by Ctrl+C handler)."""
        self._stop.set()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _produce(self, work_queue: queue.Queue) -> None:
        try:
            for batch in self.source.iter_batches(self._stop):
                if self._stop.is_set():
                    break
                work_queue.put(batch)
        except Exception as exc:
            self._producer_error = exc
            self._stop.set()
            logger.error("Producer failed: %s", exc)
        finally:
            work_queue.put(None)  # sentinel — always sent so consumers drain cleanly

    def _submit_batch(self, batch: list[dict]) -> None:
        """POST a batch to Solr with exponential-backoff retries."""
        conflict_reason: str | None = None
        for attempt in range(MAX_RETRIES):
            if self._stop.is_set():
                return
            try:
                self.client.post_documents(batch, self.commit_within_ms)
                self.progress.add(len(batch))
                return
            except VersionConflictError as exc:
                # Version conflicts won't resolve on retry — fail immediately.
                conflict_reason = str(exc)
                break
            except Exception as exc:
                wait = 2 ** attempt
                logger.warning(
                    "Batch POST failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1, MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)

        if conflict_reason:
            logger.error("Batch of %d docs dropped: version conflict — %s", len(batch), conflict_reason)
        else:
            logger.error("Batch of %d docs dropped after %d retries", len(batch), MAX_RETRIES)
        self.progress.add_failed_batch()
