"""Batch assembly, threaded submission with backpressure, and retries."""

from __future__ import annotations

import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from solr_datagen.config import COMMIT_WITHIN_MS, MAX_RETRIES
from solr_datagen.data_generator import DataGenerator
from solr_datagen.progress import ProgressTracker
from solr_datagen.solr_client import SolrClient

logger = logging.getLogger(__name__)


class DocumentIndexer:
    """Generate documents in batches and submit them to Solr in parallel."""

    def __init__(
        self,
        solr_client: SolrClient,
        generator: DataGenerator,
        progress: ProgressTracker,
        batch_size: int = 500,
        commit_within_ms: int = COMMIT_WITHIN_MS,
        max_workers: int = 4,
    ):
        self.client = solr_client
        self.generator = generator
        self.progress = progress
        self.batch_size = batch_size
        self.commit_within_ms = commit_within_ms
        self.max_workers = max_workers
        self._stop = threading.Event()

    def run(self, total_docs: int) -> None:
        """Generate and index `total_docs` documents."""
        self.progress.start()

        # Backpressure queue: limits how far ahead generation gets vs. submission.
        work_queue: queue.Queue[list[dict] | None] = queue.Queue(
            maxsize=self.max_workers * 2
        )

        # Producer thread — generates batches and puts them on the queue.
        producer = threading.Thread(
            target=self._produce, args=(total_docs, work_queue), daemon=True
        )
        producer.start()

        # Consumer pool — submits batches to Solr in parallel.
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while True:
                batch = work_queue.get()
                if batch is None:  # sentinel
                    break
                if self._stop.is_set():
                    break
                pool.submit(self._submit_batch, batch)

            # Wait for in-flight submissions to finish.
            pool.shutdown(wait=True)

        producer.join(timeout=5)

        # Final hard commit.
        try:
            self.client.commit()
        except Exception as exc:
            logger.error("Final commit failed: %s", exc)

    def stop(self) -> None:
        """Signal the indexer to stop (used by Ctrl+C handler)."""
        self._stop.set()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _produce(self, total_docs: int, work_queue: queue.Queue) -> None:
        """Generate documents in batches and enqueue them."""
        batch: list[dict] = []
        for i in range(total_docs):
            if self._stop.is_set():
                break
            batch.append(self.generator.generate(i))
            if len(batch) >= self.batch_size:
                work_queue.put(batch)
                batch = []
        if batch and not self._stop.is_set():
            work_queue.put(batch)
        work_queue.put(None)  # sentinel

    def _submit_batch(self, batch: list[dict]) -> None:
        """POST a batch to Solr with exponential-backoff retries."""
        for attempt in range(MAX_RETRIES):
            if self._stop.is_set():
                return
            try:
                self.client.post_documents(batch, self.commit_within_ms)
                self.progress.add(len(batch))
                return
            except Exception as exc:
                wait = 2**attempt
                logger.warning(
                    "Batch POST failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    MAX_RETRIES,
                    exc,
                    wait,
                )
                time.sleep(wait)

        # Exhausted retries.
        logger.error("Batch of %d docs dropped after %d retries", len(batch), MAX_RETRIES)
        self.progress.add_failed_batch()
