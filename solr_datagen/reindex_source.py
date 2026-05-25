"""Document source that reads existing docs from Solr via cursorMark deep paging."""

from __future__ import annotations

import logging
import threading
import time
from typing import Iterator

import requests as _requests

from solr_datagen.config import MAX_RETRIES, READ_BATCH_SIZE
from solr_datagen.copyfield_filter import build_drop_matcher
from solr_datagen.solr_client import SolrClient

logger = logging.getLogger(__name__)

_CURSOR_START = "*"
# HTTP status codes that are worth retrying on the read side.
_RETRIABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class ReindexSource:
    """Reads docs from Solr via cursorMark, strips copyField dests, yields POST batches.

    Limitations:
    - Fields with stored=false are not retrievable; data will be absent in reindexed docs
      if their copyField sources are also not stored.
    - Nested/child documents are not supported; _root_ and _nest_path_ are stripped.
    - If the index is being written to concurrently, cursorMark may skip or duplicate
      docs; _version_ checks will surface accidental overwrites.
    """

    def __init__(
        self,
        client: SolrClient,
        *,
        unique_key: str,
        copy_field_rules: list[dict],
        fq: str | None = None,
        read_batch_size: int = READ_BATCH_SIZE,
        post_batch_size: int = 500,
    ):
        self.client = client
        self.unique_key = unique_key
        self.fq = fq
        self.read_batch_size = read_batch_size
        self.post_batch_size = post_batch_size
        self._should_drop = build_drop_matcher(copy_field_rules)
        self._total: int | None = None

    def total(self) -> int:
        """Return numFound for the configured query (result is cached)."""
        if self._total is None:
            resp = self._fetch_page_with_retry(
                sort=f"{self.unique_key} asc",
                cursor=_CURSOR_START,
                rows=0,
                stop=threading.Event(),
            )
            self._total = resp["response"]["numFound"]
        return self._total

    def iter_batches(self, stop: threading.Event) -> Iterator[list[dict]]:
        """Yield POST-sized batches of cleaned docs using cursorMark deep paging."""
        cursor = _CURSOR_START
        sort = f"{self.unique_key} asc"
        pending: list[dict] = []

        while not stop.is_set():
            page = self._fetch_page_with_retry(sort, cursor, self.read_batch_size, stop)
            if page is None:
                break  # stop was set during retry

            docs = page["response"]["docs"]
            next_cursor = page["nextCursorMark"]

            for doc in docs:
                if stop.is_set():
                    if pending:
                        yield pending
                    return
                cleaned = {k: v for k, v in doc.items() if not self._should_drop(k)}
                pending.append(cleaned)
                if len(pending) >= self.post_batch_size:
                    yield pending
                    pending = []

            if next_cursor == cursor:
                break  # all pages exhausted
            cursor = next_cursor

        if pending and not stop.is_set():
            yield pending

    def _fetch_page_with_retry(
        self,
        sort: str,
        cursor: str,
        rows: int = 0,
        stop: threading.Event | None = None,
    ) -> dict | None:
        """Fetch one cursor page with exponential-backoff retry on transient errors."""
        if stop is None:
            stop = threading.Event()

        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            if stop.is_set():
                return None
            try:
                return self.client.search_cursor(
                    q="*:*",
                    fq=self.fq,
                    sort=sort,
                    rows=rows,
                    cursor_mark=cursor,
                )
            except ConnectionError as exc:
                last_exc = exc
            except _requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in _RETRIABLE_STATUS:
                    last_exc = exc
                else:
                    raise  # 4xx non-retriable: propagate immediately
            except Exception as exc:
                # PermissionError, FileNotFoundError, ValueError — all non-retriable
                raise

            wait = 2 ** attempt
            logger.warning(
                "cursorMark read failed (attempt %d/%d): %s — retrying in %ds",
                attempt + 1, MAX_RETRIES, last_exc, wait,
            )
            time.sleep(wait)

        raise RuntimeError(
            f"cursorMark read failed after {MAX_RETRIES} attempts: {last_exc}"
        )
