"""Solr HTTP client — all interactions with a Solr instance."""

from __future__ import annotations

import logging
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)


class SolrClient:
    """Thin wrapper around the Solr REST API."""

    def __init__(self, solr_url: str, auth: tuple[str, str] | None = None, timeout: int = 30):
        # Normalise URL: strip trailing slash, split on /solr/ to get base + collection.
        solr_url = solr_url.rstrip("/")
        idx = solr_url.find("/solr/")
        if idx == -1:
            raise ValueError(
                f"URL must contain '/solr/<collection>': {solr_url}"
            )
        self.base_url = solr_url[: idx + len("/solr")]  # e.g. http://host:8983/solr
        self.collection = solr_url[idx + len("/solr/"):]  # e.g. my_collection
        if not self.collection:
            raise ValueError("No collection/core name found in URL")

        self.timeout = timeout
        self.session = requests.Session()
        if auth:
            self.session.auth = auth
        self.session.headers.update({"Content-Type": "application/json"})

    # ------------------------------------------------------------------
    # Admin helpers
    # ------------------------------------------------------------------

    def get_version(self) -> str:
        """Return the Solr spec version string (e.g. '9.4.0')."""
        resp = self._get(f"{self.base_url}/admin/info/system")
        return resp["lucene"]["solr-spec-version"]

    def get_mode(self) -> str:
        """Return 'solrcloud' or 'std'."""
        resp = self._get(f"{self.base_url}/admin/info/system")
        return resp.get("mode", "std")

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def get_fields(self) -> list[dict]:
        resp = self._get(f"{self.base_url}/{self.collection}/schema/fields")
        return resp["fields"]

    def get_field_types(self) -> list[dict]:
        resp = self._get(f"{self.base_url}/{self.collection}/schema/fieldtypes")
        return resp["fieldTypes"]

    def get_unique_key(self) -> str:
        resp = self._get(f"{self.base_url}/{self.collection}/schema/uniquekey")
        return resp["uniqueKey"]

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------

    def post_documents(self, docs: list[dict], commit_within: int) -> dict:
        """POST a batch of documents to Solr's update handler."""
        url = f"{self.base_url}/{self.collection}/update?commitWithin={commit_within}"
        return self._post(url, docs)

    def commit(self) -> dict:
        """Issue an explicit hard commit."""
        url = f"{self.base_url}/{self.collection}/update?commit=true"
        return self._post(url, [])

    def get_doc_count(self) -> int:
        resp = self._get(
            f"{self.base_url}/{self.collection}/select",
            params={"q": "*:*", "rows": 0, "wt": "json"},
        )
        return resp["response"]["numFound"]

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
        except requests.ConnectionError:
            raise ConnectionError(f"Cannot connect to Solr at {url}")
        self._check_response(resp)
        return resp.json()

    def _post(self, url: str, body) -> dict:
        try:
            resp = self.session.post(url, json=body, timeout=self.timeout)
        except requests.ConnectionError:
            raise ConnectionError(f"Cannot connect to Solr at {url}")
        self._check_response(resp)
        return resp.json()

    @staticmethod
    def _check_response(resp: requests.Response) -> None:
        if resp.status_code == 401 or resp.status_code == 403:
            raise PermissionError(
                "Authentication required. Use --auth user:password"
            )
        if resp.status_code == 404:
            raise FileNotFoundError(f"Not found: {resp.url}")
        resp.raise_for_status()
