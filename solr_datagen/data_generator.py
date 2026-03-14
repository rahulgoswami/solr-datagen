"""Random data generation — pre-computed pools for high throughput."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from faker import Faker

from solr_datagen.schema_analyzer import FieldSpec


class DataGenerator:
    """Generate random Solr documents based on analysed field specs."""

    def __init__(
        self,
        fields: list[FieldSpec],
        unique_key_field: str,
        seed: int | None = None,
    ):
        self.fields = fields
        self.unique_key_field = unique_key_field
        self.rng = random.Random(seed)

        # Pre-compute pools via Faker to avoid per-doc overhead.
        fake = Faker()
        if seed is not None:
            Faker.seed(seed)
        self._word_pool = [fake.word() for _ in range(1000)]
        self._sentence_pool = [fake.sentence() for _ in range(500)]

        # Pre-compute date range boundaries (last 10 years).
        self._date_start = datetime(2015, 1, 1, tzinfo=timezone.utc)
        self._date_range_seconds = int(
            (datetime(2025, 12, 31, tzinfo=timezone.utc) - self._date_start).total_seconds()
        )

    def generate(self, doc_index: int) -> dict:
        """Return a single document dict."""
        doc: dict = {}
        for field in self.fields:
            if field.is_unique_key:
                doc[field.name] = f"doc_{doc_index}"
                continue
            if field.multi_valued:
                count = self.rng.randint(1, 5)
                doc[field.name] = [self._value(field.category) for _ in range(count)]
            else:
                doc[field.name] = self._value(field.category)
        return doc

    def _value(self, category: str):
        """Return a single random value for the given category."""
        if category == "string":
            return self.rng.choice(self._word_pool)
        if category == "text":
            return self.rng.choice(self._sentence_pool)
        if category == "int":
            return self.rng.randint(0, 1_000_000)
        if category == "long":
            return self.rng.randint(0, 1_000_000_000)
        if category == "float":
            return round(self.rng.uniform(0, 10_000), 4)
        if category == "double":
            return round(self.rng.uniform(0, 1_000_000), 8)
        if category == "date":
            offset = self.rng.randint(0, self._date_range_seconds)
            dt = self._date_start + timedelta(seconds=offset)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if category == "boolean":
            return self.rng.choice([True, False])
        return self.rng.choice(self._word_pool)
