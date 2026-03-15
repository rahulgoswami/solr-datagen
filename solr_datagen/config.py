"""Constants and Solr field-type mappings."""

from __future__ import annotations

# Maps Solr field-type class names to internal category labels.
# Covers both Trie (7.x; deprecated but still present through 10.x) and Point (8.x+) variants.
FIELD_TYPE_MAP: dict[str, str] = {
    # String / Text
    "solr.StrField": "string",
    "solr.TextField": "text",
    "solr.SortableTextField": "text",
    # Integer
    "solr.IntPointField": "int",
    "solr.TrieIntField": "int",
    # Long
    "solr.LongPointField": "long",
    "solr.TrieLongField": "long",
    # Float
    "solr.FloatPointField": "float",
    "solr.TrieFloatField": "float",
    # Double
    "solr.DoublePointField": "double",
    "solr.TrieDoubleField": "double",
    # Date
    "solr.DatePointField": "date",
    "solr.TrieDateField": "date",
    "solr.DateRangeField": "date",
    # Boolean
    "solr.BoolField": "boolean",
}

# Internal Solr fields that should never be populated.
INTERNAL_FIELDS: set[str] = {"_version_", "_root_", "_text_", "_nest_path_"}

# Defaults
BATCH_SIZE = 500
COMMIT_WITHIN_MS = 5000
PROGRESS_INTERVAL = 10000
MAX_RETRIES = 3
