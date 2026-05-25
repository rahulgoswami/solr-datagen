"""Verify the uniqueKey field type supports cursorMark-based deep paging."""

from __future__ import annotations

# TextField is not sortable (analyzed, no raw term ordering).
# SortableTextField stores a docValues copy and IS sortable.
_NON_SORTABLE = frozenset({"solr.TextField"})


def is_sortable_for_cursor(field_info: dict, field_type: dict) -> tuple[bool, str]:
    """Return (ok, reason). False means cursorMark sort on this field will fail."""
    klass = field_type.get("class", "")
    short = "solr." + klass.rsplit(".", 1)[-1]
    if short in _NON_SORTABLE:
        return False, f"uniqueKey type {klass!r} is not sortable — cannot use cursorMark deep paging"

    # Field must be indexed or have docValues for Solr to sort on it.
    docvalues = field_info.get("docValues", field_type.get("docValues", False))
    indexed = field_info.get("indexed", field_type.get("indexed", True))
    if not (docvalues or indexed):
        return False, "uniqueKey field must be indexed or have docValues=true for cursorMark sort"

    return True, ""
