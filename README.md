# solr-datagen

Generate and index realistic documents into Apache Solr at scale, or reindex existing documents in-place.

Given a Solr URL (with collection/core name), `solr-datagen` can either generate synthetic documents with realistic data across all field types and index them in parallel batches (**index mode**), or read existing documents from the collection and write them back in-place using cursorMark deep paging (**reindex mode**). Works with Solr 7.x through 10.x.

## Features

- **Schema-aware** — automatically discovers fields, types, unique key, and multiValued settings
- **Type-diverse generation** — covers strings, text, integers, longs, floats, doubles, dates, and booleans
- **Solr 7–10 compatible** — handles both Trie (7.x; deprecated but still present through 10.x) and Point (8.x+) field type classes transparently
- **Scales to millions** — threaded batch submission with backpressure and cursorMark deep paging for reindex
- **Reproducible** — optional `--seed` for deterministic output (index mode)
- **Resilient** — exponential-backoff retries on batch failures, graceful Ctrl+C handling

## Requirements

- Python 3.9+
- A running Apache Solr instance with at least one collection/core

## Installation

```bash
git clone https://github.com/rahulgoswami/solr-datagen.git
cd solr-datagen
pip install -r requirements.txt
```

## Usage

```bash
# Index mode (generate and index synthetic documents)
python -m solr_datagen index <solr_url> <count> [options]

# Reindex mode (read existing docs and write them back in-place)
python -m solr_datagen reindex <solr_url> [options]
```

The legacy form `python -m solr_datagen <solr_url> <count>` (without a subcommand) still works and is equivalent to `index`.

### Index mode examples

```bash
# Dry run — inspect schema without indexing
python -m solr_datagen index http://localhost:8983/solr/my_collection 0 --dry-run

# Index 1,000 documents with defaults
python -m solr_datagen index http://localhost:8983/solr/my_collection 1000

# Index 1M documents with tuned settings
python -m solr_datagen index http://localhost:8983/solr/my_collection 1000000 \
  --batch-size 1000 --workers 8

# With basic auth
python -m solr_datagen index http://localhost:8983/solr/my_collection 5000 \
  --auth admin:secret

# Always include specific fields alongside the diverse selection
python -m solr_datagen index http://localhost:8983/solr/my_collection 1000 \
  --include-fields title,price,category

# Reproducible run
python -m solr_datagen index http://localhost:8983/solr/my_collection 500 --seed 42
```

### Reindex mode examples

```bash
# Reindex all documents in the collection
python -m solr_datagen reindex http://localhost:8983/solr/my_collection

# Reindex only documents matching a filter query
python -m solr_datagen reindex http://localhost:8983/solr/my_collection \
  --fq "category:electronics"

# Reindex with more workers and a larger read page
python -m solr_datagen reindex http://localhost:8983/solr/my_collection \
  --workers 8 --read-batch-size 5000

# With basic auth
python -m solr_datagen reindex http://localhost:8983/solr/my_collection \
  --auth admin:secret
```

### Options

**Shared options (both modes)**

| Flag | Default | Description |
|------|---------|-------------|
| `-b`, `--batch-size` | 500 | Documents per HTTP POST request |
| `-c`, `--commit-within` | 0 | `commitWithin` in ms; 0 disables per-batch commits (see note below) |
| `-w`, `--workers` | 4 | Parallel submission threads |
| `-a`, `--auth` | None | Basic auth as `user:password` |
| `-v`, `--verbose` | false | Enable debug logging |

**Index mode only**

| Flag | Default | Description |
|------|---------|-------------|
| `count` | *required* | Number of documents to generate |
| `-f`, `--max-fields` | 20 | Max fields to select from schema |
| `--fields-per-type` | 3 | Max fields per type category |
| `-s`, `--seed` | None | Random seed for reproducibility |
| `--include-fields` | None | Comma-separated field names to always include, in addition to the diverse selection; unknown or unstored names raise an error before indexing starts |
| `--dry-run` | false | Analyse schema only, don't index |

**Reindex mode only**

| Flag | Default | Description |
|------|---------|-------------|
| `--fq` | None | Filter query to limit the documents reindexed (Solr syntax); omit to reindex all |
| `--read-batch-size` | 2000 | Documents per cursorMark read page |

### Commit behavior (changed in this release)

Both modes now default to `--commit-within 0`, meaning no per-batch `commitWithin` is sent to Solr. Instead, the tool relies on the collection's server-side `autoCommit` (typically every 15 seconds with `openSearcher=false` in modern Solr configsets) for tlog management during the run, and issues a single explicit hard commit at the end to make all documents visible and durable.

**If your collection does not have `autoCommit` configured** (uncommon in production, but possible in dev/test), the transaction log will grow unbounded during a large run. In that case, pass `-c 60000` or similar to enable periodic per-batch commits.

### Reindex behavior

Reindex mode reads documents from the collection using Solr's cursorMark API (deep paging sorted by the unique key), strips fields that should not be re-posted, and writes them back in-place.

**Fields stripped before reposting:**
- All `copyField` destinations (they will be repopulated from their source fields on reindex)
- `_root_` and `_nest_path_` (nested document markers — see limitations)

**`_version_` is preserved.** Each document is re-posted with its current `_version_` value. If the document was modified by another writer between the read and the write, Solr will reject the reindex of that document (HTTP 409). The batch is counted as a failed batch; the run continues with subsequent batches.

**Limitations:**
- Fields with `stored=false` are not retrievable. If a copyField source is not stored, its data will be absent in the reindexed document.
- Nested/child documents are not supported in this release. Parent-child relationships (`_root_`, `_nest_path_`) are stripped, which may leave orphaned children.
- If the index is being actively written to during a reindex run, cursorMark may skip or duplicate some documents. The `_version_` check will surface accidental concurrent overwrites as failed batches.

## How It Works

### Index mode
1. **Connect** — validates the Solr URL, detects version and mode (standalone/SolrCloud)
2. **Introspect** — fetches fields and field types from the Schema API, skips internal and non-stored fields
3. **Select** — picks a diverse subset of fields (up to `--max-fields`), ensuring representation across type categories, plus the unique key, required fields, and any `--include-fields` names
4. **Generate** — creates documents using pre-computed data pools (via Faker) for high throughput
5. **Index** — submits documents in parallel batches with backpressure, retries, and progress reporting

### Reindex mode
1. **Connect** — same as above
2. **Schema precheck** — fetches unique key, field type, and copyField rules; verifies the unique key is sortable (required for cursorMark)
3. **Read** — pages through all (or filtered) documents using cursorMark sorted by unique key; each page strips copyField destinations before batching
4. **Index** — same parallel batching pipeline as index mode; `_version_` is preserved for optimistic concurrency

## Project Structure

```
solr_datagen/
├── __init__.py
├── __main__.py          # python -m solr_datagen entry point
├── cli.py               # argument parsing and orchestration
├── config.py            # constants and field-type mappings
├── solr_client.py       # Solr HTTP client
├── schema_analyzer.py   # schema introspection and field selection
├── data_generator.py    # per-type random data generation
├── indexer.py           # batch submission pipeline (DocSource protocol)
├── reindex_source.py    # cursorMark-based document reader for reindex mode
├── copyfield_filter.py  # copyField destination matcher
├── sort_check.py        # uniqueKey sortability precheck for cursorMark
└── progress.py          # progress tracking and reporting
```
