# solr-datagen

Generate and index realistic documents into Apache Solr at scale.

Given a Solr URL (with collection/core name) and a target document count, `solr-datagen` introspects the schema, generates documents with realistic data across all field types, and indexes them in parallel batches. Works with Solr 7.x, 8.x, and 9.x.

## Features

- **Schema-aware** — automatically discovers fields, types, unique key, and multiValued settings
- **Type-diverse generation** — covers strings, text, integers, longs, floats, doubles, dates, and booleans
- **Solr 7–9 compatible** — handles both Trie (7.x) and Point (8.x/9.x) field type classes transparently
- **Scales to millions** — threaded batch submission with backpressure, `commitWithin` for optimal throughput
- **Reproducible** — optional `--seed` for deterministic output
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
python -m solr_datagen <solr_url> <count> [options]
```

### Examples

```bash
# Dry run — inspect schema without indexing
python -m solr_datagen http://localhost:8983/solr/my_collection 0 --dry-run

# Index 1,000 documents with defaults
python -m solr_datagen http://localhost:8983/solr/my_collection 1000

# Index 1M documents with tuned settings
python -m solr_datagen http://localhost:8983/solr/my_collection 1000000 \
  --batch-size 1000 --workers 8

# With basic auth (Solr 9.x)
python -m solr_datagen http://localhost:8983/solr/my_collection 5000 \
  --auth admin:secret

# Reproducible run
python -m solr_datagen http://localhost:8983/solr/my_collection 500 --seed 42
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `solr_url` | *required* | Solr collection URL, e.g. `http://localhost:8983/solr/my_core` |
| `count` | *required* | Number of documents to generate |
| `-b`, `--batch-size` | 500 | Documents per HTTP request |
| `-c`, `--commit-within` | 5000 | `commitWithin` in milliseconds |
| `-f`, `--max-fields` | 20 | Max fields to select from schema |
| `--fields-per-type` | 3 | Max fields per type category |
| `-w`, `--workers` | 4 | Parallel submission threads |
| `-a`, `--auth` | None | Basic auth as `user:password` |
| `-s`, `--seed` | None | Random seed for reproducibility |
| `--dry-run` | false | Analyse schema only, don't index |
| `-v`, `--verbose` | false | Enable debug logging |

## How It Works

1. **Connect** — validates the Solr URL, detects version and mode (standalone/SolrCloud)
2. **Introspect** — fetches fields and field types from the Schema API, skips internal and non-stored fields
3. **Select** — picks a diverse subset of fields (up to `--max-fields`), ensuring representation across type categories
4. **Generate** — creates documents using pre-computed data pools (via Faker) for high throughput
5. **Index** — submits documents in parallel batches with backpressure, retries, and progress reporting

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
├── indexer.py           # batch submission pipeline
└── progress.py          # progress tracking and reporting
```

## License

MIT
