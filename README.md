# rdfxml2jsonl

Bulk-convert RDF/XML files to queryable JSONL or standards-compliant JSON-LD.

Parses RDF/XML using [rdflib](https://rdflib.readthedocs.io/) and outputs either **simplified JSONL** optimised for analytical querying, or **valid JSON-LD** suitable for RDF pipelines. Handles single files, directories, and zip archives.

## Installation

Requires [uv](https://docs.astral.sh/uv/). No manual dependency installation needed — `uv run` handles everything automatically.

Alternatively, install as a package for library or CLI use:

```bash
pip install git+https://github.com/youruser/rdfxml2jsonl.git
```

## Quick start

```bash
# Single file → simplified JSON
uv run rdfxml2jsonl.py single input.xml

# Single file → valid JSON-LD
uv run rdfxml2jsonl.py single input.xml --jsonld

# Directory of files → gzipped JSONL
uv run rdfxml2jsonl.py batch input_dir/ -o output.jsonl.gz

# Zip archive → gzipped JSONL
uv run rdfxml2jsonl.py batch archive.zip -o output.jsonl.gz
```

## Output modes

### Simplified JSON (default)

Optimised for analytical querying in tools like DuckDB, jq, Pandas, or Polars. The output is structured for direct access by RDF type — no need to unnest or filter arrays.

Transformations applied:

| Input | Output | Rationale |
|---|---|---|
| `@graph` array | Top-level keys per `@type` | Direct column access by type |
| `{"@id": "http://..."}` | `"http://..."` | Plain string, no wrapper |
| `{"@value": 42, "@type": "xsd:long"}` | `42` | Native JSON type |
| `{"@value": "x", "@language": "en"}` | *(unchanged)* | Language tag is meaningful |
| `@type` inside nodes | *(removed)* | Redundant with grouping key |
| `@context` | *(removed)* | Prefixes baked into keys |

Types occurring once are stored as a single object. Types occurring more than once are stored as an array. Nodes without `@type` are grouped under `_untyped`.

> **Note:** This is a one-way transformation. The output is not valid JSON-LD and cannot be round-tripped back to RDF.

### JSON-LD (`--jsonld`)

Standards-compliant JSON-LD with `@context`, `@type`, `@id` wrappers, and typed values preserved exactly as rdflib serialises them. Can be loaded by any JSON-LD processor and converted back to RDF.

```bash
uv run rdfxml2jsonl.py single input.xml --jsonld
uv run rdfxml2jsonl.py batch input_dir/ -o output.jsonl.gz --jsonld
```

## Commands

### `single`

Convert a single RDF/XML file.

```
uv run rdfxml2jsonl.py single [OPTIONS] INPUT_FILE
```

| Option | Description |
|---|---|
| `-o, --output PATH` | Output path. Default: input with `.json` (or `.jsonld`) extension |
| `--indent INT` | JSON indentation level (default: 2) |
| `--jsonld` | Output valid JSON-LD instead of simplified JSON |

### `batch`

Convert many RDF/XML files to a single gzipped JSONL file. Input can be a directory or a zip archive.

```
uv run rdfxml2jsonl.py batch [OPTIONS] INPUT_PATH
```

| Option | Description |
|---|---|
| `-o, --output PATH` | Output file. Default: `<input>.jsonl.gz` |
| `--jsonld` | Output valid JSON-LD instead of simplified JSON |
| `--glob PATTERN` | File glob pattern (default: `*.xml`) |

Each input file becomes one JSON line in the output. A `_source_file` field is added to every record for traceability.

## Library use

The script is importable as a Python module:

```bash
pip install git+https://github.com/youruser/rdfxml2jsonl.git
```

```python
from rdfxml2jsonl import parse_rdfxml

# Simplified JSON
data = parse_rdfxml("input.xml")

# Valid JSON-LD
data = parse_rdfxml("input.xml", jsonld=True)

# From raw bytes (e.g. read from an API, database, or zip)
data = parse_rdfxml(xml_bytes)
```

## How it works

1. **Parse** — rdflib reads the RDF/XML and builds an in-memory RDF graph.
2. **Serialise** — The graph is serialised to JSON-LD with a compact context derived from the file's own `xmlns` namespace declarations.
3. **Simplify** (unless `--jsonld`) — The `@graph` array is regrouped by `@type`, and JSON-LD value wrappers are unwrapped where no information is lost.

Namespace prefixes are inferred from each file, so the converter works with any RDF vocabulary without configuration.

## Querying the output

The simplified JSONL output is designed for direct use with analytical tools:

```sql
-- DuckDB
SELECT * FROM read_json('output.jsonl.gz') LIMIT 10;

-- Convert to Parquet for faster repeated queries
COPY (SELECT * FROM read_json('output.jsonl.gz'))
TO 'output.parquet' (FORMAT PARQUET);
```

```bash
# jq
zcat output.jsonl.gz | jq '._source_file'
```

```python
# Pandas
import pandas as pd
df = pd.read_json("output.jsonl.gz", lines=True)

# Polars
import polars as pl
df = pl.read_ndjson("output.jsonl.gz")
```

## Limitations

- **Simplified JSON is not round-trippable.** URI references, typed literals, and type annotations are stripped. Use `--jsonld` when you need to preserve full RDF semantics.
- **Schema varies by input.** Different RDF/XML files may produce different top-level keys depending on which types they contain. DuckDB and Polars handle this gracefully via schema inference; rigid column stores may not.
- **Large zip archives are read into memory** one file at a time. The full zip member list is materialised for the progress bar.

## License

MIT