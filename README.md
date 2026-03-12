# rdfpress

Bulk-convert RDF/XML files to queryable JSONL, Parquet, or standards-compliant JSON-LD.

Parses RDF/XML using [rdflib](https://rdflib.readthedocs.io/) and outputs either **simplified JSON** optimised for analytical querying, or **valid JSON-LD** suitable for RDF pipelines. Handles single files, directories, zip archives, and remote sources via [fsspec](https://filesystem-spec.readthedocs.io/).

## Installation

Requires [uv](https://docs.astral.sh/uv/). No manual dependency installation needed — `uv run` handles everything automatically.

Alternatively, install as a package for library or CLI use:

```bash
pip install git+https://github.com/youruser/rdfpress.git
```

## Quick start

```bash
# Single file → simplified JSON
uv run rdfpress.py single input.xml

# Single file → valid JSON-LD
uv run rdfpress.py single input.xml --jsonld

# Directory of XML files → gzipped JSONL (default format)
uv run rdfpress.py batch input_dir/ -o out_dir/

# Zip archive → gzipped JSONL
uv run rdfpress.py batch archive.zip -o out_dir/

# Folder of zip archives → one output file per zip
uv run rdfpress.py batch zip_folder/ -o out_dir/ -w 8

# Multiple output formats at once
uv run rdfpress.py batch archive.zip -o out_dir/ --format jsonl.gz,parquet

# Remote zips via FTP (or any fsspec-supported URL)
uv run rdfpress.py batch ftp://user:pass@host/path/ -o out_dir/

# Resume after interruption (skips already-completed zips)
uv run rdfpress.py batch zip_folder/ -o out_dir/ --resume
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

Types occurring once are stored as an array. Nodes without `@type` are grouped under `_untyped`.

> **Note:** This is a one-way transformation. The output is not valid JSON-LD and cannot be round-tripped back to RDF.

### JSON-LD (`--jsonld`)

Standards-compliant JSON-LD with `@context`, `@type`, `@id` wrappers, and typed values preserved exactly as rdflib serialises them. Can be loaded by any JSON-LD processor and converted back to RDF.

## Commands

### `single`

Convert a single RDF/XML file.

```
uv run rdfpress.py single [OPTIONS] INPUT_FILE
```

| Option | Description |
|---|---|
| `-o, --output PATH` | Output path. Default: input with `.json` (or `.jsonld`) extension |
| `--jsonld` | Output valid JSON-LD instead of simplified JSON |

### `batch`

Convert many RDF/XML files to JSONL, gzipped JSONL, and/or Parquet.

`INPUT_PATH` can be a local path or any fsspec-compatible URL (`ftp://`, `s3://`, etc.). Accepts a directory of XML files, a `.zip` archive, or a directory of `.zip` archives.

Output is stored in per-format subdirectories under the output directory (e.g. `out/jsonl.gz/`, `out/parquet/`).

```
uv run rdfpress.py batch [OPTIONS] INPUT_PATH
```

| Option | Description |
|---|---|
| `-o, --output PATH` | Output directory. Per-format subdirectories are created inside |
| `--format FORMATS` | Comma-separated output formats: `jsonl`, `jsonl.gz`, `parquet` (default: `jsonl.gz`) |
| `--jsonld` | Output valid JSON-LD instead of simplified JSON |
| `--glob PATTERN` | File glob pattern (default: `*.xml`) |
| `-w, --workers INT` | Number of parallel workers (default: number of CPUs) |
| `--compresslevel INT` | Gzip compression level, 0–9 (default: `6`) |
| `--resume` | Skip zips whose output already exists (enabled by default) |
| `--cache-dir PATH` | Cache directory for remote downloads (default: `<output>/.cache`) |

## Library use

The script is importable as a Python module:

```bash
pip install git+https://github.com/youruser/rdfpress.git
```

```python
from rdfpress import parse_rdfxml

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
4. **Export** — The JSONL intermediate is converted to each requested output format (gzipped JSONL, Parquet via DuckDB, or plain JSONL).

Namespace prefixes are inferred from each file, so the converter works with any RDF vocabulary without configuration.

## Querying the output

The simplified JSONL output is designed for direct use with analytical tools:

```sql
-- DuckDB: query gzipped JSONL directly
SELECT * FROM read_json('out/jsonl.gz/data.jsonl.gz') LIMIT 10;

-- DuckDB: query Parquet output
SELECT * FROM 'out/parquet/data.parquet' LIMIT 10;
```

```python
# Pandas
import pandas as pd
df = pd.read_json("out/jsonl.gz/data.jsonl.gz", lines=True)

# Polars
import polars as pl
df = pl.read_ndjson("out/jsonl.gz/data.jsonl.gz")
```

## Limitations

- **Simplified JSON is not round-trippable.** URI references, typed literals, and type annotations are stripped. Use `--jsonld` when you need to preserve full RDF semantics.
- **Schema varies by input.** Different RDF/XML files may produce different top-level keys depending on which types they contain. DuckDB and Polars handle this gracefully via schema inference; rigid column stores may not.
- **Processing is bounded per zip.** Only one zip's entry list and output handle are held in memory at a time, so even very large archives (10 GB+) and large batches (thousands of zips) are handled efficiently.

## License

MIT
