# rdfpress

**Turn RDF/XML into something you can actually query.**

RDF/XML is the standard way to publish linked data, but it's notoriously hard to work with using everyday data tools. You can't load it into a DataFrame, query it with SQL, or pipe it through jq without a lot of plumbing. The typical workaround — standing up a SPARQL endpoint — adds infrastructure you may not want or need.

rdfpress converts RDF/XML files into clean JSONL, Parquet, or JSON-LD that you can query directly with DuckDB, Pandas, Polars, jq, or any tool that understands JSON. It handles single files, directories, zip archives, and remote sources over FTP or S3. Processing is parallelized and memory-efficient, so it scales from one record to thousands of large zip archives.

## Why this tool exists

rdfpress was built to enable large-scale data analysis on [Europeana](https://www.europeana.eu/)'s aggregated cultural heritage metadata. Europeana publishes the metadata of all objects in its repository as RDF/XML files on a [public FTP server](https://europeana.atlassian.net/wiki/spaces/EF/pages/2324463617/Dataset+download+and+OAI-PMH+service#Europeana%E2%80%99s-FTP-server) (`ftp://download.europeana.eu/dataset/XML/`), organized as one zip archive per dataset, regenerated weekly. This is why rdfpress supports remote file sources (via [fsspec](https://filesystem-spec.readthedocs.io/), covering FTP, S3, and other protocols) and bulk-processes zip archives efficiently — it was designed to convert Europeana's entire metadata collection into formats suitable for analytical querying.

## How it works

rdfpress reads RDF/XML with [rdflib](https://rdflib.readthedocs.io/) and converts it through a four-step pipeline:

1. **Parse** — rdflib builds an in-memory RDF graph from the XML.
2. **Serialize** — The graph is serialized to JSON-LD, using namespace prefixes inferred from each file's own `xmlns` declarations (no configuration needed).
3. **Simplify** (default) — The verbose JSON-LD structure is restructured into compact, directly queryable JSON (see [below](#what-simplification-does)). Skip this step with `--jsonld` to get standards-compliant JSON-LD instead.
4. **Export** — Output is written as JSONL, gzipped JSONL, or Parquet (via DuckDB).

## What simplification does

JSON-LD is verbose by design. Every value is wrapped in metadata containers like `{"@id": "..."}` or `{"@value": 42, "@type": "xsd:long"}`, and all nodes sit in a flat `@graph` array regardless of type. This is correct for RDF processors, but painful for data analysis — you can't just `SELECT *` or load it into a DataFrame without significant post-processing.

Simplified mode strips that ceremony. It groups nodes by RDF type, unwraps value containers to native JSON types, and removes metadata that becomes redundant after restructuring.

**Before (JSON-LD):**

```json
{
  "@context": { "dc": "http://purl.org/dc/elements/1.1/", "edm": "http://www.europeana.eu/schemas/edm/" },
  "@graph": [
    {
      "@id": "http://example.org/item/123",
      "@type": "edm:ProvidedCHO",
      "dc:title": { "@value": "Mona Lisa", "@language": "en" },
      "dc:date": { "@value": 1503, "@type": "xsd:int" },
      "dc:creator": { "@id": "http://example.org/agent/davinci" }
    },
    {
      "@id": "http://example.org/agent/davinci",
      "@type": "edm:Agent",
      "skos:prefLabel": { "@value": "Leonardo da Vinci", "@language": "en" }
    }
  ]
}
```

**After (simplified):**

```json
{
  "edm:ProvidedCHO": [
    {
      "@id": "http://example.org/item/123",
      "dc:title": { "@value": "Mona Lisa", "@language": "en" },
      "dc:date": 1503,
      "dc:creator": "http://example.org/agent/davinci"
    }
  ],
  "edm:Agent": [
    {
      "@id": "http://example.org/agent/davinci",
      "skos:prefLabel": { "@value": "Leonardo da Vinci", "@language": "en" }
    }
  ]
}
```

What changed:

- **`@graph` array → grouped by `@type`** — each type becomes a top-level key, so you can access all agents or all items directly without filtering.
- **`{"@id": "http://..."}` → `"http://..."`** — URI references become plain strings.
- **`{"@value": 1503, "@type": "xsd:int"}` → `1503`** — typed literals become native JSON values.
- **Language-tagged strings stay as-is** — `{"@value": "...", "@language": "en"}` is preserved because the language tag carries meaningful information.
- **`@type` removed from each node** — redundant with the grouping key.
- **`@context` removed** — namespace prefixes are already baked into the property names.

Nodes without a `@type` are grouped under `_untyped`.

> **Note:** Simplification is a one-way transformation. The output is not valid JSON-LD and cannot be round-tripped back to RDF. Use `--jsonld` when you need to preserve full RDF semantics.

## Installation

Requires [uv](https://docs.astral.sh/uv/). No manual dependency installation needed — `uv run` handles everything automatically.

Alternatively, install as a package:

```bash
pip install git+https://github.com/storytracer/rdfpress.git
```

## Usage

### Convert a single file

```bash
uv run rdfpress.py single record.xml
# → record.json (simplified)

uv run rdfpress.py single record.xml --jsonld
# → record.jsonld (standards-compliant JSON-LD)
```

### Batch-convert many files

```bash
# Directory of XML files → gzipped JSONL
uv run rdfpress.py batch input_dir/ -o out/

# Zip archive → gzipped JSONL
uv run rdfpress.py batch archive.zip -o out/

# Folder of zip archives, 8 parallel workers
uv run rdfpress.py batch zip_folder/ -o out/ -w 8

# Multiple output formats at once
uv run rdfpress.py batch archive.zip -o out/ --format jsonl.gz,parquet

# Remote zips via FTP (or any fsspec-supported URL)
uv run rdfpress.py batch ftp://user:pass@host/path/ -o out/
```

Batch output is stored in per-format subdirectories (e.g. `out/jsonl.gz/`, `out/parquet/`). Resume is enabled by default — if a run is interrupted, rerunning the same command skips already-completed files. Disable with `--no-resume`.

### Query the output

The simplified output is designed for direct use with analytical tools:

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

### Use as a Python library

```bash
pip install git+https://github.com/storytracer/rdfpress.git
```

```python
from rdfpress import parse_rdfxml

data = parse_rdfxml("input.xml")                # simplified JSON
data = parse_rdfxml("input.xml", jsonld=True)    # valid JSON-LD
data = parse_rdfxml(xml_bytes)                   # from raw bytes
```

## Command reference

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

Batch-convert RDF/XML files to JSONL, gzipped JSONL, and/or Parquet. `INPUT_PATH` can be a directory of XML files, a `.zip` archive, a directory of `.zip` archives, or any [fsspec](https://filesystem-spec.readthedocs.io/)-compatible URL (`ftp://`, `s3://`, etc.).

```
uv run rdfpress.py batch [OPTIONS] INPUT_PATH
```

| Option | Description |
|---|---|
| `-o, --output PATH` | Output directory |
| `--format FORMATS` | Comma-separated output formats: `jsonl`, `jsonl.gz`, `parquet` (default: `jsonl.gz`) |
| `--jsonld` | Output valid JSON-LD instead of simplified JSON |
| `--glob PATTERN` | File glob pattern (default: `*.xml`) |
| `-w, --workers INT` | Number of parallel workers (default: number of CPUs) |
| `--compresslevel INT` | Gzip compression level, 0–9 (default: `6`) |
| `--resume / --no-resume` | Skip zips whose output already exists (default: enabled) |
| `--cache-dir PATH` | Cache directory for remote downloads (default: `<output>/.cache`) |

## Limitations

- **Simplified JSON is not round-trippable.** URI references, typed literals, and type annotations are stripped. Use `--jsonld` when you need to preserve full RDF semantics.
- **Schema varies by input.** Different RDF/XML files may produce different top-level keys depending on which types they contain. DuckDB and Polars handle this gracefully via schema inference; rigid column stores may not.
- **Memory is bounded per zip.** Only one zip's entry list and output handle are held in memory at a time, so even very large archives (10 GB+) and large batches (thousands of zips) work efficiently.

## License

MIT
