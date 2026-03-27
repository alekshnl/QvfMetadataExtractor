# QVF Metadata And Table Extractor

🧩 A focused utility for reading Qlik `.qvf` files directly, surfacing discoveries in a browser workspace, and packaging the discovered metadata and reconstructed tables into a downloadable ZIP archive.

The project now follows a host-native approach:

- upload a `.qvf` file through the web UI
- analyze the file with a Python extractor
- inspect discoveries in a tabbed browser workspace
- download the extracted metadata package when needed
- remove the temporary upload and job folder afterward

No Qlik runtime, Docker container, or external engine is required for the extraction path.

## ✨ What This Tool Does

The extractor reads the `.qvf` file format directly and pulls out the structured information that can be decoded without opening the app in Qlik Sense.

The ZIP result is designed to be useful for both human inspection and follow-up parser work. It typically includes:

- `manifest.json`
- `app.json`
- `sheets.json`
- `masterobjects.json`
- `measures.json`
- `dimensions.json`
- `variables.json`
- `script.qvs`
- `data-sources.json`
- `assets.json`
- `data-model.json`
- `load-model.json`
- `color-maps.json`
- `tables/_manifest.json`
- `tables/_confidence.json`
- `tables/<table>.parquet`
- `tables/<table>.tsv`
- `summary.txt`
- `raw/blocks.jsonl`
- `raw/decoded-objects.jsonl`
- `raw/non-scalar-streams.json`
- `raw/string-findings.txt`
- `raw/unknown-blocks.json`
- `raw/table-block-mapping.json`

## 🔄 Processing Flow

When a `.qvf` file is uploaded, the application performs the following workflow:

1. The file is stored in a temporary job folder under `runtime/tmp/`.
2. The Node.js web service invokes the Python extractor.
3. The extractor scans the QVF structure for `gzjson` and binary records.
4. Structured payloads are decoded into JSON output files.
5. The load script is extracted and saved as `script.qvs`.
6. Reconstructed tables are written as `Parquet` and `TSV`, with confidence markers and source-block provenance.
7. Embedded media items are exported when they can be cleanly bounded.
8. The Node.js layer builds a UI-ready analysis payload around the extractor output.
9. The browser renders the tabbed analysis workspace and can fetch the ZIP separately.
10. Temporary job folders remain available until the configured TTL expires.

Version 1 processes one upload at a time. If another extraction is already running, the service returns a busy response instead of running jobs in parallel.

## 🧠 Runtime Design

The repository contains two cooperating parts:

- **Node.js web layer**
  - serves the UI
  - accepts uploads
  - manages temporary job folders
  - streams the ZIP result back to the browser
- **Python extractor**
  - parses the QVF file directly
  - decodes structured payload blocks
  - writes normalized metadata files
  - attempts best-effort row and lookup table reconstruction
  - exports tables as `Parquet` and `TSV`
  - creates the downloadable ZIP archive

This keeps the web layer small and makes the extractor reusable as a standalone CLI tool.

## 🖥️ Web Interface

The web interface is available on:

- `http://<server-ip>:5165`

The page now focuses on discovery first. It includes:

- an empty-state upload workspace
- a primary action to analyze a `.qvf`
- a multi-tab analysis view with overview, structure, design, tables, script, and assets
- live processing status and error states
- a secondary ZIP download action tied to the active analysis job
- support for keeping multiple analysis jobs in browser state for future compare workflows

## 🔌 Web API

Primary routes:

- `POST /api/analyze`
  - uploads a `.qvf`
  - runs the extractor
  - returns JSON with `jobId`, `appLabel`, `downloadUrl`, and `analysis`
- `GET /api/jobs/:jobId/analysis`
  - reloads a saved analysis while the job is still inside TTL
- `GET /api/jobs/:jobId/download`
  - downloads the ZIP package for a saved analysis job
- `GET /api/jobs/:jobId/assets/:filename`
  - serves extracted assets used by the analysis workspace

Compatibility route:

- `POST /api/extract`
  - keeps the older ZIP-only workflow intact

## 🐍 Running The Extractor Directly

The Python extractor can also be run without the web UI, which makes local analysis on macOS or Linux straightforward as long as Python 3 is available.

Install the Python dependency first:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Example:

```bash
.venv/bin/python scripts/extract_qvf.py "Asset Management.qvf" \
  --output-dir artifacts/output/asset-management \
  --zip artifacts/output/asset-management.zip
```

This creates the extracted metadata folder, table exports, and a matching ZIP archive.

## 🚀 Installation And Startup

### Ubuntu 24.04 server

After cloning the repository on the target server:

```bash
./start.sh
```

The script:

- installs base packages on Ubuntu if needed
- installs Node.js if it is missing or too old
- creates a local Python virtual environment
- installs Python requirements from `requirements.txt`
- installs npm dependencies
- creates runtime folders
- starts the web application on port `5165`

To stop the service:

```bash
./stop.sh
```

### macOS or other local environments

The Python extractor itself can run without Docker as long as Python 3 is available.

If you also want the web UI locally, ensure `node`, `npm`, and `python3` are installed and then run:

```bash
./start.sh
```

## ⚙️ Configuration

Default runtime values are listed in [.env.example](./.env.example).

Key defaults:

```env
PORT=5165
HOST=0.0.0.0
MAX_UPLOAD_MB=512
TMP_ROOT=./runtime/tmp
JOB_TTL_MINUTES=30
PYTHON_BIN=python3
EXTRACTOR_SCRIPT=./scripts/extract_qvf.py
KEEP_FAILED_JOBS=false
```

If `PYTHON_BIN` is not set, the server prefers `./.venv/bin/python` when it exists and falls back to `python3` otherwise.

The Python dependency list lives in `requirements.txt`. The current extractor uses `pyarrow` for Parquet output.

## 🗂️ Temporary Data And Cleanup

Runtime data is stored under `runtime/` and is not meant for source control.

Cleanup happens automatically for stale job folders older than the configured TTL.

If you need to inspect a failed run, set:

```env
KEEP_FAILED_JOBS=true
```

Failed jobs are then preserved under `runtime/tmp/jobs/<job-id>/` together with a `failure-report.txt`.

## 📦 Output Notes

The extractor is designed to be transparent about what it can and cannot decode.

That means the output intentionally contains both:

- normalized metadata files such as `sheets.json`, `measures.json`, and `script.qvs`
- reconstructed table files under `tables/`
- confidence files such as `tables/_confidence.json`
- raw evidence files such as `raw/decoded-objects.jsonl`, `raw/non-scalar-streams.json`, `raw/unknown-blocks.json`, and `raw/table-block-mapping.json`

This makes it easier to extend the parser over time without losing traceability back to the original file structure.

## ⚠️ Scope And Limits

This project does **not** claim to be a drop-in replacement for `qlik app unbuild`.

Instead, it provides a Linux- and macOS-friendly extraction path that works by analyzing the QVF file structure directly. Many modern QVF files expose rich structured metadata this way, and some tables can be reconstructed exactly or partially, but other binary blocks remain opaque and are explicitly listed as such in the output.

## 📌 Operational Notes

- Open TCP port `5165` in the Hetzner firewall if the interface must be reachable externally.
- The service currently runs over HTTP only.
- The extraction path is host-native and does not depend on Docker.
- The Python CLI is suitable for local offline analysis when no web service is needed.

## ❤️ Practical Summary

This repository provides a controlled way to:

- upload a Qlik app
- decode as much metadata as possible directly from the `.qvf`
- inspect discoveries in a browser workspace before downloading raw output
- keep the load script as a real `.qvs` file
- export reconstructed model tables as `Parquet` and `TSV`
- tell you which table columns are exact, heuristic, partial, or still missing
- download the result as a ZIP package
- clean up temporary server-side files automatically
