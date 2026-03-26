# QVF Metadata Extractor

🧩 A focused utility for reading Qlik `.qvf` files directly and packaging the discovered metadata into a downloadable ZIP archive.

The project now follows a host-native approach:

- upload a `.qvf` file through the web UI
- analyze the file with a Python extractor
- download the extracted metadata package
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
- `summary.txt`
- `raw/blocks.jsonl`
- `raw/decoded-objects.jsonl`
- `raw/string-findings.txt`
- `raw/unknown-blocks.json`

## 🔄 Processing Flow

When a `.qvf` file is uploaded, the application performs the following workflow:

1. The file is stored in a temporary job folder under `runtime/tmp/`.
2. The Node.js web service invokes the Python extractor.
3. The extractor scans the QVF structure for `gzjson` and binary records.
4. Structured payloads are decoded into JSON output files.
5. The load script is extracted and saved as `script.qvs`.
6. Embedded media items are exported when they can be cleanly bounded.
7. A ZIP archive is created and returned to the browser.
8. After processing, the uploaded file and temporary extraction folder are removed.

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
  - creates the downloadable ZIP archive

This keeps the web layer small and makes the extractor reusable as a standalone CLI tool.

## 🖥️ Web Interface

The web interface is available on:

- `http://<server-ip>:5165`

The page is intentionally compact and task-oriented. It includes:

- a `.qvf` file input
- a primary action to start extraction
- live upload and processing status
- an English explanation of the file lifecycle
- an English summary of the runtime components

## 🐍 Running The Extractor Directly

The Python extractor can also be run without the web UI, which makes local analysis on macOS or Linux straightforward as long as Python 3 is available.

Example:

```bash
python3 scripts/extract_qvf.py "Asset Management.qvf" \
  --output-dir artifacts/output/asset-management \
  --zip artifacts/output/asset-management.zip
```

This creates the extracted metadata folder and a matching ZIP archive.

## 🚀 Installation And Startup

### Ubuntu 24.04 server

After cloning the repository on the target server:

```bash
./start.sh
```

The script:

- installs base packages on Ubuntu if needed
- installs Node.js if it is missing or too old
- verifies Python availability
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

## 🗂️ Temporary Data And Cleanup

Runtime data is stored under `runtime/` and is not meant for source control.

Cleanup happens in two ways:

- immediately after a successful extraction
- automatically for stale job folders older than the configured TTL

If you need to inspect a failed run, set:

```env
KEEP_FAILED_JOBS=true
```

Failed jobs are then preserved under `runtime/tmp/jobs/<job-id>/` together with a `failure-report.txt`.

## 📦 Output Notes

The extractor is designed to be transparent about what it can and cannot decode.

That means the output intentionally contains both:

- normalized metadata files such as `sheets.json`, `measures.json`, and `script.qvs`
- raw evidence files such as `raw/decoded-objects.jsonl` and `raw/unknown-blocks.json`

This makes it easier to extend the parser over time without losing traceability back to the original file structure.

## ⚠️ Scope And Limits

This project does **not** claim to be a drop-in replacement for `qlik app unbuild`.

Instead, it provides a Linux- and macOS-friendly extraction path that works by analyzing the QVF file structure directly. Many modern QVF files expose rich structured metadata this way, but some binary blocks remain opaque and are explicitly listed as such in the output.

## 📌 Operational Notes

- Open TCP port `5165` in the Hetzner firewall if the interface must be reachable externally.
- The service currently runs over HTTP only.
- The extraction path is host-native and does not depend on Docker.
- The Python CLI is suitable for local offline analysis when no web service is needed.

## ❤️ Practical Summary

This repository provides a controlled way to:

- upload a Qlik app
- decode as much metadata as possible directly from the `.qvf`
- keep the load script as a real `.qvs` file
- download the result as a ZIP package
- clean up temporary server-side files automatically
