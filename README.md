# QVF Metadata Extractor

📦 A focused web utility for extracting metadata from Qlik `.qvf` files through a local Qlik Core engine.

The application provides a simple browser-based workflow:

- upload a `.qvf` file
- process it through Qlik Core and `qlik-cli`
- download the extracted metadata as a ZIP archive

The service is designed for self-hosting on Ubuntu `24.04.x` and exposes the web interface on:

- `http://<server-ip>:5165`

## ✨ Overview

QVF Metadata Extractor is intended for cases where a Qlik application needs to be inspected outside a broader Qlik environment. Instead of opening the app manually in a desktop workflow, the uploaded file is processed on the server and transformed into a structured metadata export.

The project focuses on:

- a minimal and clear user flow
- temporary handling of uploaded files
- automatic cleanup after processing
- deployment through `git clone` and `./start.sh`

## 🧭 What The Tool Does

When a `.qvf` file is uploaded through the web interface, the application performs the following workflow:

1. The uploaded file is stored in a temporary folder on the server.
2. The file is imported into a local Qlik Core engine instance.
3. `qlik-cli` runs the `app unbuild` command against the imported app.
4. The extracted metadata files are packaged into a ZIP archive.
5. The ZIP archive is returned to the browser as a download.
6. The uploaded file, extracted temporary files, and temporary engine app are removed.

This keeps the server focused on short-lived processing rather than long-term storage.

## ⚙️ Runtime Architecture

The solution uses two runtime components:

- **Node.js web application**
- **Qlik Core engine running locally in Docker**

This runtime split is intentional:

- the web application is the only public entrypoint
- the Qlik Core engine is bound to `127.0.0.1:9076`
- the browser never connects directly to the engine

## 🖥️ Web Interface

The web interface is intentionally compact and task-oriented. It includes:

- a file input for `.qvf` uploads
- a primary action to start extraction
- visible status messages during processing
- an English explanation of what happens to the uploaded app
- an English overview of the technology used

The UI is styled to remain clean, business-like, and easy to understand on both desktop and mobile screens.

## 🔄 Processing Flow

The backend exposes a small HTTP surface:

- `GET /`
- `POST /api/extract`
- `GET /healthz`

The extraction endpoint performs these tasks:

- validates the uploaded file
- writes it to a temporary job folder
- imports the app via `qlik app import`
- runs `qlik app unbuild`
- creates a ZIP archive from the extracted output
- streams the ZIP file back to the browser
- removes temporary files and the temporary imported app

Version 1 processes one upload at a time. If another extraction is already running, the service returns a busy response instead of running parallel jobs.

## 🧰 Technology Used

The repository uses the following components:

- **Node.js**
- **Express**
- **Multer**
- **Archiver**
- **Qlik CLI**
- **Qlik Core Engine**
- **Docker** on the server for the engine runtime only

## 🚀 Installation And Startup

This repository is intended for Ubuntu `24.04.x` LTS.

After cloning the repository on the target server, the full installation and startup process is handled by:

```bash
./start.sh
```

The script is responsible for:

- validating the operating system
- installing required system packages
- installing Docker Engine
- installing Node.js
- installing npm dependencies
- downloading and installing `qlik-cli`
- pulling the pinned Qlik Core engine image
- starting the engine locally
- starting the web application on port `5165`

To stop the service:

```bash
./stop.sh
```

## 🧱 Configuration

Default runtime values are listed in [.env.example](./.env.example).

Key defaults:

```env
PORT=5165
HOST=0.0.0.0
ENGINE_URL=127.0.0.1:9076
MAX_UPLOAD_MB=512
TMP_ROOT=./runtime/tmp
JOB_TTL_MINUTES=30
QLIK_BIN=./bin/qlik
ENGINE_CONTAINER_NAME=qlik-engine
KEEP_FAILED_JOBS=false
```

## 🗂️ Temporary Data And Cleanup

Runtime data is created under the local `runtime/` folder structure and is not meant for source control.

Cleanup happens in two ways:

- immediately after a successful or failed extraction
- periodically for stale job folders older than the configured TTL

This keeps the service aligned with temporary processing rather than file retention.

If you need to inspect failed imports, set:

```env
KEEP_FAILED_JOBS=true
```

When enabled, failed job folders are preserved under `runtime/tmp/jobs/<job-id>/` together with a `failure-report.txt` file for debugging.

## 🧷 Engine Image Retention

The Qlik Core engine image is pinned in [config/engine-image.env](./config/engine-image.env) by tag and digest.

To create an offline archive of the engine image:

```bash
./scripts/backup-engine-image.sh
```

To restore it later:

```bash
./scripts/restore-engine-image.sh
```

This is useful when the engine image must remain available independently of the upstream registry.

## 📌 Operational Notes

- Open TCP port `5165` in the Hetzner firewall if the interface must be reachable externally.
- The Qlik Core engine itself is intentionally not exposed publicly.
- The service is currently HTTP-only.
- The repository is designed for direct server deployment rather than local container-based development.

## ❤️ Practical Summary

This project provides a controlled way to:

- upload a Qlik app
- extract its metadata
- download the result
- remove temporary processing data automatically

The implementation stays deliberately small, operationally clear, and easy to run on a single Ubuntu server.

## 🔗 References

- [Qlik CLI install](https://qlik.dev/toolkits/qlik-cli/install-qlik-cli/)
- [Qlik CLI app import](https://qlik.dev/toolkits/qlik-cli/app/app-import/)
- [Qlik CLI app unbuild](https://qlik.dev/toolkits/qlik-cli/app/app-unbuild/)
- [Qlik CLI app rm](https://qlik.dev/toolkits/qlik-cli/app/app-rm/)
- [Qlik Core engine image](https://hub.docker.com/r/qlikcore/engine)
