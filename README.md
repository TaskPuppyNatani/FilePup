# FilePup

FilePup is a safety-first Linux media automation service intended to replace the fragile file-move portions of the current qBittorrent/FileFlows workflow without reimplementing transcoding.

## Core principle

**A failed automation should create clutter, not data loss.**

FilePup must never delete the only known-good copy of a media file. Destructive operations are centralized behind a safety controller and remain hard-disabled in the initial scaffold.

## Intended workflow

```text
qBittorrent completes download
        ↓
filepupctl ingest "%F"
        ↓
FilePup records and safely processes the job
        ↓
Completed_Torrents → Staging
        ↓
HandBrake watches Staging and handles transcoding
        ↓
Transcoding / downstream media flow
        ↓
Shows / Movies / Books
        ↓
FilePup verifies success before any future source cleanup
```

`TV` may be used internally as a media classification, but the actual television library folder is **Shows**.

## Current status: v0.0.1 scaffold

Implemented:

- Python package structure
- `filepupctl` command entrypoint
- `filepupd` daemon entrypoint
- SQLite-backed persistent job records
- idempotent ingest behavior
- explicit job-state model
- central `SafetyController`
- deletion hard-disabled
- safe single-file and per-file directory staging
- explicit retry/resume for partially staged jobs
- basic tests
- systemd service template
- Linux media path configuration

Not implemented yet:

- media classification
- HandBrake monitoring/integration
- output verification
- source cleanup
- Jellyfin refresh
- GUI

## Retry/resume implementation note

`stage` is intentionally limited to `DISCOVERED` jobs. A failed or partially
processed job enters `NEEDS_ATTENTION` and can be retried explicitly with:

```bash
filepupctl retry <job_id>
```

Retry re-checks the source, Completed Torrents boundary, Staging availability,
destination safety, and post-move verification. For directory jobs it
re-inventories without erasing historical `STAGED` or `NEEDS_ATTENTION` child
rows, skips `STAGED` and `IGNORED` children, and retries only unresolved
supported children. A remaining conflict or duplicate keeps the parent in
`NEEDS_ATTENTION`; once every supported child is `STAGED`, the parent advances
to `STAGED`. Unsupported files remain preserved in the source directory and do
not block the supported-media result. Source deletion remains hard-disabled.

## Development

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install pytest
pytest
```

Example commands after installation:

```bash
filepupctl ingest "/Media/Completed_Torrents/example"
filepupctl jobs
filepupd
```

Do not replace the current qBittorrent completion hook with FilePup yet. The current scaffold records jobs only and intentionally performs no media moves or cleanup.
