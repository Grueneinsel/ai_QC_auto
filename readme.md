# MCQuaC Watcher — automated QC pipeline orchestration

This project watches one or more input folders for new mass‑spectrometry files (e.g., `*.raw`), copies stable candidates to a temporary working directory, auto‑generates the **McQuaC** parameter file, and then launches **Nextflow** (using the Docker profile). Upon success, results are transferred to your target folder and repeat processing is prevented.

> In short: **drop → process → deliver → don’t reprocess.**

---

## Table of contents
- [Features](#features)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Configuration](#configuration)
  - [`config/app.json`](#configappjson)
  - [SMB / network shares (`mounts`)](#smb--network-shares-mounts)
  - [Input/output pairs (`io_pairs`)](#inputoutput-pairs-io_pairs)
  - [FASTA & spike‑in](#fasta--spike-in)
- [How it works](#how-it-works)
- [Run it](#run-it)
- [Run as a systemd service](#run-as-a-systemd-service)
  - [Option A: User service (no SMB mounts)](#option-a-user-service-no-smb-mounts)
  - [Option B: System/root service (with SMB mounts)](#option-b-systemroot-service-with-smb-mounts)
- [Troubleshooting](#troubleshooting)
- [Project layout](#project-layout)
- [License](#license)

---

## Features
- Watcher for any number of input folders (supports glob patterns like `*std.raw`).
- Robust **candidate** check: only files that remain unchanged (same size in ≥2 consecutive scans) are picked up.
- Automatic job creation: writes `mcquac.json` from template, injects FASTA & spike‑in paths.
- Nextflow runner (Docker profile) with log and status files (`.ready`, `.working`, `.finish`).
- Post‑processing: copy results to the final output folder, update `ignore.txt`, and clean temporary folders.
- Optional SMB mounts with version fallback (tries SMB 3.1.1 → 3.0 → 2.1).

## Requirements
- Debian/Ubuntu‑like system (WSL works as well).
- **Docker Engine** + **Docker Compose plugin**.
- **Java ≥ 11** (required by Nextflow).
- **Python 3.10+** (uses modern type annotations like `X | Y`).
- For network shares: `cifs-utils` (for SMB).

> The included `setup.sh` helps install prerequisites, downloads Nextflow into the project, clones the McQuaC repository, and can pre‑pull container images.

## Quick start
1. **Prepare the repo** (if not done yet):
   ```bash
   chmod +x ./setup.sh
   ./setup.sh
   ```
   The script installs Docker/Java (if missing), downloads Nextflow locally, clones **McQuaC**, and creates/updates `config/app.json` with proper paths.

2. **Adjust configuration:**
   - Review `config/app.json` (see below). Especially `mcquac_path`, `nextflow_bin`, `io_pairs`, and optional `mounts`.
   - Place your FASTA under `config/fasta/` (top level, e.g., `human.fasta`).
   - Place your spike‑in table under `config/spike/` (top level, `*.csv`).

3. **Start the watcher:**
   ```bash
   python3 main.py
   # or make it executable: ./main.py
   ```

4. **Check results:**
   - Logs: `tmp/<hash>/logs/nextflow-YYYYMMDD-HHMMSS.log`
   - Final output: under the target defined in `io_pairs[*].output` (after post‑processing).

## Configuration

### `config/app.json`
Minimal example:
```json
{
  "interval_minutes": 6,
  "default_pattern": "*std.raw",
  "mcquac_path": "/path/to/McQuaC/main.nf",
  "nextflow_bin": "/path/to/nextflow",
  "mounts": [],
  "unmount_on_exit": true,
  "io_pairs": [
    { "input": "/data/in", "output": "/data/out", "pattern": "*std.raw" }
  ]
}
```
**Fields:**
- `interval_minutes` — watcher polling interval in minutes (internally converted to seconds).
- `default_pattern` — glob used when an `io_pair` doesn’t specify its own `pattern`.
- `mcquac_path` — path to the McQuaC `main.nf`.
- `nextflow_bin` *(optional)* — path to the Nextflow binary (otherwise resolution order: `$NEXTFLOW_BIN` → local `./nextflow` → `PATH`).
- `mounts` *(optional)* — SMB share definitions (see below).
- `continue_on_mount_error` *(optional, bool)* — continue even if a mount fails.
- `unmount_on_exit` *(optional, bool)* — unmount shares when shutting down.
- `io_pairs` — list of objects `{ input, output, pattern? }`.

### SMB / network shares (`mounts`)
Example with domain and extra options:
```json
{
  "mounts": [
    {
      "name": "archive",
      "host": "192.168.10.20",
      "share": "Archiv",
      "mountpoint": "/mnt/archive",
      "username": "user123",
      "password": "SECRET",
      "domain": "ACME",
      "vers": null,
      "file_mode": "0664",
      "dir_mode": "0775",
      "extra_opts": ["noserverino"]
    }
  ]
}
```
**Notes:**
- Mounting requires root privileges (run with `sudo`).
- The mounter checks ping/port 445 and tries SMB versions **3.1.1 → 3.0 → 2.1**, creating a temporary credentials file.
- **Security:** passwords live in clear text in `app.json`. Lock down access to `config/` accordingly.

### Input/output pairs (`io_pairs`)
Each pair defines a watched folder (`input`) and a final target (`output`).
- The target folder maintains an `ignore.txt` file. After a successful run, the **base filename** of each processed source file is added there to prevent re‑processing. Remove an entry to force a re‑run.
- Each watcher thread also keeps a thread‑local ignore file under `tmp/` to avoid duplicates while the process is running.

### FASTA & spike‑in
Put files at the top level of these folders:
```
config/
├── fasta/   # *.fasta (the "newest" file is used automatically)
└── spike/   # *.csv   (same rule)
```
The `mcquac.json` template supports placeholders `%%%FASTA%%%%` and `%%%SPIKE%%%%` which are automatically replaced with the detected paths during job creation.

## How it works
1. **Watch** — For each `io_pair`, a thread scans its input folder on an interval (`interval_minutes`). A file becomes a **candidate** only if it appears with the same size in at least **two consecutive scans** and is not matched by either ignore file (thread‑local `tmp/...` and `output/ignore.txt`).
2. **Copy & job** — Each candidate is copied to `tmp/<hash>/input/`. For each hash, the system creates:
   - `mcquac.json` (from `config/mcquac.json` template; injects FASTA and spike‑in and replaces placeholders)
   - `info.json` (metadata: paths, source, watch context)
   - `.ready` (signal for the runner)
3. **Run** — The Nextflow runner consumes jobs from `tmp/*/.ready`, resolves Nextflow (order: `$NEXTFLOW_BIN` → `app.json:nextflow_bin` → local `./nextflow` → `PATH`), and starts:
   ```bash
   nextflow run -profile docker <mcquac main.nf> -params-file mcquac.json
   ```
   Logs are written to `tmp/<hash>/logs/`.
4. **Post‑processing** *(on success)*:
   - Copy the contents of `tmp/<hash>/output` into a **unique subfolder** under `io_pairs[*].output` (base name = source filename; collisions resolved by timestamp/counter).
   - Update `ignore.txt` (uses the last known ignore file from the watch context; otherwise `<output>/ignore.txt`).
   - Empty `tmp/<hash>/{input,output,work}`. The runner renames `.working` → `.finish` and records the return code.

## Run it
- **Direct:**
  ```bash
  python3 main.py
  ```
- **With a virtual env (optional):**
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt  # if present
  python3 main.py
  ```
- **Test Nextflow:**
  ```bash
  ./nextflow run hello
  ./nextflow run hello -with-docker
  ```

## Run as a systemd service

You can run MCQuaC Watcher as a long-running background service using **systemd**. This is useful on machines where the watcher should keep running across logouts and reboots.

> **Important**  
> - If you **do not use SMB mounts** (`"mounts": []` in `config/app.json`), you can run it as a **user service**.  
> - If you **use SMB mounts**, MCQuaC Watcher needs to call `mount.cifs` and therefore must run as **root** as a **system service**.

In the examples below, replace `/path/to/mcquac-watcher` with the actual project directory and adapt the Python/venv path as needed.

### Option A: User service (no SMB mounts)

Use this if `mounts` in `config/app.json` is empty and you do not mount any network shares from the watcher itself.

1. Create a user unit:

   ```bash
   mkdir -p ~/.config/systemd/user
   nano ~/.config/systemd/user/mcquac-watcher.service

2. Paste the following unit file:

```
[Unit]
Description=MCQuaC Watcher (user service)
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/mcquac-watcher
ExecStart=/path/to/mcquac-watcher/.venv/bin/python -u main.py
Restart=always
RestartSec=10

# Optional: send all output to the journal
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target

```

3. Reload and start:

```
systemctl --user daemon-reload
systemctl --user start mcquac-watcher.service
systemctl --user enable mcquac-watcher.service
```

4. View logs:
```
journalctl --user-unit mcquac-watcher.service -f
```

On WSL you must have systemd enabled and the distro restarted so that `systemctl --user` works.

### Option B: System/root service (with SMB mounts)

Use this if you configured any `mounts` in `config/app.json`. Mounting CIFS shares requires root privileges; otherwise MCQuaC Watcher will fail when trying to mount.

1. Create a system unit as root:

```
sudo nano /etc/systemd/system/mcquac-watcher.service
```

2. Paste the following unit file:


```
[Unit]
Description=MCQuaC Watcher (root + SMB mounts)
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/mcquac-watcher
ExecStart=/path/to/mcquac-watcher/.venv/bin/python -u main.py
Restart=always
RestartSec=10

# Example if you want to force a specific Nextflow binary:
# Environment="NEXTFLOW_BIN=/path/to/mcquac-watcher/nextflow"

[Install]
WantedBy=multi-user.target
```

3. Reload and start:

```
sudo systemctl daemon-reload
sudo systemctl start mcquac-watcher.service
sudo systemctl enable mcquac-watcher.service
```

4. View logs:

```
sudo journalctl -u mcquac-watcher.service -f
```

If you previously ran MCQuaC Watcher manually, make sure the `tmp/` directory is writable for the user that systemd uses (for a root service this is usually no problem; for a user service just keep running it as the same Linux user as before).


## Troubleshooting
- **Docker daemon not reachable** — ensure the service is running and your user is in the `docker` group. Log out/in or run `newgrp docker`.
- **`nextflow` not found** — set `nextflow_bin` in `app.json` or export `$NEXTFLOW_BIN`.
- **`mcquac.json` or `main.nf` missing** — verify `mcquac_path` and that the McQuaC repo/branch is cloned correctly.
- **SMB mount errors** — run with `sudo`, install `cifs-utils`, check network/port 445; optionally set `continue_on_mount_error: true`.
- **No watchers active** — fill `io_pairs` in `app.json`.
- **Stuck temp state** — run the cleanup helper to reset the working area:
  ```bash
  python3 -m src.clear
  # or use the function `nuke_tmp()` in your own script
  ```

## Project layout
```
project/
├── main.py
├── src/
│   ├── load_config.py      # read & validate app.json (incl. mounts, nextflow_bin)
│   ├── search.py           # watcher thread (stable candidates via repeated scans)
│   ├── size.py             # file listing & size helper with glob + ignore patterns
│   ├── copier.py           # copy candidates → tmp/<hash>; write mcquac.json/info.json/.ready
│   ├── job_creater.py      # replace %%%INPUT%%% / %%%OUTPUT%%% into mcquac.json
│   ├── mcquac_runner.py    # consume .ready, run Nextflow, post‑process, write .finish
│   ├── mounter.py          # optional SMB mounting from config
│   └── clear.py            # `nuke_tmp()` to clean ./tmp
├── config/
│   ├── app.json            # main configuration
│   ├── mcquac.json         # job template (placeholders supported)
│   ├── fasta/              # *.fasta (top level)
│   └── spike/              # *.csv   (top level)
├── tmp/                    # working directory (hash folders)
└── setup.sh                # optional bootstrap script
```

## License
TBD

---

### Acknowledgments
- **McQuaC** (mpc‑bioinformatics) for the underlying QC pipeline.

