#!/usr/bin/env python3
"""
MCQuaC Runner
-------------

Überwacht die Hash-Ordner unter ./tmp und führt für jedes vorkommende
".ready" einen Nextflow-Run aus. Ablauf pro Job:
  1) .ready -> .working (Uhrzeit anhängen)
  2) nextflow run -profile docker <main.nf> -params-file <mcquac.json>
     - <main.nf> wird immer aus config/app.json (mcquac_path) gelesen
  3) Bei Prozessende wird die .working-Datei erweitert (Endzeit + Returncode)
     und in .finish umbenannt.

Integration in main.py:
  from src.mcquac_runner import start_runner_thread
  runner = start_runner_thread(TMP_DIR, cfg, max_parallel=1, poll_interval=1.0)
  ... im graceful_stop(...): runner["stop"].set(); runner["thread"].join(timeout=5)

Dieses Modul kann auch alleine gestartet werden (siehe __main__).
"""
from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Iterable, Any, Dict, Tuple
import threading
import subprocess
import json
from datetime import datetime
import queue
import os
import shutil

# Projektwurzel: eine Ebene über /src
PROJECT_ROOT = Path(__file__).resolve().parents[1]
TMP_DEFAULT = PROJECT_ROOT / "tmp"

# Aus bestehendem Projekt
try:
    from src.load_config import load_config, AppConfig  # type: ignore
except Exception:  # Fallback, damit das Modul allein lauffähig bleibt
    AppConfig = object  # type: ignore
    def load_config(*args, **kwargs):  # type: ignore
        raise RuntimeError("load_config() nicht verfügbar – bitte innerhalb des Projekts ausführen.")


@dataclass
class _RunningJob:
    hash_dir: Path
    working_file: Path
    log_file: Path
    proc: subprocess.Popen
    started_at: datetime


# ----------------------------- Hilfsfunktionen -----------------------------

def _iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _append_line(p: Path, line: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def _rename_atomic(src: Path, dst: Path) -> None:
    """Robustes Umbenennen (auch wenn Ziel schon existiert)."""
    if dst.exists():
        dst.unlink()
    src.replace(dst)


def _read_json(p: Path) -> Optional[Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _find_mcquac_json(hash_dir: Path) -> Optional[Path]:
    # 1) Bevorzugt direkte Datei im Hash-Ordner
    p = hash_dir / "mcquac.json"
    if p.is_file():
        return p
    # 2) Optional über info.json auflösen
    info = hash_dir / "info.json"
    data = _read_json(info)
    try:
        cand = Path(data["paths"]["mcquac_json"])  # type: ignore[index]
        if cand.is_file():
            return cand
    except Exception:
        pass
    return None


def _resolve_main_nf(cfg_mcquac_path: Optional[Path]) -> Optional[Path]:
    """Liest den Pfad zu main.nf **ausschließlich** aus config/app.json (mcquac_path)."""
    if cfg_mcquac_path:
        p = Path(os.path.expandvars(os.path.expanduser(str(cfg_mcquac_path)))).resolve()
        if p.is_file():
            return p
    return None


def _discover_ready_dirs(tmp_dir: Path) -> Iterable[Tuple[Path, Path]]:
    """Liefert (hash_dir, ready_file) für alle Ordner mit .ready."""
    if not tmp_dir.is_dir():
        return []
    for child in tmp_dir.iterdir():
        if not child.is_dir():
            continue
        ready = child / ".ready"
        if ready.is_file():
            yield child, ready


def _resolve_nextflow_bin(cfg: AppConfig) -> str:
    """Suche Nextflow-Binary: $NEXTFLOW_BIN -> cfg.nextflow_bin -> ./nextflow -> PATH."""
    cand = os.environ.get("NEXTFLOW_BIN")
    if cand:
        p = Path(os.path.expandvars(os.path.expanduser(str(cand)))).resolve()
        if p.is_file():
            return str(p)
    cfg_bin = getattr(cfg, "nextflow_bin", None)  # optional in app.json
    if cfg_bin:
        p = Path(os.path.expandvars(os.path.expanduser(str(cfg_bin)))).resolve()
        if p.is_file():
            return str(p)
    local = PROJECT_ROOT / "nextflow"
    if local.is_file():
        return str(local)
    found = shutil.which("nextflow")
    return found or "nextflow"


# ----------------------------- Runner-Thread -----------------------------

def _runner_loop(
    tmp_dir: Path,
    cfg: AppConfig,
    max_parallel: int,
    poll_interval: float,
    stop_evt: threading.Event,
    status_q: "queue.Queue[str]",
) -> None:
    """Runner-Schleife: verarbeitet .ready nacheinander (oder bis max_parallel) und idlet sonst.

    - .ready -> .working (Zeitstempel + ausgeführter Befehl)
    - nextflow run -profile docker <main.nf> -params-file <mcquac.json>
      <main.nf> stammt **immer** aus cfg.mcquac_path (config/app.json)
    - nach Ende -> .finish + returncode
    """
    running: Dict[Path, _RunningJob] = {}

    while not stop_evt.is_set():
        # 1) Beendete Prozesse einsammeln
        for hdir, job in list(running.items()):
            rc = job.proc.poll()
            if rc is None:
                continue
            try:
                _append_line(job.working_file, f"finished: {_iso_now()}")
                _append_line(job.working_file, f"returncode: {rc}")
                _rename_atomic(job.working_file, hdir / ".finish")
                status_q.put(f"[OK] {hdir.name} -> .finish (rc={rc})")
            except Exception as e:
                status_q.put(f"[WARN] Abschluss für {hdir.name} fehlgeschlagen: {e}")
            finally:
                running.pop(hdir, None)

        # 2) Neue Jobs starten, wenn Kapazität frei ist
        capacity = max(0, int(max_parallel) - len(running))
        if capacity > 0:
            # FIFO: älteste .ready zuerst (mtime)
            ready_list = list(_discover_ready_dirs(tmp_dir))
            ready_list.sort(key=lambda t: (t[1].stat().st_mtime, t[0].name))

            for hdir, ready in ready_list:
                if capacity <= 0:
                    break
                if hdir in running:
                    continue

                working = hdir / ".working"
                try:
                    _rename_atomic(ready, working)
                    _append_line(working, f"started: {_iso_now()}")
                except Exception as e:
                    status_q.put(f"[WARN] Konnte .ready für {hdir.name} nicht übernehmen: {e}")
                    continue

                # mcquac.json & main.nf (aus app.json) prüfen
                mcq_json = _find_mcquac_json(hdir)
                main_nf = _resolve_main_nf(getattr(cfg, "mcquac_path", None))
                if not mcq_json or not mcq_json.is_file() or not main_nf:
                    _append_line(working, "error: mcquac.json oder main.nf (aus app.json) nicht gefunden")
                    try:
                        _rename_atomic(working, hdir / ".finish")
                    except Exception:
                        pass
                    status_q.put(f"[ERR] {hdir.name}: mcquac.json oder main.nf (app.json) fehlt")
                    continue

                # Logs
                logs_dir = hdir / "logs"
                logs_dir.mkdir(parents=True, exist_ok=True)
                log_file = logs_dir / f"nextflow-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

                # Nextflow-Binary & Befehl
                nf_bin = _resolve_nextflow_bin(cfg)
                cmd = [
                    nf_bin,
                    "run",
                    "-profile",
                    "docker",
                    str(main_nf),
                    "-params-file",
                    str(mcq_json),
                ]
                _append_line(working, "cmd: " + " ".join(cmd))

                try:
                    lf = log_file.open("ab", buffering=0)
                    proc = subprocess.Popen(
                        cmd,
                        stdout=lf,
                        stderr=subprocess.STDOUT,
                        cwd=hdir,
                        env=os.environ.copy(),
                    )
                    running[hdir] = _RunningJob(
                        hash_dir=hdir,
                        working_file=working,
                        log_file=log_file,
                        proc=proc,
                        started_at=datetime.now(),
                    )
                    status_q.put(f"[RUN] {hdir.name}: PID {proc.pid}")
                    capacity -= 1
                except FileNotFoundError:
                    _append_line(
                        working,
                        f"error: nextflow nicht gefunden (bin={nf_bin}). Installiere Nextflow oder setze $NEXTFLOW_BIN; z.B.: curl -s https://get.nextflow.io | bash",
                    )
                    try:
                        _rename_atomic(working, hdir / ".finish")
                    except Exception:
                        pass
                    status_q.put(f"[ERR] {hdir.name}: nextflow nicht gefunden")
                except Exception as e:
                    _append_line(working, f"error: {e}")
                    try:
                        _rename_atomic(working, hdir / ".finish")
                    except Exception:
                        pass
                    status_q.put(f"[ERR] {hdir.name}: Start fehlgeschlagen: {e}")

        # 3) Idle kurz schlafen
        stop_evt.wait(poll_interval)


# ----------------------------- Public API -----------------------------

def start_runner_thread(
    tmp_dir: Path,
    cfg: AppConfig,
    *,
    max_parallel: int = 1,
    poll_interval: float = 1.0,
) -> Dict[str, Any]:
    """
    Startet den Runner als Hintergrund-Thread.
    Rückgabe: {"thread": Thread, "stop": Event, "status": Queue[str]}
    """
    stop_evt = threading.Event()
    status_q: "queue.Queue[str]" = queue.Queue()
    t = threading.Thread(
        target=_runner_loop,
        args=(tmp_dir, cfg, max(1, int(max_parallel)), float(poll_interval), stop_evt, status_q),
        name="mcquac-runner",
        daemon=True,
    )
    t.start()
    return {"thread": t, "stop": stop_evt, "status": status_q}


# ----------------------------- Standalone -----------------------------
if __name__ == "__main__":
    try:
        cfg = load_config()
    except Exception as e:
        raise SystemExit(f"Config konnte nicht geladen werden: {e}")

    tmp_dir = TMP_DEFAULT
    tmp_dir.mkdir(parents=True, exist_ok=True)
    ctl = start_runner_thread(tmp_dir, cfg, max_parallel=1, poll_interval=1.0)

    print("MCQuaC Runner gestartet. Strg+C zum Beenden.")
    try:
        while True:
            try:
                msg = ctl["status"].get(timeout=0.5)
                print(msg)
            except queue.Empty:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        ctl["stop"].set()
        ctl["thread"].join(timeout=5)
        print("Beendet.")
