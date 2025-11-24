#!/usr/bin/env python3
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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TMP_DEFAULT = PROJECT_ROOT / "tmp"

try:
    from src.load_config import load_config, AppConfig  # type: ignore
except Exception:
    AppConfig = object  # type: ignore

    def load_config(*args, **kwargs):  # type: ignore
        raise RuntimeError(
            "load_config() nicht verfügbar – bitte innerhalb des Projekts ausführen."
        )


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
    if dst.exists():
        dst.unlink()
    src.replace(dst)


def _read_json(p: Path) -> Optional[Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _find_mcquac_json(hash_dir: Path) -> Optional[Path]:
    p = hash_dir / "mcquac.json"
    if p.is_file():
        return p
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
    if cfg_mcquac_path:
        p = Path(os.path.expandvars(os.path.expanduser(str(cfg_mcquac_path)))).resolve()
        if p.is_file():
            return p
    return None


def _discover_ready_dirs(tmp_dir: Path) -> Iterable[Tuple[Path, Path]]:
    if not tmp_dir.is_dir():
        return []
    for child in tmp_dir.iterdir():
        if not child.is_dir():
            continue
        ready = child / ".ready"
        if ready.is_file():
            yield child, ready


def _resolve_nextflow_bin(cfg: AppConfig) -> str:
    cand = os.environ.get("NEXTFLOW_BIN")
    if cand:
        p = Path(os.path.expandvars(os.path.expanduser(str(cand)))).resolve()
        if p.is_file():
            return str(p)
    cfg_bin = getattr(cfg, "nextflow_bin", None)
    if cfg_bin:
        p = Path(os.path.expandvars(os.path.expanduser(str(cfg_bin)))).resolve()
        if p.is_file():
            return str(p)
    local = PROJECT_ROOT / "nextflow"
    if local.is_file():
        return str(local)
    found = shutil.which("nextflow")
    return found or "nextflow"


# ------ Post-Processing: Output / Logs kopieren, ignore.txt aktualisieren, tmp leeren


def _unique_subdir(root: Path, base_name: str) -> Path:
    """Erzeuge eindeutigen Zielordner root/base_name, bei Kollision mit Zeitstempel."""
    root.mkdir(parents=True, exist_ok=True)
    cand = root / base_name
    if not cand.exists():
        return cand
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    cand2 = root / f"{base_name}-{ts}"
    if not cand2.exists():
        return cand2
    i = 2
    while True:
        c = root / f"{base_name}-{i}"
        if not c.exists():
            return c
        i += 1


def _copy_output_tree(src_dir: Path, dst_dir: Path) -> None:
    """Kopiere den *Inhalt* von src_dir nach dst_dir (nicht den Ordner selbst)."""
    if not src_dir.is_dir():
        return
    dst_dir.mkdir(parents=True, exist_ok=True)
    for child in src_dir.iterdir():
        target = dst_dir / child.name
        if child.is_dir():
            shutil.copytree(child, target, dirs_exist_ok=True)
        else:
            shutil.copy2(child, target)


def _append_to_ignore_file(ignore_file: Path, filename: str) -> None:
    ignore_file.parent.mkdir(parents=True, exist_ok=True)
    line = (filename or "").strip()
    if not line:
        return
    # Dedupe: Datei kurz einlesen, ansonsten hinten anhängen
    try:
        if ignore_file.exists():
            existing = {
                ln.strip()
                for ln in ignore_file.read_text(
                    encoding="utf-8", errors="ignore"
                ).splitlines()
            }
            if line in existing:
                return
    except Exception:
        pass
    with ignore_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _empty_dir(d: Path) -> None:
    """Inhalt eines Ordners löschen, Ordner bestehen lassen."""
    if not d.exists():
        return
    for child in d.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)  # py>=3.8
        except Exception:
            pass


def _postprocess_success(hash_dir: Path, status_q: "queue.Queue[str]") -> None:
    """
    Wird bei rc==0 aufgerufen:

    - Sucht im tmp_output_dir nach *.hdf5 (rekursiv).
    - Wählt die "beste" Datei (zuletzt geändert, bei Gleichstand größere).
    - Kopiert sie als <SRC_STEM>.hdf5 direkt nach final_output_root.
      Beispiel: EXII12567std.hdf5 liegt direkt im Output-Ordner.
    - Aktualisiert ignore.txt.
    - Leert tmp/<hash>/{input,output,work}.
    """
    info = _read_json(hash_dir / "info.json")
    if not isinstance(info, dict):
        status_q.put(
            f"[WARN] {hash_dir.name}: info.json fehlt/korrupt – überspringe Post-Processing"
        )
        return

    # Pfade/Infos aus info.json
    try:
        paths = info.get("paths") or {}
        watch = info.get("watch") or {}
        tmp_output_dir = Path(
            paths.get("tmp_output_dir") or (hash_dir / "output")
        )
        final_root = Path(watch.get("final_output_root"))
        src_name = str((info.get("source") or {}).get("name") or "")
        src_stem = Path(src_name).stem if src_name else hash_dir.name
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: info.json unvollständig ({e}) – überspringe Post-Processing"
        )
        return

    # Nur *.hdf5-Datei in den Zielordner kopieren, und zwar als <SRC_STEM>.hdf5
    try:
        final_root.mkdir(parents=True, exist_ok=True)

        hdf5_files: list[Path] = []
        if tmp_output_dir.is_dir():
            hdf5_files = [
                p for p in tmp_output_dir.rglob("*.hdf5") if p.is_file()
            ]

        if not hdf5_files:
            status_q.put(
                f"[WARN] {hash_dir.name}: Keine .hdf5-Datei in {tmp_output_dir} gefunden – "
                "überspringe Output-Kopie"
            )
            return

        def _hdf5_key(p: Path) -> tuple[float, int]:
            try:
                st = p.stat()
                return (float(st.st_mtime), int(st.st_size))
            except Exception:
                return (0.0, 0)

        best_file = max(hdf5_files, key=_hdf5_key)
        target_file = final_root / f"{src_stem}.hdf5"
        shutil.copy2(best_file, target_file)
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: Output-Kopie (.hdf5) nach '{final_root}' fehlgeschlagen: {e}"
        )
        return

    # ignore.txt befüllen (wenn in info.watch.ignore_files vorhanden, das nehmen; sonst <final_root>/ignore.txt)
    try:
        ignore_candidates: list[Path] = []
        w = info.get("watch") or {}
        ig = w.get("ignore_files", [])
        if isinstance(ig, list):
            ignore_candidates = [
                Path(p)
                for p in ig
                if isinstance(p, str) and p.endswith("ignore.txt")
            ]
        ignore_file = ignore_candidates[-1] if ignore_candidates else (
            final_root / "ignore.txt"
        )
        _append_to_ignore_file(ignore_file, src_name)
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: ignore.txt-Update fehlgeschlagen: {e}"
        )

    # tmp/<hash>/{input,output,work} leeren
    try:
        _empty_dir(hash_dir / "input")
        _empty_dir(hash_dir / "output")
        _empty_dir(hash_dir / "work")
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: Leeren von input/output/work fehlgeschlagen: {e}"
        )

    status_q.put(
        f"[OUT] {hash_dir.name}: {best_file.name} → {target_file} ; "
        f"ignore.txt aktualisiert ; tmp/input & tmp/output geleert"
    )


def _postprocess_failure(hash_dir: Path, status_q: "queue.Queue[str]") -> None:
    """
    Wird bei rc!=0 aufgerufen.

    - Wenn KEINE .hdf5 erzeugt wurde:
        .nextflow.log im Hash-Ordner wird als <SRC_STEM>.error.log in
        final_output_root kopiert, z. B. EXII12567std.error.log.
    - Wenn doch eine .hdf5 existiert, wird kein Fehler-Log gespiegelt.
    """
    info = _read_json(hash_dir / "info.json")
    if not isinstance(info, dict):
        status_q.put(
            f"[WARN] {hash_dir.name}: info.json fehlt/korrupt – Fehler-Post-Processing übersprungen"
        )
        return

    try:
        paths = info.get("paths") or {}
        watch = info.get("watch") or {}
        tmp_output_dir = Path(
            paths.get("tmp_output_dir") or (hash_dir / "output")
        )
        final_root = Path(watch.get("final_output_root"))
        src_name = str((info.get("source") or {}).get("name") or "")
        src_stem = Path(src_name).stem if src_name else hash_dir.name
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: info.json unvollständig ({e}) – Fehler-Post-Processing übersprungen"
        )
        return

    # Wenn doch eine .hdf5 existiert, kein Fehlerlog erzeugen
    has_hdf5 = False
    try:
        if tmp_output_dir.is_dir():
            for _ in tmp_output_dir.rglob("*.hdf5"):
                has_hdf5 = True
                break
    except Exception:
        has_hdf5 = False

    if has_hdf5:
        status_q.put(
            f"[WARN] {hash_dir.name}: rc!=0, aber .hdf5 gefunden – kein .error.log erzeugt"
        )
        return

    # .nextflow.log im Hash-Ordner verwenden
    log_src = hash_dir / ".nextflow.log"
    if not log_src.is_file():
        status_q.put(
            f"[WARN] {hash_dir.name}: .nextflow.log nicht gefunden – kein .error.log erzeugt"
        )
        return

    try:
        final_root.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: Fehler-Output-Ziel '{final_root}' nicht nutzbar: {e}"
        )
        return

    target_file = final_root / f"{src_stem}.error.log"
    try:
        shutil.copy2(log_src, target_file)
        status_q.put(f"[OUT] {hash_dir.name}: Fehler-Log → {target_file}")
    except Exception as e:
        status_q.put(
            f"[WARN] {hash_dir.name}: Kopieren der Fehler-Logdatei fehlgeschlagen: {e}"
        )


# ----------------------------- Runner-Loop -----------------------------


def _runner_loop(
    tmp_dir: Path,
    cfg: AppConfig,
    max_parallel: int,
    poll_interval: float,
    stop_evt: threading.Event,
    status_q: "queue.Queue[str]",
) -> None:
    running: Dict[Path, _RunningJob] = {}

    while not stop_evt.is_set():
        # Beendete Prozesse einsammeln
        for hdir, job in list(running.items()):
            rc = job.proc.poll()
            if rc is None:
                continue

            # Post-Processing für Erfolg/Fehler
            try:
                if rc == 0:
                    _postprocess_success(hdir, status_q)
                else:
                    _postprocess_failure(hdir, status_q)
            except Exception as e:
                status_q.put(
                    f"[WARN] {hdir.name}: Post-Processing Fehler: {e}"
                )

            # Abschluss markieren
            try:
                _append_line(job.working_file, f"finished: {_iso_now()}")
                _append_line(job.working_file, f"returncode: {rc}")
                _rename_atomic(job.working_file, hdir / ".finish")
                status_q.put(f"[OK] {hdir.name} -> .finish (rc={rc})")
            except Exception as e:
                status_q.put(
                    f"[WARN] Abschluss für {hdir.name} fehlgeschlagen: {e}"
                )
            finally:
                running.pop(hdir, None)

        # Neue Jobs starten
        capacity = max(0, int(max_parallel) - len(running))
        if capacity > 0:
            ready_list = list(_discover_ready_dirs(tmp_dir))
            # Älteste .ready zuerst
            ready_list.sort(
                key=lambda t: (t[1].stat().st_mtime, t[0].name)
            )

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
                    status_q.put(
                        f"[WARN] Konnte .ready für {hdir.name} nicht übernehmen: {e}"
                    )
                    continue

                mcq_json = _find_mcquac_json(hdir)
                main_nf = _resolve_main_nf(
                    getattr(cfg, "mcquac_path", None)
                )
                if not mcq_json or not mcq_json.is_file() or not main_nf:
                    _append_line(
                        working,
                        "error: mcquac.json oder main.nf (aus app.json) nicht gefunden",
                    )
                    try:
                        _rename_atomic(working, hdir / ".finish")
                    except Exception:
                        pass
                    status_q.put(
                        f"[ERR] {hdir.name}: mcquac.json oder main.nf (app.json) fehlt"
                    )
                    continue

                logs_dir = hdir / "logs"
                logs_dir.mkdir(parents=True, exist_ok=True)
                log_file = logs_dir / (
                    "nextflow-"
                    + datetime.now().strftime("%Y%m%d-%H%M%S")
                    + ".log"
                )

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
                        f"error: nextflow nicht gefunden (bin={nf_bin})",
                    )
                    try:
                        _rename_atomic(working, hdir / ".finish")
                    except Exception:
                        pass
                    status_q.put(
                        f"[ERR] {hdir.name}: nextflow nicht gefunden (bin={nf_bin})"
                    )
                except Exception as e:
                    _append_line(working, f"error: {e!r}")
                    try:
                        _rename_atomic(working, hdir / ".finish")
                    except Exception:
                        pass
                    status_q.put(
                        f"[ERR] {hdir.name}: Start fehlgeschlagen: {e!r}"
                    )

        # kleinem Sleep über stop_evt.wait, damit der Loop nicht busy ist
        stop_evt.wait(poll_interval)


def start_runner_thread(
    tmp_dir: Path,
    cfg: AppConfig,
    *,
    max_parallel: int = 1,
    poll_interval: float = 1.0,
) -> Dict[str, Any]:
    stop_evt = threading.Event()
    status_q: "queue.Queue[str]" = queue.Queue()
    t = threading.Thread(
        target=_runner_loop,
        args=(
            tmp_dir,
            cfg,
            max(1, int(max_parallel)),
            float(poll_interval),
            stop_evt,
            status_q,
        ),
        name="mcquac-runner",
        daemon=True,
    )
    t.start()
    return {
        "thread": t,
        "stop": stop_evt,
        "status": status_q,
    }


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
