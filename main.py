#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import sys, re, signal, time, queue
from typing import Callable, Any

# --- Projektroot & Imports absichern ---
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clear import nuke_tmp
from src.load_config import load_config
from src.search import start_watch_thread            # unterstützt extra_ignore_file + pre_scan_hook
from src.copier import copy_candidates               # erzeugt mcquac.json + info.json + .ready
from src.mounter import (
    ensure_mounts_from_cfg,
    unmount_all_from_cfg,
    ensure_smb_mount,                                # <- für Guard pro Watcher
)
from src.mcquac_runner import start_runner_thread    # führt .ready-Jobs mit Nextflow aus

TMP_DIR = ROOT / "tmp"

def slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s).strip()) or "x"

def graceful_stop(watchers: list[dict]) -> None:
    print("\nBeende Überwachung …")
    for w in watchers:
        try:
            w["stop"].set()
            w["thread"].join(timeout=2)
        except Exception:
            pass
    print("Fertig.")

def _get(obj: Any, name: str, default=None):
    # cfg kann Dict oder Objekt sein
    try:
        return getattr(obj, name)
    except Exception:
        try:
            return obj.get(name, default)  # type: ignore
        except Exception:
            return default

def _drain_status(status_q: "queue.Queue[str]") -> None:
    if not status_q:
        return
    while True:
        try:
            msg = status_q.get_nowait()
        except queue.Empty:
            break
        else:
            print(msg)

def _is_subpath(child: Path, parent: Path) -> bool:
    try:
        child = Path(child).resolve()
        parent = Path(parent).resolve()
        _ = child.relative_to(parent)
        return True
    except Exception:
        return False

def _make_mount_guard(folder_in: Path, cfg: Any) -> Callable[[], None]:
    """
    Liefert eine Funktion, die vor JEDEM Scan ausgeführt wird und ggf. den
    relevanten SMB-Mount repariert. Idempotent dank ensure_smb_mount().
    """
    mounts = _get(cfg, "mounts", []) or []

    relevant = []
    for entry in mounts:
        mp = _get(entry, "mountpoint")
        if not mp:
            continue
        try:
            if _is_subpath(Path(folder_in), Path(mp)):
                relevant.append(entry)
        except Exception:
            continue

    def guard() -> None:
        for e in relevant:
            try:
                # kehrt sofort zurück, wenn Mount aktiv + listbar ist
                ensure_smb_mount(e, non_interactive=True)
            except Exception as ex:
                name = _get(e, "name", f"{_get(e,'share','?')}@{_get(e,'host','?')}")
                print(f"[WARN] Mount-Prüfung/Reparatur '{name}' fehlgeschlagen: {ex}")
    return guard

def main() -> int:
    # tmp leeren (falls unerwünscht: auskommentieren)
    cleaned = nuke_tmp(ROOT)
    print(f"Bereinigt: {cleaned}\n")

    # Config laden (expects interval_seconds in config/app.json)
    cfg = load_config()
    print("Starte Überwachung aus config/app.json …")
    print(f"  Interval: { _get(cfg,'interval_seconds') } s")
    print(f"  default_pattern: { _get(cfg,'default_pattern') }")
    print(f"  mcquac_path:     { _get(cfg,'mcquac_path') }\n")

    # --- Systemweite Mounts initial bereitstellen (optional) ---------------- #
    mounts = _get(cfg, "mounts", None) or []
    continue_on_mount_error = bool(_get(cfg, "continue_on_mount_error", False))
    if mounts:
        try:
            statuses = ensure_mounts_from_cfg(
                cfg,
                best_effort=continue_on_mount_error,
                non_interactive=True
            )
            print("Mount-Status:")
            for name, st in statuses.items():
                print(f"  - {name}: {st}")
            print()
        except PermissionError as e:
            msg = f"[FATAL] {e}\nTipp: Als root ausführen (sudo) und 'cifs-utils' installieren."
            if continue_on_mount_error:
                print("[WARN] Mount-Fehler, fahre trotzdem fort:", msg)
            else:
                print(msg)
                return 2
        except Exception as e:
            if continue_on_mount_error:
                print(f"[WARN] Mount fehlgeschlagen, fahre fort: {e}")
            else:
                print(f"[FATAL] Mount fehlgeschlagen: {e}")
                return 2
    else:
        print("Keine Mounts in config/app.json definiert – überspringe Mount-Schritt.\n")
    # ----------------------------------------------------------------------- #

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Für jedes IO-Pair einen Watcher-Thread starten
    watchers: list[dict] = []
    io_pairs = _get(cfg, "io_pairs", []) or []
    if not io_pairs:
        print("[WARN] Keine io_pairs in der Config. Es werden keine Watcher gestartet.\n")

    for i, pair in enumerate(io_pairs, start=1):
        # pair kann Dict oder Objekt sein
        folder_in  = _get(pair, "input")
        final_out  = _get(pair, "output")
        pattern    = _get(pair, "pattern") or _get(cfg, "default_pattern")
        use_full   = True
        recursive  = False
        interval_s = _get(cfg, "interval_seconds")

        # thread-spezifische Ignore-Datei im tmp/
        ign_thread = TMP_DIR / f"ignore-{slug(folder_in)}-{slug(pattern)}.txt"
        ign_thread.touch(exist_ok=True)

        # zweite Ignore-Datei: ignore.txt im Output-Ordner des Pairs
        Path(final_out).mkdir(parents=True, exist_ok=True)      # Ordner sicherstellen
        ign_output = Path(final_out) / "ignore.txt"
        ign_output.touch(exist_ok=True)

        # Mount-Guard für diesen Watcher (No-Op, wenn input nicht unter einem Mountpoint liegt)
        pre_hook = _make_mount_guard(Path(folder_in), cfg)

        print(f"[Watcher {i}]")
        print(f"  IN : {folder_in}")
        print(f"  OUT: {final_out}")
        print(f"  PAT: {pattern}")
        print(f"  IGN: {ign_thread.name} + {ign_output}")       # Pfad der output-ignore.txt
        print()

        t, q, stop_evt = start_watch_thread(
            folder=folder_in,
            pattern=pattern,
            recursive=recursive,
            interval_seconds=interval_s,   # **Sekunden**
            use_full_path=use_full,
            ignore_file=ign_thread,        # thread-spezifische Ignore (im tmp/)
            extra_ignore_file=ign_output,  # Ignore-Datei im Output-Ordner des Pairs
            pre_scan_hook=pre_hook,        # <- NEU: vor jedem Scan Mount checken/reparieren
        )
        # Falls start_watch_thread den Thread nicht selbst startet:
        try:
            if hasattr(t, "is_alive") and not t.is_alive():
                t.start()
        except RuntimeError:
            # war evtl. schon gestartet
            pass

        watchers.append({
            "idx": i,
            "thread": t,
            "queue": q,
            "stop": stop_evt,
            "in_root": Path(folder_in),
            "final_out": Path(final_out),
            "pattern": pattern,
            "interval_s": interval_s,
            "ign_thread": ign_thread,
            "ign_output": ign_output,
        })

    # MCQuaC-Runner starten (überwacht tmp/ auf .ready und führt Nextflow aus)
    runner = start_runner_thread(TMP_DIR, cfg, max_parallel=1, poll_interval=1.0)

    # globales Cache, um Doppelkopien über alle Threads hinweg zu vermeiden
    copied_cache: set[tuple[str,int]] = set()

    # Sauberes Stoppen per SIGINT/SIGTERM
    stop_now = {"flag": False}
    def _sig_handler(signum, frame):
        stop_now["flag"] = True
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    try:
        # Haupt-Loop: robustes, dauerhaftes Event-Processing
        while not stop_now["flag"]:
            if not watchers:
                # Keine Watcher konfiguriert -> dennoch Runner-Status anzeigen
                _drain_status(runner["status"])  # Statusmeldungen des Runners ausgeben
                time.sleep(0.5)
                continue

            any_processed = False

            # Round-robin: alle Queues kurz abfragen
            for w in watchers:
                q = w["queue"]
                try:
                    # Kurzes Warten pro Watcher, damit CPU-Last gering bleibt
                    kind, ts, snapshot = q.get(timeout=0.5)
                except Exception:
                    continue

                any_processed = True
                print("-" * 60)
                print(f"[{ts}] [Watcher {w['idx']}] Kandidaten (min. 2×): {len(snapshot)}")
                if not snapshot:
                    print("  (keine Treffer)")
                    continue

                for item in snapshot:
                    print(f"  {item['count']}×  size={item['size']}  hash={item['hash']}  name={item['name']}")

                # Metadaten, die in info.json pro Hash abgelegt werden
                info_for_hash = {
                    "input_root": w["in_root"],
                    "final_output_root": w["final_out"],  # für späteres Kopieren vorbereitet
                    "pattern": w["pattern"],
                    "interval_seconds": w["interval_s"],
                    "ignore_files": [w["ign_thread"], w["ign_output"]],
                }

                new_paths, added_ign = copy_candidates(
                    snapshot,
                    folder=w["in_root"],
                    tmp_dir=TMP_DIR,
                    copied_cache=copied_cache,
                    ignore_file=w["ign_thread"],   # nur Dateinamen werden angehängt (in die tmp-ignore)
                    add_to_ignore=True,
                    info_for_hash=info_for_hash,   # -> src/copier.py legt info.json an & .ready-Datei
                )
                for p in new_paths:
                    print(f"    -> COPIED to {p}")
                if added_ign:
                    print(f"    -> {added_ign} Name(n) zu {w['ign_thread'].name} hinzugefügt")

            # Runner-Status ausgeben
            _drain_status(runner["status"])

            # Wenn gerade keine Queue-Items kamen, ganz kurz idlen -> CPU-schonend, "dauerhaft"
            if not any_processed:
                time.sleep(0.1)

    finally:
        # Runner sauber stoppen
        try:
            runner["stop"].set()
            runner["thread"].join(timeout=5)
        except Exception:
            pass

        graceful_stop(watchers)
        # Optionales Unmount (nur wenn in der Config aktiviert)
        try:
            if bool(_get(cfg, "unmount_on_exit", False)):
                unmount_all_from_cfg(cfg)
        except Exception as e:
            print(f"[WARN] Unmount beim Beenden: {e}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
