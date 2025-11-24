#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from collections import defaultdict
from queue import Queue
from datetime import datetime
from typing import Iterable, Union, Optional, Dict, List, Callable, Tuple
import threading
import time
import fnmatch
import hashlib
import re

from .size import file_sizes_folder  # nutzt deine bestehende Funktion (unterstützt .d-Verzeichnisse)


def _read_ignore_list(path: Path | None) -> List[str]:
    """
    Liest eine Ignore-Datei (eine Zeile pro Muster) und gibt eine Liste von
    glob/fnmatch-Mustern zurück. Zeilen, die mit '#' beginnen, werden ignoriert.
    """
    if not path or not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]


def _normalize_patterns(pattern: Union[str, Iterable[str]]) -> List[str]:
    """
    Normalisiert das pattern-Argument zu einer Liste von Glob-Mustern.

    Unterstützt:
      - einfache Strings: "*std.raw"
      - mehrere Muster in einem String, getrennt durch ',', ';' oder '|':
          "*std.raw,*std.d" oder "*std.raw|*std.d"
      - bereits übergebene Iterables: ["*std.raw", "*std.d"]

    Zusätzlich:
      - Wenn ein Muster auf ".raw" endet, wird automatisch die passende
        ".d"-Variante ergänzt (z.B. "*std.raw" -> ["*std.raw", "*std.d"]),
        falls noch nicht vorhanden.
    """
    patterns: List[str] = []

    if isinstance(pattern, str):
        s = pattern.strip()
        if not s:
            return []
        # Mehrere Muster in einem String?
        if any(sep in s for sep in (",", ";", "|")):
            parts = re.split(r"[;,|]", s)
            raw_list = [p.strip() for p in parts if p.strip()]
        else:
            raw_list = [s]
    else:
        raw_list = [str(p).strip() for p in pattern if str(p).strip()]

    for p in raw_list:
        if not p:
            continue
        if p not in patterns:
            patterns.append(p)
        # Automatisches Ergänzen der .d-Variante für .raw-Muster
        if p.endswith(".raw"):
            base = p[:-4]  # ".raw" abschneiden
            alt = base + ".d"
            if alt not in patterns:
                patterns.append(alt)

    return patterns


def _make_hash(name: str, size: int) -> str:
    """
    Erzeugt einen stabilen Hash aus Name und Größe. Dieser Hash wird als
    Verzeichnisname für tmp/<hash>/... verwendet.
    """
    h = hashlib.sha1()
    # Name kann Pfad oder Basisname sein; wichtig ist nur, dass er stabil bleibt.
    h.update(name.encode("utf-8", errors="replace"))
    h.update(str(size).encode("ascii", errors="replace"))
    return h.hexdigest()


def _finalize_snapshot(
    history: Dict[str, Tuple[int, int]],
    *,
    min_stable_scans: int = 2,
) -> List[Dict[str, Union[str, int]]]:
    """
    Erzeugt aus der History eine Snapshot-Liste für den Kopier-Thread.

    history: { name: (size, stable_count) }
    Ein Eintrag gilt als 'stabil', wenn stable_count >= min_stable_scans ist.
    """
    snapshot: List[Dict[str, Union[str, int]]] = []
    for name, (size, stable_count) in history.items():
        if stable_count >= min_stable_scans:
            snapshot.append(
                {
                    "name": name,
                    "size": int(size),
                    "hash": _make_hash(name, int(size)),
                    "count": int(stable_count),
                }
            )

    # deterministische Reihenfolge (nur kosmetisch)
    snapshot.sort(key=lambda d: str(d.get("name", "")).lower())
    return snapshot


def start_watch_thread(
    folder: Union[str, Path],
    pattern: Union[str, Iterable[str]] = "*std.raw",
    *,
    interval_seconds: int = 60,
    recursive: bool = False,
    use_full_path: bool = False,
    ignore_file: Optional[Path] = None,
    extra_ignore_file: Optional[Path] = None,
    pre_scan_hook: Optional[Callable[[Path], None]] = None,
) -> tuple[threading.Thread, Queue, threading.Event]:
    """
    Startet einen Hintergrund-Thread, der 'folder' in einem festen Intervall
    nach passenden Dateien/Verzeichnissen durchsucht und bei stabilen Kandidaten
    Snapshots in eine Queue schreibt.

    Rückgabe:
      (thread, queue, stop_event)

    Queue-Events:
      ("snapshot", iso_timestamp, snapshot_list)
        - snapshot_list: Liste von Dicts mit Schlüsseln name/size/hash/count
    """
    folder = Path(folder).expanduser()
    if not folder.is_dir():
        raise FileNotFoundError(f"Watch-Ordner existiert nicht: {folder}")

    # Muster normalisieren (+ .d-Autovervollständigung)
    patterns = _normalize_patterns(pattern)

    q: Queue = Queue()
    stop_evt = threading.Event()

    # History pro Name: (size, stable_count)
    history: Dict[str, Tuple[int, int]] = {}

    def _worker() -> None:
        nonlocal history

        while not stop_evt.is_set():
            try:
                if pre_scan_hook is not None:
                    try:
                        pre_scan_hook(folder)
                    except Exception:
                        # Pre-Scan-Fehler sollen nicht den Watcher töten
                        pass

                ig1 = _read_ignore_list(ignore_file)
                ig2 = _read_ignore_list(extra_ignore_file)
                # zusammenführen (ohne Duplikate, Reihenfolge egal)
                ignores = list(dict.fromkeys(ig1 + ig2))

                # Größen der passenden Dateien/Verzeichnisse bestimmen
                sizes_now = file_sizes_folder(
                    folder=folder,
                    pattern=patterns,
                    ignore=ignores,
                    recursive=recursive,
                    print_output=False,
                    show_full_path=use_full_path,
                )

                # History aktualisieren: Stabilität über mehrere Scans verfolgen
                new_history: Dict[str, Tuple[int, int]] = {}

                for name, size in sizes_now.items():
                    prev_size, prev_count = history.get(name, (None, 0))
                    if prev_size == size:
                        stable_count = min(prev_count + 1, 1_000_000)
                    else:
                        # neu gesehen oder Größe hat sich geändert -> wieder bei 1 beginnen
                        stable_count = 1
                    new_history[name] = (size, stable_count)

                # History auf aktuelle Kandidaten beschränken
                history = new_history

                snapshot = _finalize_snapshot(history, min_stable_scans=2)
                ts = datetime.now().isoformat(timespec="seconds")
                q.put(("snapshot", ts, snapshot))

            except Exception as e:
                # Fehler als Event in die Queue senden, damit der Hauptthread
                # reagieren/loggen kann, aber der Watcher weiterläuft.
                ts = datetime.now().isoformat(timespec="seconds")
                q.put(("error", ts, repr(e)))

            # Warteintervall – mit früherem Abbruch, falls stop_evt gesetzt wird
            end_time = time.time() + max(1, int(interval_seconds))
            while time.time() < end_time:
                if stop_evt.is_set():
                    break
                time.sleep(0.2)

        # optionale Abschlussmeldung
        ts = datetime.now().isoformat(timespec="seconds")
        q.put(("stopped", ts, []))

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return t, q, stop_evt
