#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
from collections import defaultdict
from queue import Queue
from datetime import datetime
from typing import Iterable, Union, Optional, Dict, List
import threading, time, fnmatch, hashlib

from .size import file_sizes_folder  # nutzt deine bestehende Funktion

def _read_ignore_list(path: Path) -> list[str]:
    if not path or not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]

def _make_hash(name_key: str, size: int, digest_bytes: int = 32) -> str:
    return hashlib.blake2b(f"{name_key}|{size}".encode("utf-8"), digest_size=digest_bytes).hexdigest()

def _finalize_snapshot(counts: Dict[tuple[str,int], int], ignores: list[str]) -> list[dict]:
    """Filtert ignorierte raus, gibt nur count>=2 zurück (mit Hash)."""
    result: List[dict] = []
    for (name_key, size), cnt in sorted(counts.items(), key=lambda kv: (kv[0][0].lower(), kv[0][1])):
        if cnt < 2:
            continue
        base = Path(name_key).name
        if any(fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(name_key, pat) for pat in ignores):
            continue
        result.append({"name": name_key, "size": size, "count": cnt, "hash": _make_hash(name_key, size)})
    return result

def start_watch_thread(
    folder: Union[str, Path],
    *,
    pattern: Union[str, Iterable[str]] = "*std.raw",
    recursive: bool = False,
    interval_seconds: int = 5,
    use_full_path: bool = True,
    ignore_file: Union[str, Path, None] = None,  # << exakt diese Datei wird genutzt (z. B. tmp/ignore-*.txt)
) -> tuple[threading.Thread, Queue, threading.Event]:
    """
    Startet einen Endlos-Thread, der alle 'interval_seconds' scannt und Snapshots liefert.
    Snapshot-Format: Liste von Dicts mit keys: name, size, count, hash (nur count>=2).
    'ignore_file' ist der Pfad zu der pro-Thread-Ignore-Datei (liegt z. B. in tmp/).
    """
    folder = Path(folder)
    ignore_path = Path(ignore_file) if ignore_file is not None else None

    q: Queue = Queue()
    stop_evt = threading.Event()

    def _worker() -> None:
        counts: Dict[tuple[str,int], int] = defaultdict(int)
        while not stop_evt.is_set():
            ignores = _read_ignore_list(ignore_path) if ignore_path else []
            sizes_now = file_sizes_folder(
                folder=folder,
                pattern=pattern,
                ignore=ignores,
                recursive=recursive,
                print_output=False,
                show_full_path=use_full_path,
            )
            for name, size in sizes_now.items():
                counts[(name, size)] += 1

            snapshot = _finalize_snapshot(counts, ignores)
            ts = datetime.now().isoformat(timespec="seconds")
            q.put(("snapshot", ts, snapshot))
            time.sleep(interval_seconds)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return t, q, stop_evt
