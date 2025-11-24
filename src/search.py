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

from .size import file_sizes_folder  # uses your existing helper (supports .d directories)


def _read_ignore_list(path: Path | None) -> List[str]:
    """
    Read an ignore file (one pattern per line) and return a list of
    glob/fnmatch patterns. Lines starting with '#' are ignored.
    """
    if not path or not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]


def _normalize_patterns(pattern: Union[str, Iterable[str]]) -> List[str]:
    """
    Normalize the pattern argument into a list of glob patterns.

    Supports:
      - simple strings: "*std.raw"
      - multiple patterns in a single string, separated by ',', ';' or '|':
          "*std.raw,*std.d" or "*std.raw|*std.d"
      - iterables that are already given: ["*std.raw", "*std.d"]

    Additionally:
      - If a pattern ends with ".raw", the corresponding ".d" variant is
        automatically added (e.g. "*std.raw" -> ["*std.raw", "*std.d"]),
        if it is not already present.
    """
    patterns: List[str] = []

    if isinstance(pattern, str):
        s = pattern.strip()
        if not s:
            return []
        # Multiple patterns in a single string?
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
        # Automatically add the .d variant for .raw patterns
        if p.endswith(".raw"):
            base = p[:-4]  # cut off ".raw"
            alt = base + ".d"
            if alt not in patterns:
                patterns.append(alt)

    return patterns


def _make_hash(name: str, size: int) -> str:
    """
    Create a stable hash from name and size. This hash is used as
    directory name for tmp/<hash>/...
    """
    h = hashlib.sha1()
    # Name may be a path or a basename; it only needs to be stable.
    h.update(name.encode("utf-8", errors="replace"))
    h.update(str(size).encode("ascii", errors="replace"))
    return h.hexdigest()


def _finalize_snapshot(
    history: Dict[str, Tuple[int, int]],
    *,
    min_stable_scans: int = 2,
) -> List[Dict[str, Union[str, int]]]:
    """
    Create a snapshot list for the copy thread from the history.

    history: { name: (size, stable_count) }
    An entry is considered 'stable' if stable_count >= min_stable_scans.
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

    # deterministic order (cosmetic only)
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
    Start a background thread that scans `folder` at a fixed interval
    for matching files/directories and writes snapshots to a queue
    once candidates are stable.

    Returns:
      (thread, queue, stop_event)

    Queue events:
      ("snapshot", iso_timestamp, snapshot_list)
        - snapshot_list: list of dicts with keys name/size/hash/count
    """
    folder = Path(folder).expanduser()
    if not folder.is_dir():
        raise FileNotFoundError(f"Watch folder does not exist: {folder}")

    # Normalize patterns (+ automatic .d completion)
    patterns = _normalize_patterns(pattern)

    q: Queue = Queue()
    stop_evt = threading.Event()

    # History per name: (size, stable_count)
    history: Dict[str, Tuple[int, int]] = {}

    def _worker() -> None:
        nonlocal history

        while not stop_evt.is_set():
            try:
                if pre_scan_hook is not None:
                    try:
                        pre_scan_hook(folder)
                    except Exception:
                        # Pre-scan failures should not kill the watcher
                        pass

                ig1 = _read_ignore_list(ignore_file)
                ig2 = _read_ignore_list(extra_ignore_file)
                # Merge (no duplicates, order irrelevant)
                ignores = list(dict.fromkeys(ig1 + ig2))

                # Determine sizes of matching files/directories
                sizes_now = file_sizes_folder(
                    folder=folder,
                    pattern=patterns,
                    ignore=ignores,
                    recursive=recursive,
                    print_output=False,
                    show_full_path=use_full_path,
                )

                # Update history: track stability over multiple scans
                new_history: Dict[str, Tuple[int, int]] = {}

                for name, size in sizes_now.items():
                    prev_size, prev_count = history.get(name, (None, 0))
                    if prev_size == size:
                        stable_count = min(prev_count + 1, 1_000_000)
                    else:
                        # newly seen or size changed -> start at 1 again
                        stable_count = 1
                    new_history[name] = (size, stable_count)

                # Restrict history to current candidates
                history = new_history

                snapshot = _finalize_snapshot(history, min_stable_scans=2)
                ts = datetime.now().isoformat(timespec="seconds")
                q.put(("snapshot", ts, snapshot))

            except Exception as e:
                # Send errors as events to the queue so the main thread can
                # react/log, but keep the watcher running.
                ts = datetime.now().isoformat(timespec="seconds")
                q.put(("error", ts, repr(e)))

            # Wait interval – with early exit if stop_evt is set
            end_time = time.time() + max(1, int(interval_seconds))
            while time.time() < end_time:
                if stop_evt.is_set():
                    break
                time.sleep(0.2)

        # Optional final message
        ts = datetime.now().isoformat(timespec="seconds")
        q.put(("stopped", ts, []))

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return t, q, stop_evt
