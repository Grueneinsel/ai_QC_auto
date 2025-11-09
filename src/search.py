#!/usr/bin/env python3
from pathlib import Path
from collections import defaultdict
from queue import Queue
from datetime import datetime
import threading, time, sys, hashlib, fnmatch

# ---- Defaults (anpassbar) ----
FOLDER   = r"/mnt/c/Users/info/OneDrive/Rub/StudienProject/StudienProject_01_10_2025/MS raw data"
PATTERN  = "*std.raw"      # z. B. "*.raw" oder ["*std.raw", "*.raw"]
RECURSIVE = False
INTERVAL_SECONDS = 5
USE_FULL_PATH = True
IGNORE_FILE = "ignore.txt"  # liegt im Ausführungsordner

# ---- Import aus src/size.py ----
ROOT = Path(__file__).resolve().parent
for p in {ROOT, ROOT.parent}:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
from src.size import file_sizes_folder  # erwartet src/size.py mit file_sizes_folder(...)

# ---- Helpers ----
def read_ignore_list(path: Path) -> list[str]:
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]

def make_hash(name_key: str, size: int, digest_bytes: int = 16) -> str:
    """Deterministischer Hash (BLAKE2b) aus Name/Pfad und Größe."""
    return hashlib.blake2b(f"{name_key}|{size}".encode("utf-8"), digest_size=digest_bytes).hexdigest()

def finalize_snapshot(counts: dict[tuple[str,int], int], ignores: list[str]) -> list[dict]:
    """Filtert ignorierte komplett heraus, gibt nur count>=2 zurück."""
    result = []
    for (name_key, size), cnt in sorted(counts.items(), key=lambda kv: (kv[0][0].lower(), kv[0][1])):
        if cnt < 2:
            continue
        base = Path(name_key).name
        if any(fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(name_key, pat) for pat in ignores):
            continue
        result.append({"name": name_key, "size": size, "count": cnt, "hash": make_hash(name_key, size)})
    return result

# ---- Worker-Thread: läuft unendlich und liefert Snapshots ----
def watch_folder_stream(
    result_queue: Queue,
    folder: str | Path = FOLDER,
    pattern = PATTERN,                # str oder Liste[str]
    recursive: bool = RECURSIVE,
    interval_seconds: int = INTERVAL_SECONDS,
    use_full_path: bool = USE_FULL_PATH,
    ignore_file: str | Path = IGNORE_FILE,
    stop_event: threading.Event | None = None,
) -> None:
    folder = Path(folder)
    counts: dict[tuple[str, int], int] = defaultdict(int)

    while True:
        if stop_event and stop_event.is_set():
            break

        # Ignore-Liste jedes Mal neu laden
        ignores_dynamic = read_ignore_list(Path.cwd() / str(ignore_file))

        # Scan (file_sizes_folder berücksichtigt ignore bereits beim Finden)
        sizes_now = file_sizes_folder(
            folder=folder,
            pattern=pattern,
            ignore=ignores_dynamic,
            recursive=recursive,
            print_output=False,
            show_full_path=use_full_path,
        )

        # Zählen (nur nicht-ignorierte wurden geliefert)
        for name, size in sizes_now.items():
            counts[(name, size)] += 1

        # Snapshot bauen (ignorierte erneut komplett herausfiltern, count>=2)
        snapshot = finalize_snapshot(counts, ignores_dynamic)
        ts = datetime.now().isoformat(timespec="seconds")
        result_queue.put(("snapshot", ts, snapshot))

        time.sleep(interval_seconds)

def start_watch_thread(**kwargs) -> tuple[threading.Thread, Queue, threading.Event]:
    q: Queue = Queue()
    stop_evt = threading.Event()
    t = threading.Thread(target=watch_folder_stream, kwargs={"result_queue": q, "stop_event": stop_evt, **kwargs}, daemon=True)
    t.start()
    return t, q, stop_evt

# ---- Lauf / fortlaufende Ausgabe ----
if __name__ == "__main__":
    print("Starte endlose Überwachung … (Strg+C zum Beenden)")
    print(f"  Ordner:   {FOLDER}\n  Pattern:  {PATTERN}\n  Intervall:{INTERVAL_SECONDS}s\n  Ignore:   {Path.cwd()/IGNORE_FILE}\n")

    t, q, stop_evt = start_watch_thread(
        folder=FOLDER,
        pattern=PATTERN,
        recursive=RECURSIVE,
        interval_seconds=INTERVAL_SECONDS,
        use_full_path=USE_FULL_PATH,
        ignore_file=IGNORE_FILE,
    )

    try:
        while True:
            kind, ts, snapshot = q.get()  # blockiert bis neuer Snapshot kommt
            print("-" * 60)
            print(f"[{ts}] Dateien (min. 2× identisch, ignorierte entfernt):")
            if not snapshot:
                print("  (keine Treffer)")
            else:
                for item in snapshot:
                    print(f"  {item['count']}×  size={item['size']}  hash={item['hash']}  name={item['name']}")
    except KeyboardInterrupt:
        print("\nBeende Überwachung …")
        stop_evt.set()
        t.join(timeout=2)
        print("Fertig.")
