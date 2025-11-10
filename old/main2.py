#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clear import nuke_tmp
from src.search import start_watch_thread
from src.copier import copy_candidates

FOLDER   = Path(r"/mnt/c/Users/info/OneDrive/Rub/StudienProject/StudienProject_01_10_2025/MS raw data")
TMP_DIR  = ROOT / "tmp"
INTERVAL_SECONDS = 5
USE_FULL_PATH = True
RECURSIVE = False
PATTERN = "*std.raw"

IGNORE_FILE_THREAD = TMP_DIR / "ignore-std.txt"  # 1) thread-spezifisch (im tmp)
IGNORE_FILE_GLOBAL = ROOT / "ignore-global.txt"  # 2) fixe Datei im Ausführungsordner

if __name__ == "__main__":
    cleaned = nuke_tmp(ROOT)
    print(f"Bereinigt: {cleaned}")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    print("Starte Überwachung … (Strg+C zum Beenden)")
    print(f"  Ordner:   {FOLDER}\n  TMP:      {TMP_DIR}\n  Pattern:  {PATTERN}")
    print(f"  Ignore 1: {IGNORE_FILE_THREAD}")
    print(f"  Ignore 2: {IGNORE_FILE_GLOBAL}\n")

    t, q, stop_evt = start_watch_thread(
        folder=FOLDER,
        pattern=PATTERN,
        recursive=RECURSIVE,
        interval_seconds=INTERVAL_SECONDS,
        use_full_path=USE_FULL_PATH,
        ignore_file=IGNORE_FILE_THREAD,      # im tmp
        extra_ignore_file=IGNORE_FILE_GLOBAL # fixe Datei im Ausführungsordner
    )

    copied_cache: set[tuple[str,int]] = set()

    try:
        while True:
            kind, ts, snapshot = q.get()
            print("-" * 60)
            print(f"[{ts}] Kandidaten (min. 2×, ignoriert via {IGNORE_FILE_THREAD.name} + {IGNORE_FILE_GLOBAL.name}):")
            if not snapshot:
                print("  (keine Treffer)")
                continue

            for item in snapshot:
                print(f"  {item['count']}×  size={item['size']}  hash={item['hash']}  name={item['name']}")

            new_paths, added_ign = copy_candidates(
                snapshot,
                folder=FOLDER,
                tmp_dir=TMP_DIR,
                copied_cache=copied_cache,
                ignore_file=IGNORE_FILE_THREAD,  # nur Dateiname wird angehängt (in die thread-spezifische Datei)
                add_to_ignore=True,
            )
            for p in new_paths:
                print(f"    -> COPIED to {p}")
            if added_ign:
                print(f"    -> {added_ign} Name(n) zu {IGNORE_FILE_THREAD.name} hinzugefügt")
    except KeyboardInterrupt:
        print("\nBeende Überwachung …")
        stop_evt.set()
        t.join(timeout=2)
        print("Fertig.")
