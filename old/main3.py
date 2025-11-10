#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import sys, re

# --- Projektroot & Imports absichern ---
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clear import nuke_tmp
from src.load_config import load_config
from src.search import start_watch_thread            # unterstützt extra_ignore_file
from src.copier import copy_candidates               # erzeugt mcquac.json + info.json

TMP_DIR = ROOT / "tmp"

def slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s).strip()) or "x"

if __name__ == "__main__":
    # tmp leeren (falls unerwünscht: auskommentieren)
    cleaned = nuke_tmp(ROOT)
    print(f"Bereinigt: {cleaned}\n")

    # Config laden (expects interval_seconds in config/app.json)
    cfg = load_config()
    print("Starte Überwachung aus config/app.json …")
    print(f"  Interval: {cfg.interval_seconds} s")
    print(f"  default_pattern: {cfg.default_pattern}")
    print(f"  mcquac_path:     {cfg.mcquac_path}\n")

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Für jedes IO-Pair einen Watcher-Thread starten
    watchers = []
    for i, pair in enumerate(cfg.io_pairs, start=1):
        folder_in  = pair.input
        final_out  = pair.output
        pattern    = pair.pattern or cfg.default_pattern
        use_full   = True
        recursive  = False
        interval_s = cfg.interval_seconds

        # thread-spezifische Ignore-Datei im tmp/
        ign_thread = TMP_DIR / f"ignore-{slug(folder_in)}-{slug(pattern)}.txt"
        ign_thread.touch(exist_ok=True)

        # zweite Ignore-Datei: ignore.txt im Output-Ordner des Pairs
        final_out.mkdir(parents=True, exist_ok=True)           # sicherstellen, dass es den Ordner gibt
        ign_output = final_out / "ignore.txt"
        ign_output.touch(exist_ok=True)

        print(f"[Watcher {i}]")
        print(f"  IN : {folder_in}")
        print(f"  OUT: {final_out}")
        print(f"  PAT: {pattern}")
        print(f"  IGN: {ign_thread.name} + {ign_output}")      # zeigt Pfad der output-ignore.txt
        print()

        t, q, stop_evt = start_watch_thread(
            folder=folder_in,
            pattern=pattern,
            recursive=recursive,
            interval_seconds=interval_s,   # **Sekunden**
            use_full_path=use_full,
            ignore_file=ign_thread,        # thread-spezifische Ignore (im tmp/)
            extra_ignore_file=ign_output,  # << Ignore-Datei im Output-Ordner des Pairs
        )
        watchers.append({
            "idx": i,
            "thread": t,
            "queue": q,
            "stop": stop_evt,
            "in_root": folder_in,
            "final_out": final_out,
            "pattern": pattern,
            "interval_s": interval_s,
            "ign_thread": ign_thread,
            "ign_output": ign_output,
        })

    # globales Cache, um Doppelkopien über alle Threads hinweg zu vermeiden
    copied_cache: set[tuple[str,int]] = set()

    try:
        while True:
            # Round-robin: alle Queues kurz abfragen
            for w in watchers:
                q = w["queue"]
                try:
                    kind, ts, snapshot = q.get(timeout=1.0)
                except Exception:
                    continue

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
                    info_for_hash=info_for_hash,   # -> src/copier.py legt info.json an
                )
                for p in new_paths:
                    print(f"    -> COPIED to {p}")
                if added_ign:
                    print(f"    -> {added_ign} Name(n) zu {w['ign_thread'].name} hinzugefügt")

    except KeyboardInterrupt:
        print("\nBeende Überwachung …")
        for w in watchers:
            w["stop"].set()
            w["thread"].join(timeout=2)
        print("Fertig.")
