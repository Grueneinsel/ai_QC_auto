#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Dict, List, Set, Tuple
import shutil

# ruft die Job-Erzeugung auf
from .job_creater import write_from_config

def _ensure_hash_dirs(tmp_dir: Path, hash_str: str) -> tuple[Path, Path, Path]:
    """
    Stellt ./tmp/<hash>/, ./tmp/<hash>/input und ./tmp/<hash>/output bereit.
    """
    hash_dir = tmp_dir / hash_str
    input_dir = hash_dir / "input"
    output_dir = hash_dir / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return hash_dir, input_dir, output_dir

def _resolve_source_path(name_key: str, folder: Path) -> Path:
    p = Path(name_key)
    return p if p.is_absolute() else (folder / p)

def _append_to_ignore_filenames(ignore_file: Path, names: Iterable[str]) -> int:
    """
    Hängt eindeutige **Dateinamen** (Basenames) an ignore_file an.
    Gibt die Anzahl neu hinzugefügter Einträge zurück.
    """
    ignore_file.parent.mkdir(parents=True, exist_ok=True)
    existing: Set[str] = set()
    if ignore_file.exists():
        existing = {
            ln.strip()
            for ln in ignore_file.read_text(encoding="utf-8", errors="ignore").splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")
        }

    to_add: List[str] = []
    for n in names:
        base = Path(n).name.strip()
        if base and base not in existing:
            to_add.append(base)

    if to_add:
        with ignore_file.open("a", encoding="utf-8") as f:
            for s in to_add:
                f.write(s + "\n")
    return len(to_add)

def copy_candidates(
    snapshot: List[Dict],
    *,
    folder: Path,
    tmp_dir: Path,
    copied_cache: Set[Tuple[str,int]],
    ignore_file: Path | None = None,  # je Thread eigene Datei im tmp/
    add_to_ignore: bool = True,
) -> tuple[List[Path], int]:
    """
    Kopiert alle Items (name,size,hash,count) einmalig nach tmp/<hash>/input/.
    Verhindert Doppelkopien via copied_cache (name,size).

    Zusätzlich: ruft write_from_config(...) auf, um in tmp/<hash>/ die mcquac.json
    aus ./config/mcquac.json zu erzeugen:
      INPUT  = tmp/<hash>/input
      OUTPUT = tmp/<hash>/output  (Ordner wird angelegt)

    Fügt **nur den Dateinamen** der kopierten Quelle in ignore_file hinzu (falls gesetzt).

    Returns: (Liste der neu kopierten Zielpfade, Anzahl neu hinzugefügter Ignore-Zeilen)
    """
    copied_paths: List[Path] = []
    names_for_ignore: List[str] = []
    jobs_written_for_hash: Set[str] = set()  # in diesem Aufruf bereits erzeugte Jobs

    for item in snapshot:
        name = item["name"]
        size = item["size"]
        h    = item["hash"]
        key = (name, size)
        if key in copied_cache:
            continue

        src = _resolve_source_path(name, folder)
        if not src.is_file():
            continue

        # Zielordner vorbereiten
        hash_dir, input_dir, output_dir = _ensure_hash_dirs(tmp_dir, h)

        # Datei kopieren
        dst = input_dir / src.name
        try:
            shutil.copy2(src, dst)
            copied_cache.add(key)
            copied_paths.append(dst)
            if add_to_ignore and ignore_file is not None:
                names_for_ignore.append(src.name)  # nur Basename

            # mcquac.json in diesem Hash-Ordner erzeugen (einmal pro Hash)
            if h not in jobs_written_for_hash:
                try:
                    write_from_config(
                        input_value=str(input_dir),
                        output_value=str(output_dir),
                        inner_folder=h,                 # schreibt nach ./tmp/<hash>/mcquac.json
                        template_filename="mcquac.json",
                        config_dir="config",
                    )
                except Exception:
                    # bewusst still; optional Logging möglich
                    pass
                jobs_written_for_hash.add(h)

        except Exception:
            # bewusst still; optional Logging möglich
            pass

    added = 0
    if add_to_ignore and ignore_file is not None and names_for_ignore:
        added = _append_to_ignore_filenames(ignore_file, names_for_ignore)

    return copied_paths, added
