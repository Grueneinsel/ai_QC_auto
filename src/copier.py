#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Dict, List, Set, Tuple, Optional
from datetime import datetime
import json, shutil

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

def _write_info_json(
    hash_dir: Path,
    *,
    hash_str: str,
    input_dir: Path,
    output_dir: Path,
    src_abs: Path,
    size: int,
    info_extra: Optional[dict] = None,
    filename: str = "info.json",
    overwrite: bool = False,
) -> Path:
    """
    Schreibt ./tmp/<hash>/info.json mit allen relevanten Pfaden/Parametern.
    """
    info_path = hash_dir / filename
    if info_path.exists() and not overwrite:
        return info_path

    info: dict = {
        "hash": hash_str,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "paths": {
            "hash_dir": str(hash_dir),
            "tmp_input_dir": str(input_dir),
            "tmp_output_dir": str(output_dir),
            "mcquac_json": str(hash_dir / "mcquac.json"),
            "source_file": str(src_abs),
        },
        "source": {
            "name": src_abs.name,
            "absolute_path": str(src_abs),
            "size_bytes": size,
        },
    }
    if info_extra:
        info["watch"] = {
            "input_root": str(info_extra.get("input_root", "")),
            "final_output_root": str(info_extra.get("final_output_root", "")),
            "pattern": str(info_extra.get("pattern", "")),
            "interval_seconds": int(info_extra.get("interval_seconds", 0)),
            "ignore_files": [str(p) for p in info_extra.get("ignore_files", [])],
        }

    info_path.write_text(json.dumps(info, indent=2), encoding="utf-8")
    return info_path

def _write_ready_file(hash_dir: Path, filename: str = ".ready", overwrite: bool = False) -> Path:
    """
    Legt ./tmp/<hash>/.ready mit Zeitstempel an (oder lässt bestehende Datei stehen).
    """
    p = hash_dir / filename
    if p.exists() and not overwrite:
        return p
    p.write_text(datetime.now().isoformat(timespec="seconds") + "\n", encoding="utf-8")
    return p

def copy_candidates(
    snapshot: List[Dict],
    *,
    folder: Path,
    tmp_dir: Path,
    copied_cache: Set[Tuple[str,int]],
    ignore_file: Path | None = None,  # je Thread eigene Datei im tmp/
    add_to_ignore: bool = True,
    # Zusatz: Metadaten für info.json (werden pro Hash geschrieben)
    info_for_hash: Optional[dict] = None,
) -> tuple[List[Path], int]:
    """
    Kopiert alle Items (name,size,hash,count) einmalig nach tmp/<hash>/input/.
    Verhindert Doppelkopien via copied_cache (name,size).

    Zusätzlich:
      - erzeugt mcquac.json via write_from_config(...) mit INPUT=tmp/<hash>/input, OUTPUT=tmp/<hash>/output
      - schreibt info.json mit allen Pfaden & Parametern für später (aus info_for_hash)
      - legt .ready im Hash-Ordner an, sobald Job & Info erzeugt sind

    Fügt **nur den Dateinamen** der kopierten Quelle in ignore_file hinzu (falls gesetzt).

    Returns: (Liste der neu kopierten Zielpfade, Anzahl neu hinzugefügter Ignore-Zeilen)
    """
    copied_paths: List[Path] = []
    names_for_ignore: List[str] = []
    jobs_written_for_hash: Set[str] = set()  # in diesem Aufruf bereits erzeugte Jobs/Infos

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

            # pro Hash: mcquac.json & info.json & .ready (nur 1x pro Aufruf)
            if h not in jobs_written_for_hash:
                try:
                    # mcquac.json
                    write_from_config(
                        input_value=str(input_dir),
                        output_value=str(output_dir),
                        inner_folder=h,  # schreibt nach ./tmp/<hash>/mcquac.json
                        template_filename="mcquac.json",
                        config_dir="config",
                    )
                    # info.json
                    _write_info_json(
                        hash_dir,
                        hash_str=h,
                        input_dir=input_dir,
                        output_dir=output_dir,
                        src_abs=src.resolve(),
                        size=size,
                        info_extra=info_for_hash or {},
                        filename="info.json",
                        overwrite=False,
                    )
                    # .ready
                    _write_ready_file(hash_dir, filename=".ready", overwrite=False)

                except Exception:
                    # bewusst still; optional Logging/Print ergänzen
                    pass
                jobs_written_for_hash.add(h)

        except Exception:
            # bewusst still; optional Logging/Print ergänzen
            pass

    added = 0
    if add_to_ignore and ignore_file is not None and names_for_ignore:
        added = _append_to_ignore_filenames(ignore_file, names_for_ignore)

    return copied_paths, added
