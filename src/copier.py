#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, List, Set, Tuple, Optional, Any
from datetime import datetime
from functools import lru_cache
import json
import shutil

# Projektwurzel (eine Ebene über /src)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# FASTA: Top-Level in config/fasta
FASTA_DIR = (PROJECT_ROOT / "config" / "fasta").resolve()

# SPIKE: Top-Level in config/spike
SPIKE_DIR = (PROJECT_ROOT / "config" / "spike").resolve()

# ruft die Job-Erzeugung auf
from .job_creater import write_from_config


# ----------------------------- Hilfsfunktionen -----------------------------


def _ensure_hash_dirs(tmp_dir: Path, hash_str: str) -> tuple[Path, Path, Path]:
    """
    Legt tmp/<hash>/{input,output} an und gibt diese Pfade zurück.
    """
    hash_dir = tmp_dir / hash_str
    input_dir = hash_dir / "input"
    output_dir = hash_dir / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return hash_dir, input_dir, output_dir


def _resolve_source_path(name_key: str, folder: Path) -> Path:
    """
    Wandelt das 'name'-Feld aus dem Snapshot in einen absoluten Pfad um.
    - Wenn 'name' bereits absolut ist -> direkt verwenden.
    - Sonst relativ zu 'folder'.
    """
    p = Path(name_key)
    return p if p.is_absolute() else (folder / p)


def _append_to_ignore_filenames(ignore_file: Path, names: Iterable[str]) -> int:
    """
    Hängt Basis-Dateinamen an eine ignore.txt an (wenn noch nicht vorhanden).
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
    Schreibt info.json in tmp/<hash>/ mit Meta-Informationen:
    - Pfade zu tmp/input, tmp/output, mcquac.json
    - Quelle (Name, absoluter Pfad, Größe, ob Verzeichnis)
    - FASTA-/Spike-Info
    - Optional: Watcher-Infos (input_root, final_output_root, pattern, interval_seconds, ignore_files)
    """
    info_path = hash_dir / filename
    if info_path.exists() and not overwrite:
        return info_path

    fasta_file = _cached_fasta_file()
    spike_file = _cached_spike_file()

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
            "size_bytes": int(size),
            # NEU: Hilfsflag, ob es sich um einen Ordner (z. B. *.d) handelt
            "is_dir": src_abs.is_dir(),
        },
        "fasta": {
            "dir": str(FASTA_DIR),
            "file": str(fasta_file) if fasta_file else None,
        },
        "spike": {
            "dir": str(SPIKE_DIR),
            "file": str(spike_file) if spike_file else None,
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
    Legt tmp/<hash>/.ready an (oder lässt vorhandene Datei stehen).
    """
    p = hash_dir / filename
    if p.exists() and not overwrite:
        return p
    p.write_text(datetime.now().isoformat(timespec="seconds") + "\n", encoding="utf-8")
    return p


# ----------------------------- FASTA finden -----------------------------


def _find_fasta_file(base_dir: Path = FASTA_DIR) -> Optional[Path]:
    """
    Sucht im Top-Level von config/fasta nach *.fasta.
    Bei mehreren: jüngste zuerst, dann größere, dann Name.
    """
    if not base_dir.is_dir():
        return None
    candidates = [p for p in base_dir.glob("*.fasta") if p.is_file()]
    if not candidates:
        return None

    def key(p: Path):
        try:
            st = p.stat()
            return (float(st.st_mtime), int(st.st_size), p.name.lower())
        except Exception:
            return (0.0, 0, p.name.lower())

    return max(candidates, key=key).resolve()


@lru_cache(maxsize=1)
def _cached_fasta_file() -> Optional[Path]:
    return _find_fasta_file(FASTA_DIR)


# ----------------------------- SPIKE finden -----------------------------


def _find_spike_file(base_dir: Path = SPIKE_DIR) -> Optional[Path]:
    """
    Sucht im Top-Level von config/spike nach *.csv.
    Bei mehreren: jüngste zuerst, dann größere, dann Name.
    """
    if not base_dir.is_dir():
        return None
    candidates = [p for p in base_dir.glob("*.csv") if p.is_file()]
    if not candidates:
        return None

    def key(p: Path):
        try:
            st = p.stat()
            return (float(st.st_mtime), int(st.st_size), p.name.lower())
        except Exception:
            return (0.0, 0, p.name.lower())

    return max(candidates, key=key).resolve()


@lru_cache(maxsize=1)
def _cached_spike_file() -> Optional[Path]:
    return _find_spike_file(SPIKE_DIR)


# ---------------------- mcquac.json injizieren (+ Platzhalter) ----------------------


def _json_replace_placeholder(obj: Any, placeholder: str, value: str) -> Any:
    """
    Ersetzt rekursiv Strings, die exakt dem Platzhalter entsprechen.
    """
    if isinstance(obj, dict):
        return {k: _json_replace_placeholder(v, placeholder, value) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_replace_placeholder(v, placeholder, value) for v in obj]
    if isinstance(obj, str) and obj == placeholder:
        return value
    return obj


def _inject_fasta_file_in_mcquac(mcquac_path: Path) -> None:
    """
    Trägt 'main_fasta_file' ein und ersetzt optionale FASTA-Platzhalter.
    """
    if not mcquac_path.is_file():
        return
    fasta = _cached_fasta_file()
    if not fasta:
        return
    try:
        data = json.loads(mcquac_path.read_text(encoding="utf-8"))
    except Exception:
        return

    data["main_fasta_file"] = str(fasta)
    # Platzhalter-Varianten ersetzen (zur Sicherheit beide)
    for ph in ("%%%FASTA%%%", "%%%FASTA%%%%"):
        data = _json_replace_placeholder(data, ph, str(fasta))

    try:
        mcquac_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def _inject_spike_file_in_mcquac(mcquac_path: Path) -> None:
    """
    Trägt 'main_spike_file' ein und ersetzt optionale SPIKE-Platzhalter.
    """
    if not mcquac_path.is_file():
        return
    spike = _cached_spike_file()
    if not spike:
        return
    try:
        data = json.loads(mcquac_path.read_text(encoding="utf-8"))
    except Exception:
        return

    data["main_spike_file"] = str(spike)
    for ph in ("%%%SPIKE%%%", "%%%SPIKE%%%%"):
        data = _json_replace_placeholder(data, ph, str(spike))

    try:
        mcquac_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


# ----------------------------- Hauptfunktion -----------------------------


def copy_candidates(
    snapshot: List[Dict],
    *,
    folder: Path,
    tmp_dir: Path,
    copied_cache: Set[Tuple[str, int]],
    ignore_file: Path | None = None,  # je Thread eigene Datei im tmp/
    add_to_ignore: bool = True,
    # Zusatz: Metadaten für info.json (werden pro Hash geschrieben)
    info_for_hash: Optional[dict] = None,
) -> tuple[List[Path], int]:
    """
    Kopiert alle Items (name,size,hash,count) einmalig nach tmp/<hash>/input/.
    Verhindert Doppelkopien via copied_cache (name,size).

    Zusätzlich:
      - erzeugt mcquac.json via write_from_config(...) mit
        INPUT=tmp/<hash>/input und OUTPUT=tmp/<hash>/output
      - setzt 'main_fasta_file' (gefunden in config/fasta) und
        'main_spike_file' (gefunden in config/spike)
      - ersetzt optionale Platzhalter %%%FASTA%%% / %%%FASTA%%%% und
        %%%SPIKE%%% / %%%SPIKE%%%% in mcquac.json
      - schreibt info.json mit allen Pfaden & Parametern für später (aus info_for_hash)
      - legt .ready im Hash-Ordner an, sobald Job & Info erzeugt sind

    WICHTIG:
      - Es werden sowohl normale Dateien (z.B. *.raw) als auch Verzeichnisse
        (z.B. *.d) unterstützt. Verzeichnisse werden komplett rekursiv kopiert.
    """
    folder = Path(folder)
    tmp_dir = Path(tmp_dir)

    copied_paths: List[Path] = []
    names_for_ignore: List[str] = []
    jobs_written_for_hash: Set[str] = set()  # in diesem Aufruf bereits erzeugte Jobs/Infos

    for item in snapshot:
        try:
            name = item["name"]
            size = item["size"]
            h = item["hash"]
        except KeyError:
            # falls Snapshot-Eintrag nicht das erwartete Format hat, überspringen
            continue

        key = (name, size)
        if key in copied_cache:
            continue

        src = _resolve_source_path(name, folder)
        if not src.exists():
            # Quelle ist verschwunden -> überspringen
            continue

        # Zielordner vorbereiten
        hash_dir, input_dir, output_dir = _ensure_hash_dirs(tmp_dir, h)

        # Zielpfad (gleicher Name wie Quelle, im input/)
        dst = input_dir / src.name

        try:
            # Dateien UND Verzeichnisse behandeln
            if src.is_dir():
                # z.B. Bruker-Folder *.d
                if dst.exists() and dst.is_file():
                    dst.unlink()
                # dirs_exist_ok=True erlaubt Wiederanläufe ohne Fehler
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)

            copied_cache.add(key)
            copied_paths.append(dst)
            if add_to_ignore and ignore_file is not None:
                # nur der Basename kommt in die ignore.txt
                names_for_ignore.append(src.name)

            # pro Hash: mcquac.json & info.json & .ready (nur 1x pro Aufruf)
            if h not in jobs_written_for_hash:
                try:
                    # mcquac.json erzeugen
                    write_from_config(
                        input_value=str(input_dir),
                        output_value=str(output_dir),
                        inner_folder=h,  # schreibt nach ./tmp/<hash>/mcquac.json
                        template_filename="mcquac.json",
                        config_dir="config",
                    )

                    mcquac = hash_dir / "mcquac.json"
                    # FASTA & SPIKE eintragen (und Platzhalter ersetzen)
                    _inject_fasta_file_in_mcquac(mcquac)
                    _inject_spike_file_in_mcquac(mcquac)

                    # info.json
                    _write_info_json(
                        hash_dir,
                        hash_str=h,
                        input_dir=input_dir,
                        output_dir=output_dir,
                        src_abs=src,
                        size=size,
                        info_extra=info_for_hash,
                    )

                    # .ready-Datei anlegen
                    _write_ready_file(hash_dir)

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
