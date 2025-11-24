#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, List, Set, Tuple, Optional, Any
from datetime import datetime
from functools import lru_cache
import json
import shutil

# Project root (one level above /src)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# FASTA: top-level in config/fasta
FASTA_DIR = (PROJECT_ROOT / "config" / "fasta").resolve()

# SPIKE: top-level in config/spike
SPIKE_DIR = (PROJECT_ROOT / "config" / "spike").resolve()

# triggers job creation
from .job_creater import write_from_config


# ----------------------------- Helper functions -----------------------------


def _ensure_hash_dirs(tmp_dir: Path, hash_str: str) -> tuple[Path, Path, Path]:
    """
    Create tmp/<hash>/{input,output} and return these paths.
    """
    hash_dir = tmp_dir / hash_str
    input_dir = hash_dir / "input"
    output_dir = hash_dir / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return hash_dir, input_dir, output_dir


def _resolve_source_path(name_key: str, folder: Path) -> Path:
    """
    Turn the 'name' field from the snapshot into an absolute path.
    - If 'name' is already absolute -> use directly.
    - Otherwise, resolve relative to 'folder'.
    """
    p = Path(name_key)
    return p if p.is_absolute() else (folder / p)


def _append_to_ignore_filenames(ignore_file: Path, names: Iterable[str]) -> int:
    """
    Append base file names to an ignore.txt (if not already present).
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
    Write info.json into tmp/<hash>/ with meta information:
    - paths to tmp/input, tmp/output, mcquac.json
    - source (name, absolute path, size, whether it is a directory)
    - FASTA / spike info
    - Optional: watcher info (input_root, final_output_root, pattern,
      interval_seconds, ignore_files)
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
            # Helper flag indicating whether this is a directory (e.g. *.d)
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
    Create tmp/<hash>/.ready (or keep an existing file).
    """
    p = hash_dir / filename
    if p.exists() and not overwrite:
        return p
    p.write_text(datetime.now().isoformat(timespec="seconds") + "\n", encoding="utf-8")
    return p


# ----------------------------- FASTA lookup -----------------------------


def _find_fasta_file(base_dir: Path = FASTA_DIR) -> Optional[Path]:
    """
    Search top-level of config/fasta for *.fasta.
    If multiple exist: prefer newer, then larger, then by name.
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


# ----------------------------- SPIKE lookup -----------------------------


def _find_spike_file(base_dir: Path = SPIKE_DIR) -> Optional[Path]:
    """
    Search top-level of config/spike for *.csv.
    If multiple exist: prefer newer, then larger, then by name.
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


# ---------------------- mcquac.json injection (+ placeholders) ----------------------


def _json_replace_placeholder(obj: Any, placeholder: str, value: str) -> Any:
    """
    Recursively replace strings that exactly match the given placeholder.
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
    Set 'main_fasta_file' and replace optional FASTA placeholders.
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
    # Replace placeholder variants (both, just to be safe)
    for ph in ("%%%FASTA%%%", "%%%FASTA%%%%"):
        data = _json_replace_placeholder(data, ph, str(fasta))

    try:
        mcquac_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


def _inject_spike_file_in_mcquac(mcquac_path: Path) -> None:
    """
    Set 'main_spike_file' and replace optional SPIKE placeholders.
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


# ----------------------------- Main function -----------------------------


def copy_candidates(
    snapshot: List[Dict],
    *,
    folder: Path,
    tmp_dir: Path,
    copied_cache: Set[Tuple[str, int]],
    ignore_file: Path | None = None,  # per-thread file in tmp/
    add_to_ignore: bool = True,
    # Additional metadata for info.json (written per hash)
    info_for_hash: Optional[dict] = None,
) -> tuple[List[Path], int]:
    """
    Copy all items (name,size,hash,count) once into tmp/<hash>/input/.
    Prevent duplicate copies via copied_cache (name,size).

    Additionally:
      - create mcquac.json via write_from_config(...) with
        INPUT=tmp/<hash>/input and OUTPUT=tmp/<hash>/output
      - set 'main_fasta_file' (found in config/fasta) and
        'main_spike_file' (found in config/spike)
      - replace optional placeholders %%%FASTA%%% / %%%FASTA%%%% and
        %%%SPIKE%%% / %%%SPIKE%%%% in mcquac.json
      - write info.json with all paths & parameters for later use (from info_for_hash)
      - create .ready in the hash directory once job & info have been generated

    IMPORTANT:
      - Supports both regular files (e.g. *.raw) and directories
        (e.g. *.d). Directories are copied recursively.
    """
    folder = Path(folder)
    tmp_dir = Path(tmp_dir)

    copied_paths: List[Path] = []
    names_for_ignore: List[str] = []
    jobs_written_for_hash: Set[str] = set()  # jobs/info already created in this call

    for item in snapshot:
        try:
            name = item["name"]
            size = item["size"]
            h = item["hash"]
        except KeyError:
            # snapshot entry does not have the expected shape -> skip
            continue

        key = (name, size)
        if key in copied_cache:
            continue

        src = _resolve_source_path(name, folder)
        if not src.exists():
            # source disappeared -> skip
            continue

        # prepare target directories
        hash_dir, input_dir, output_dir = _ensure_hash_dirs(tmp_dir, h)

        # target path (same name as source, inside input/)
        dst = input_dir / src.name

        try:
            # handle files AND directories
            if src.is_dir():
                # e.g. Bruker folders *.d
                if dst.exists() and dst.is_file():
                    dst.unlink()
                # dirs_exist_ok=True allows retries without errors
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)

            copied_cache.add(key)
            copied_paths.append(dst)
            if add_to_ignore and ignore_file is not None:
                # only the basename is written to ignore.txt
                names_for_ignore.append(src.name)

            # per hash: mcquac.json & info.json & .ready (only once per call)
            if h not in jobs_written_for_hash:
                try:
                    # create mcquac.json
                    write_from_config(
                        input_value=str(input_dir),
                        output_value=str(output_dir),
                        inner_folder=h,  # writes to ./tmp/<hash>/mcquac.json
                        template_filename="mcquac.json",
                        config_dir="config",
                    )

                    mcquac = hash_dir / "mcquac.json"
                    # inject FASTA & SPIKE (and replace placeholders)
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

                    # create .ready file
                    _write_ready_file(hash_dir)

                except Exception:
                    # intentionally silent; add logging/prints if needed
                    pass

                jobs_written_for_hash.add(h)

        except Exception:
            # intentionally silent; add logging/prints if needed
            pass

    added = 0
    if add_to_ignore and ignore_file is not None and names_for_ignore:
        added = _append_to_ignore_filenames(ignore_file, names_for_ignore)

    return copied_paths, added
