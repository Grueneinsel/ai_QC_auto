#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Any, Optional
import json, os

"""
Configuration loader
--------------------

Reads `config/app.json`, validates its contents and returns an `AppConfig`.
Supports optional fields such as `mounts` and `nextflow_bin`.

Important fields in app.json:
- interval_minutes (int >= 0)
- mcquac_path (path to main.nf)
- default_pattern (glob)
- io_pairs: list of {input, output, pattern?}
- mounts: optional
- continue_on_mount_error: optional (bool)
- unmount_on_exit: optional (bool)
- nextflow_bin: optional (path to the Nextflow binary)
"""

# Project root: one directory above /src
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class IOPair:
    input: Path
    output: Path
    pattern: str


@dataclass
class MountEntry:
    name: str
    host: str
    share: str
    mountpoint: Path
    username: str
    password: str
    domain: str | None = None
    vers: str | None = None
    file_mode: str = "0664"
    dir_mode: str = "0775"
    extra_opts: List[str] = field(default_factory=list)


@dataclass
class AppConfig:
    interval_minutes: int
    interval_seconds: int
    mcquac_path: Path
    default_pattern: str
    io_pairs: List[IOPair]
    mounts: List[MountEntry] = field(default_factory=list)
    continue_on_mount_error: bool = False
    unmount_on_exit: bool = False
    nextflow_bin: Optional[Path] = None  # <- NEW: optional path to the Nextflow binary


def _expand(p: str, base: Path) -> Path:
    """Expand environment variables and ~; resolve relative paths relative to `base`."""
    s = os.path.expandvars(os.path.expanduser(str(p)))
    pp = Path(s)
    return (base / pp).resolve() if not pp.is_absolute() else pp.resolve()


def _read_io_pairs(pairs_field: Any, default_pattern: str) -> List[IOPair]:
    if not isinstance(pairs_field, list) or not pairs_field:
        raise ValueError("'io_pairs' must be a non-empty list.")

    io_pairs: List[IOPair] = []
    for i, item in enumerate(pairs_field, start=1):
        if not isinstance(item, dict) or "input" not in item or "output" not in item:
            raise ValueError(f"Entry {i} in 'io_pairs' must contain {{'input': ..., 'output': ...}}.")
        in_p = _expand(item["input"], PROJECT_ROOT)
        out_p = _expand(item["output"], PROJECT_ROOT)
        pat = str(item.get("pattern", default_pattern) or default_pattern)
        io_pairs.append(IOPair(input=in_p, output=out_p, pattern=pat))
    return io_pairs


def _read_mounts(mounts_field: Any) -> List[MountEntry]:
    if mounts_field is None:
        return []
    if not isinstance(mounts_field, list):
        raise ValueError("'mounts' must be a list.")

    mounts: List[MountEntry] = []
    for i, item in enumerate(mounts_field, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Mount entry {i}: expected an object.")

        host = str(item.get("host", "")).strip()
        share = str(item.get("share", "")).strip()
        mp_raw = item.get("mountpoint", "")
        username = str(item.get("username", "")).strip()
        password = str(item.get("password", "")).strip()

        if not (host and share and mp_raw and username and password):
            raise ValueError(
                f"Mount entry {i}: 'host', 'share', 'mountpoint', 'username', 'password' are required."
            )

        mountpoint = _expand(mp_raw, PROJECT_ROOT)
        name = str(item.get("name") or f"{share}@{host}")
        domain = (str(item["domain"]).strip() or None) if "domain" in item and item["domain"] is not None else None
        vers_raw = item.get("vers", None)
        vers = (str(vers_raw).strip() or None) if vers_raw is not None else None
        file_mode = str(item.get("file_mode", "0664"))
        dir_mode = str(item.get("dir_mode", "0775"))

        extra_raw = item.get("extra_opts", [])
        if isinstance(extra_raw, (list, tuple)):
            extra_opts = [str(x) for x in extra_raw]
        elif isinstance(extra_raw, str) and extra_raw.strip():
            extra_opts = [extra_raw.strip()]
        else:
            extra_opts = []

        mounts.append(MountEntry(
            name=name,
            host=host,
            share=share,
            mountpoint=mountpoint,
            username=username,
            password=password,
            domain=domain,
            vers=vers,
            file_mode=file_mode,
            dir_mode=dir_mode,
            extra_opts=extra_opts,
        ))
    return mounts


def load_config(cfg_path: Path | None = None) -> AppConfig:
    """
    Load `config/app.json` and return a validated `AppConfig`
    (including `mounts` and `nextflow_bin`).
    """
    cfg_path = cfg_path or (PROJECT_ROOT / "config" / "app.json")
    if not cfg_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {cfg_path}")

    raw: Any = json.loads(cfg_path.read_text(encoding="utf-8"))

    # Check required fields (mounts/nextflow_bin are optional)
    required = ("interval_minutes", "mcquac_path", "default_pattern", "io_pairs")
    missing = [k for k in required if k not in raw]
    if missing:
        raise ValueError(f"Missing fields in config: {', '.join(missing)}")

    # interval_minutes
    try:
        interval_minutes = int(raw["interval_minutes"])
        if interval_minutes < 0:
            raise ValueError
    except Exception:
        raise ValueError("'interval_minutes' must be a non-negative integer.")

    # default_pattern
    default_pattern = str(raw["default_pattern"]).strip()
    if not default_pattern:
        raise ValueError("'default_pattern' must not be empty.")

    # mcquac_path
    mcquac_path = _expand(raw["mcquac_path"], PROJECT_ROOT)

    # optional: nextflow_bin
    nextflow_bin_raw = raw.get("nextflow_bin")
    nextflow_bin: Optional[Path] = None
    if nextflow_bin_raw:
        nextflow_bin = _expand(str(nextflow_bin_raw), PROJECT_ROOT)

    # io_pairs & mounts
    io_pairs = _read_io_pairs(raw["io_pairs"], default_pattern)
    mounts = _read_mounts(raw.get("mounts"))

    continue_on_mount_error = bool(raw.get("continue_on_mount_error", False))
    unmount_on_exit = bool(raw.get("unmount_on_exit", False))

    return AppConfig(
        interval_minutes=interval_minutes,
        interval_seconds=interval_minutes * 60,
        mcquac_path=mcquac_path,
        default_pattern=default_pattern,
        io_pairs=io_pairs,
        mounts=mounts,
        continue_on_mount_error=continue_on_mount_error,
        unmount_on_exit=unmount_on_exit,
        nextflow_bin=nextflow_bin,
    )
