#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Any
import json, os

PROJECT_ROOT = Path(__file__).resolve().parent

@dataclass
class IOPair:
    input: Path
    output: Path
    pattern: str

@dataclass
class AppConfig:
    interval_minutes: int
    interval_seconds: int
    mcquac_path: Path
    default_pattern: str
    io_pairs: List[IOPair]

def _expand(p: str, base: Path) -> Path:
    s = os.path.expandvars(os.path.expanduser(str(p)))
    pp = Path(s)
    return (base / pp).resolve() if not pp.is_absolute() else pp.resolve()

def load_config(cfg_path: Path = PROJECT_ROOT / "config" / "app.json") -> AppConfig:
    if not cfg_path.is_file():
        raise FileNotFoundError(f"Konfigurationsdatei nicht gefunden: {cfg_path}")

    raw: Any = json.loads(cfg_path.read_text(encoding="utf-8"))

    # Pflichtfelder prüfen
    required = ("interval_minutes", "mcquac_path", "default_pattern", "io_pairs")
    missing = [k for k in required if k not in raw]
    if missing:
        raise ValueError(f"Fehlende Felder in config: {', '.join(missing)}")

    try:
        interval_minutes = int(raw["interval_minutes"])
        if interval_minutes < 0:
            raise ValueError
    except Exception:
        raise ValueError("'interval_minutes' muss eine nichtnegative ganze Zahl sein.")

    default_pattern = str(raw["default_pattern"]).strip()
    if not default_pattern:
        raise ValueError("'default_pattern' darf nicht leer sein.")

    mcquac_path = _expand(raw["mcquac_path"], PROJECT_ROOT)

    pairs_field = raw["io_pairs"]
    if not isinstance(pairs_field, list) or not pairs_field:
        raise ValueError("'io_pairs' muss eine nichtleere Liste sein.")

    io_pairs: List[IOPair] = []
    for i, item in enumerate(pairs_field, start=1):
        if not isinstance(item, dict) or "input" not in item or "output" not in item:
            raise ValueError(f"Eintrag {i} in 'io_pairs' muss {{'input':..., 'output':...}} enthalten.")
        in_p  = _expand(item["input"], PROJECT_ROOT)
        out_p = _expand(item["output"], PROJECT_ROOT)
        pat   = str(item.get("pattern", default_pattern) or default_pattern)
        io_pairs.append(IOPair(input=in_p, output=out_p, pattern=pat))

    return AppConfig(
        interval_minutes=interval_minutes,
        interval_seconds=interval_minutes * 60,
        mcquac_path=mcquac_path,
        default_pattern=default_pattern,
        io_pairs=io_pairs,
    )

if __name__ == "__main__":
    cfg = load_config()
    print(f"Interval: {cfg.interval_minutes} min ({cfg.interval_seconds} s)")
    print(f"mcquac :  {cfg.mcquac_path}")
    print(f"default:  {cfg.default_pattern}")
    print("Pairs:")
    
    
    for pair in cfg.io_pairs:
        print(f"  IN : {pair.input}\n  OUT: {pair.output}\n  PAT: {pair.pattern}\n")

    # Beispiel, wie du das Pattern je Pair nutzt:
    # for pair in cfg.io_pairs:
    #     for path in pair.input.glob(pair.pattern):
    #         print("Verarbeite:", path)
    #     # time.sleep(cfg.interval_seconds)
