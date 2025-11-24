#!/usr/bin/env python3
from pathlib import Path
import fnmatch
from typing import Iterable, Union, Dict, List, Optional


def _dir_total_size(p: Path) -> int:
    """
    Berechnet die Gesamtgröße eines Verzeichnisses (rekursiv), indem die Größen
    aller enthaltenen Dateien aufsummiert werden.
    """
    total = 0
    try:
        for child in p.rglob("*"):
            try:
                if child.is_file():
                    total += child.stat().st_size
            except Exception:
                # Einzelne Dateien dürfen fehlschlagen, ohne alles zu blockieren
                continue
    except Exception:
        # Falls das rglob selbst scheitert (z. B. Berechtigungen), geben wir 0 zurück
        return 0
    return total


def file_sizes_folder(
    folder: Union[str, Path],
    pattern: Union[str, Iterable[str]] = "*.raw",
    ignore: Optional[Iterable[str]] = None,
    recursive: bool = False,
    print_output: bool = True,
    show_full_path: bool = False,
) -> Dict[str, int]:
    """
    Listet Größen (Bytes) für Einträge in 'folder', gefiltert über 'pattern'
    und ignoriert Einträge aus 'ignore'. Unterstützt:

      - normale Dateien (z. B. *.raw)
      - Verzeichnisse mit Endung '.d' (z. B. *.d), die als GANZE Einheit
        betrachtet werden (Größe = Summe aller enthaltenen Dateien).

    Rückgabe: Dict {name/pfad: bytes}
    """
    root = Path(folder).expanduser()
    if not root.is_dir():
        raise FileNotFoundError(f"Ordner nicht gefunden: {root}")

    # pattern kann String oder Iterable von Strings sein
    patterns: List[str] = [pattern] if isinstance(pattern, str) else list(pattern)
    ignore_list: List[str] = list(ignore or [])

    def is_ignored(p: Path) -> bool:
        rel = str(p.relative_to(root))
        name = p.name
        abs_ = str(p)
        for ig in ignore_list:
            if fnmatch.fnmatch(rel, ig) or fnmatch.fnmatch(name, ig) or fnmatch.fnmatch(abs_, ig):
                return True
        return False

    # Dateien und .d-Verzeichnisse sammeln
    entries: set[Path] = set()
    for pat in patterns:
        it = root.rglob(pat) if recursive else root.glob(pat)
        for x in it:
            if is_ignored(x):
                continue
            # Normale Dateien immer berücksichtigen
            if x.is_file():
                entries.add(x)
            # Zusätzlich: Verzeichnisse mit Endung ".d" als eigene Einheit überwachen
            elif x.is_dir() and x.name.lower().endswith(".d"):
                entries.add(x)

    if not entries:
        if print_output:
            print("Keine passenden Dateien/Verzeichnisse gefunden.")
        return {}

    # Größen ermitteln & ausgeben
    results: Dict[str, int] = {}
    for p in sorted(entries, key=lambda x: str(x.relative_to(root)).lower()):
        if p.is_dir():
            size = _dir_total_size(p)
        else:
            try:
                size = p.stat().st_size  # Bytes
            except Exception:
                # Falls stat fehlschlägt, Eintrag überspringen
                continue

        key = str(p if show_full_path else p.name)
        results[key] = size

        if print_output:
            print(f"{size}  {p if show_full_path else p.name}")

    return results

