#!/usr/bin/env python3
from pathlib import Path
import fnmatch
from typing import Iterable, Union, Dict, List, Optional

def file_sizes_folder(
    folder: Union[str, Path],
    pattern: Union[str, Iterable[str]] = "*.raw",
    ignore: Optional[Iterable[str]] = None,
    recursive: bool = False,
    print_output: bool = True,
    show_full_path: bool = False,
) -> Dict[str, int]:
    """
    Listet Dateigrößen (Bytes) für Dateien in 'folder', gefiltert über 'pattern'
    und ignoriert Einträge aus 'ignore'. Gibt ein Dict {name/pfad: bytes} zurück.
    """
    root = Path(folder).expanduser()
    if not root.is_dir():
        raise FileNotFoundError(f"Ordner nicht gefunden: {root}")

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

    # Dateien sammeln
    files: set[Path] = set()
    for pat in patterns:
        it = root.rglob(pat) if recursive else root.glob(pat)
        for x in it:
            if x.is_file() and not is_ignored(x):
                files.add(x)

    if not files:
        if print_output:
            print("Keine passenden Dateien gefunden.")
        return {}

    # Größen ermitteln & ausgeben
    results: Dict[str, int] = {}
    for p in sorted(files, key=lambda x: str(x.relative_to(root)).lower()):
        size = p.stat().st_size  # Bytes
        key = str(p if show_full_path else p.name)
        results[key] = size
        if print_output:
            print(f"{size}  {p if show_full_path else p.name}")

    return results


# --- CLI/Beispiel ---
if __name__ == "__main__":
    FOLDER = r"/mnt/c/Users/info/OneDrive/Rub/StudienProject/StudienProject_01_10_2025/MS raw data"

    result = file_sizes_folder(
        folder=FOLDER,
        pattern="*std.raw",                 # z.B. "*std.raw" oder ["*std.raw", "*.raw"]
        ignore=["defekt.std.raw", "*.tmp","EXII12567std.raw"], # ignorierte Dateien/Patterns
        recursive=False,
        print_output=True,                  # True: sofortige Ausgabe in der Funktion
        show_full_path=True,                # False: nur Dateinamen
    )

    # Return-Wert NOCHMAL ausgeben
    if result:
        print("\n# Ergebnisse (Return-Wert):")
        for name, size in result.items():
            print(f"{size}  {name}")
