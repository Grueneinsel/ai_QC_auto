#!/usr/bin/env python3
from pathlib import Path
from blake3 import blake3
import fnmatch
from typing import Iterable, Union, Dict, List, Optional

def hash_blake3_folder(
    folder: Union[str, Path],
    pattern: Union[str, Iterable[str]] = "*.raw",
    ignore: Optional[Iterable[str]] = None,
    recursive: bool = False,
    buf_size: int = 8 << 20,
    print_output: bool = True,
    show_full_path: bool = False,
) -> Dict[str, str]:
    """
    Berechnet BLAKE3-Hashes für Dateien in 'folder', gefiltert über 'pattern'
    und ignoriert Einträge aus 'ignore'. Gibt ein Dict {name/weg: digest} zurück.
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

    def blake3_file(p: Path) -> str:
        h = blake3()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(buf_size), b""):
                h.update(chunk)
        return h.hexdigest()

    # Hashen & ausgeben
    results: Dict[str, str] = {}
    # stabile Reihenfolge: relativ zum Root sortieren
    for p in sorted(files, key=lambda x: str(x.relative_to(root)).lower()):
        digest = blake3_file(p)
        key = str(p if show_full_path else p.name)
        results[key] = digest
        if print_output:
            print(f"{digest}  {p if show_full_path else p.name}")

    return results


# --- CLI/Beispiel ---
if __name__ == "__main__":
    FOLDER = r"/mnt/c/Users/info/OneDrive/Rub/StudienProject/StudienProject_01_10_2025/MS raw data"

    # Funktionsaufruf: hier kannst du Pattern/Ignore/rekursiv etc. anpassen
    result = hash_blake3_folder(
        folder=FOLDER,
        pattern="*std.raw",                 # z.B. "*std.raw" oder ["*std.raw", "*.raw"]
        ignore=["defekt.std.raw", "*.tmp"], # ignorierte Dateien/Patterns
        recursive=False,
        print_output=True,                  # True: sofortige Ausgabe in der Funktion
        show_full_path=True,               # False: nur Dateinamen
    )

    # Return-Wert NOCHMAL ausgeben (wie gewünscht)
    if result:
        print("\n# Ergebnisse (Return-Wert):")
        for name, digest in result.items():   # Reihenfolge entspricht der Einfüge-Reihenfolge
            print(f"{digest}  {name}")
