#!/usr/bin/env python3
from pathlib import Path
from blake3 import blake3

# >>> FESTER ORDNER <<<
FOLDER = r"/mnt/c/Users/info/OneDrive/Rub/StudienProject/StudienProject_01_10_2025/MS raw data"
RECURSIVE = False          # True = Unterordner mit durchsuchen
BUF_SIZE = 8 << 20         # 8 MiB für schnelleres Streaming



def blake3_file(p: Path) -> str:
    h = blake3()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(BUF_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()   # 32 Byte = 64 Hex-Zeichen

def main():
    folder = Path(FOLDER).expanduser()
    if not folder.is_dir():
        raise SystemExit(f"Ordner nicht gefunden: {folder}")

    files = folder.rglob("*.raw") if RECURSIVE else folder.glob("*.raw")

    found = False
    for p in sorted(files):
        if p.is_file() and p.suffix.lower() == ".raw":
            print(f"{blake3_file(p)}  {p.name}")  # {p} für vollständigen Pfad
            found = True
    if not found:
        print("Keine .raw-Dateien gefunden.")

if __name__ == "__main__":
    main()
