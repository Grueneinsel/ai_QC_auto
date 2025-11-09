#!/usr/bin/env python3
from pathlib import Path
import os, shutil, stat

def nuke_tmp(base_dir: str | Path = ".") -> Path:
    """
    Löscht ./tmp zuverlässig (auch bei Read-only/Windows-Locks) und legt ihn leer neu an.
    """
    tmp = (Path(base_dir) / "tmp").resolve()

    # Falls wir gerade IN tmp arbeiten, vorher rauswechseln
    try:
        cwd = Path.cwd().resolve()
        if str(cwd).startswith(str(tmp)):
            os.chdir(tmp.parent)
    except Exception:
        pass

    def _onerror(func, path, exc_info):
        # Schreibschutz entfernen und erneut versuchen
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
            func(path)
        except Exception as e:
            print(f"[WARN] Could not remove {path}: {e}")

    if tmp.exists():
        shutil.rmtree(tmp, onerror=_onerror)  # keine ignore_errors: Fehler sichtbar machen
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

if __name__ == "__main__":
    path = nuke_tmp()
    print(f"Bereinigt: {path}")
