#!/usr/bin/env python3
from pathlib import Path
import os, shutil, stat


def nuke_tmp(base_dir: str | Path = ".") -> Path:
    """
    Reliably deletes ./tmp (even with read-only/Windows locks) and recreates it empty.
    """
    tmp = (Path(base_dir) / "tmp").resolve()

    # If we are currently working *inside* tmp, change to its parent first
    try:
        cwd = Path.cwd().resolve()
        if str(cwd).startswith(str(tmp)):
            os.chdir(tmp.parent)
    except Exception:
        pass

    def _onerror(func, path, exc_info):
        # Remove write protection and retry
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
            func(path)
        except Exception as e:
            print(f"[WARN] Could not remove {path}: {e}")

    if tmp.exists():
        # no ignore_errors: surface errors via _onerror
        shutil.rmtree(tmp, onerror=_onerror)
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp


if __name__ == "__main__":
    path = nuke_tmp()
    print(f"Cleaned: {path}")
