#!/usr/bin/env python3
from pathlib import Path
import fnmatch
from typing import Iterable, Union, Dict, List, Optional


def _dir_total_size(p: Path) -> int:
    """
    Compute the total size of a directory (recursively) by summing the sizes
    of all contained files.
    """
    total = 0
    try:
        for child in p.rglob("*"):
            try:
                if child.is_file():
                    total += child.stat().st_size
            except Exception:
                # Individual files may fail without blocking everything
                continue
    except Exception:
        # If rglob itself fails (e.g. permissions), return 0
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
    List sizes (bytes) for entries in `folder`, filtered via `pattern`
    and ignoring entries from `ignore`. Supports:

      - regular files (e.g. *.raw)
      - directories ending with '.d' (e.g. *.d), which are treated as a
        single unit (size = sum of all contained files).

    Return value: Dict {name/path: bytes}
    """
    root = Path(folder).expanduser()
    if not root.is_dir():
        raise FileNotFoundError(f"Folder not found: {root}")

    # pattern can be a string or an iterable of strings
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

    # Collect files and .d directories
    entries: set[Path] = set()
    for pat in patterns:
        it = root.rglob(pat) if recursive else root.glob(pat)
        for x in it:
            if is_ignored(x):
                continue
            # Always consider regular files
            if x.is_file():
                entries.add(x)
            # Additionally: treat directories ending with ".d" as single units
            elif x.is_dir() and x.name.lower().endswith(".d"):
                entries.add(x)

    if not entries:
        if print_output:
            print("No matching files/directories found.")
        return {}

    # Compute sizes & optionally print
    results: Dict[str, int] = {}
    for p in sorted(entries, key=lambda x: str(x.relative_to(root)).lower()):
        if p.is_dir():
            size = _dir_total_size(p)
        else:
            try:
                size = p.stat().st_size  # bytes
            except Exception:
                # If stat fails, skip this entry
                continue

        key = str(p if show_full_path else p.name)
        results[key] = size

        if print_output:
            print(f"{size}  {p if show_full_path else p.name}")

    return results
