#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import sys, re, signal, time, queue
from typing import Callable, Any

# --- Project root & import safety ---
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clear import nuke_tmp
from src.load_config import load_config
from src.search import start_watch_thread            # supports extra_ignore_file + pre_scan_hook
from src.copier import copy_candidates               # creates mcquac.json + info.json + .ready
from src.mounter import (
    ensure_mounts_from_cfg,
    unmount_all_from_cfg,
    ensure_smb_mount,                                # <- used by per-watcher guard
)
from src.mcquac_runner import start_runner_thread    # executes .ready jobs via Nextflow

TMP_DIR = ROOT / "tmp"


def slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(s).strip()) or "x"


def graceful_stop(watchers: list[dict]) -> None:
    print("\nStopping watchers …")
    for w in watchers:
        try:
            w["stop"].set()
            w["thread"].join(timeout=2)
        except Exception:
            pass
    print("Done.")


def _get(obj: Any, name: str, default=None):
    # cfg may be a dict or an object
    try:
        return getattr(obj, name)
    except Exception:
        try:
            return obj.get(name, default)  # type: ignore
        except Exception:
            return default


def _drain_status(status_q: "queue.Queue[str]") -> None:
    if not status_q:
        return
    while True:
        try:
            msg = status_q.get_nowait()
        except queue.Empty:
            break
        else:
            print(msg)


def _is_subpath(child: Path, parent: Path) -> bool:
    try:
        child = Path(child).resolve()
        parent = Path(parent).resolve()
        _ = child.relative_to(parent)
        return True
    except Exception:
        return False


def _make_mount_guard(folder_in: Path, cfg: Any) -> Callable[[], None]:
    """
    Return a function that is executed before EVERY scan and, if necessary,
    repairs the relevant SMB mount. Idempotent thanks to ensure_smb_mount().
    """
    mounts = _get(cfg, "mounts", []) or []

    relevant = []
    for entry in mounts:
        mp = _get(entry, "mountpoint")
        if not mp:
            continue
        try:
            if _is_subpath(Path(folder_in), Path(mp)):
                relevant.append(entry)
        except Exception:
            continue

    def guard() -> None:
        for e in relevant:
            try:
                # returns immediately if the mount is active and listable
                ensure_smb_mount(e, non_interactive=True)
            except Exception as ex:
                name = _get(e, "name", f"{_get(e,'share','?')}@{_get(e,'host','?')}")
                print(f"[WARN] Mount check/repair '{name}' failed: {ex}")

    return guard


def main() -> int:
    # clear tmp (comment out if you want to keep it)
    cleaned = nuke_tmp(ROOT)
    print(f"Cleaned tmp dir: {cleaned}\n")

    # Load config (expects interval_seconds in config/app.json)
    cfg = load_config()
    print("Starting watcher based on config/app.json …")
    print(f"  Interval:        {_get(cfg, 'interval_seconds')} s")
    print(f"  default_pattern: {_get(cfg, 'default_pattern')}")
    print(f"  mcquac_path:     {_get(cfg, 'mcquac_path')}\n")

    # --- Initialize system-wide mounts once (optional) --------------------- #
    mounts = _get(cfg, "mounts", None) or []
    continue_on_mount_error = bool(_get(cfg, "continue_on_mount_error", False))
    if mounts:
        try:
            statuses = ensure_mounts_from_cfg(
                cfg,
                best_effort=continue_on_mount_error,
                non_interactive=True,
            )
            print("Mount status:")
            for name, st in statuses.items():
                print(f"  - {name}: {st}")
            print()
        except PermissionError as e:
            msg = f"[FATAL] {e}\nHint: run as root (sudo) and install 'cifs-utils'."
            if continue_on_mount_error:
                print("[WARN] Mount error, continuing anyway:", msg)
            else:
                print(msg)
                return 2
        except Exception as e:
            if continue_on_mount_error:
                print(f"[WARN] Mount failed, continuing anyway: {e}")
            else:
                print(f"[FATAL] Mount failed: {e}")
                return 2
    else:
        print("No mounts defined in config/app.json – skipping mount step.\n")
    # ----------------------------------------------------------------------- #

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Start one watcher thread per IO pair
    watchers: list[dict] = []
    io_pairs = _get(cfg, "io_pairs", []) or []
    if not io_pairs:
        print("[WARN] No io_pairs in config. No watchers will be started.\n")

    for i, pair in enumerate(io_pairs, start=1):
        # pair may be either a dict or an object
        folder_in = _get(pair, "input")
        final_out = _get(pair, "output")
        pattern = _get(pair, "pattern") or _get(cfg, "default_pattern")
        use_full = True
        recursive = False
        interval_s = _get(cfg, "interval_seconds")

        # thread-specific ignore file in tmp/
        ign_thread = TMP_DIR / f"ignore-{slug(folder_in)}-{slug(pattern)}.txt"
        ign_thread.touch(exist_ok=True)

        # second ignore file: ignore.txt in this pair's output folder
        Path(final_out).mkdir(parents=True, exist_ok=True)      # make sure the output directory exists
        ign_output = Path(final_out) / "ignore.txt"
        ign_output.touch(exist_ok=True)

        # Mount guard for this watcher (no-op if input is not under any mountpoint)
        pre_hook = _make_mount_guard(Path(folder_in), cfg)

        print(f"[Watcher {i}]")
        print(f"  IN : {folder_in}")
        print(f"  OUT: {final_out}")
        print(f"  PAT: {pattern}")
        print(f"  IGN: {ign_thread.name} + {ign_output}")       # path of the output ignore.txt
        print()

        t, q, stop_evt = start_watch_thread(
            folder=folder_in,
            pattern=pattern,
            recursive=recursive,
            interval_seconds=interval_s,   # **seconds**
            use_full_path=use_full,
            ignore_file=ign_thread,        # thread-specific ignore file (in tmp/)
            extra_ignore_file=ign_output,  # ignore file in the pair's output folder
            pre_scan_hook=pre_hook,        # <- NEW: check/repair mount before each scan
        )
        # If start_watch_thread did not start the thread itself:
        try:
            if hasattr(t, "is_alive") and not t.is_alive():
                t.start()
        except RuntimeError:
            # might already have been started
            pass

        watchers.append({
            "idx": i,
            "thread": t,
            "queue": q,
            "stop": stop_evt,
            "in_root": Path(folder_in),
            "final_out": Path(final_out),
            "pattern": pattern,
            "interval_s": interval_s,
            "ign_thread": ign_thread,
            "ign_output": ign_output,
        })

    # Start MCQuaC runner (watches tmp/ for .ready and runs Nextflow)
    runner = start_runner_thread(TMP_DIR, cfg, max_parallel=1, poll_interval=1.0)

    # Global cache to avoid duplicate copies across all threads
    copied_cache: set[tuple[str, int]] = set()

    # Graceful shutdown on SIGINT/SIGTERM
    stop_now = {"flag": False}

    def _sig_handler(signum, frame):
        stop_now["flag"] = True

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    try:
        # Main loop: robust long-running event processing
        while not stop_now["flag"]:
            if not watchers:
                # No watchers configured -> still drain runner status
                _drain_status(runner["status"])  # print status messages from the runner
                time.sleep(0.5)
                continue

            any_processed = False

            # Round-robin: poll all queues briefly
            for w in watchers:
                q = w["queue"]
                try:
                    # Short wait per watcher to keep CPU usage low
                    kind, ts, snapshot = q.get(timeout=0.5)
                except Exception:
                    continue

                any_processed = True
                print("-" * 60)
                print(f"[{ts}] [Watcher {w['idx']}] candidates (stable for ≥2 scans): {len(snapshot)}")
                if not snapshot:
                    print("  (no matches)")
                    continue

                for item in snapshot:
                    print(f"  {item['count']}×  size={item['size']}  hash={item['hash']}  name={item['name']}")

                # Metadata that is written into info.json per hash
                info_for_hash = {
                    "input_root": w["in_root"],
                    "final_output_root": w["final_out"],  # prepared for later copying
                    "pattern": w["pattern"],
                    "interval_seconds": w["interval_s"],
                    "ignore_files": [w["ign_thread"], w["ign_output"]],
                }

                new_paths, added_ign = copy_candidates(
                    snapshot,
                    folder=w["in_root"],
                    tmp_dir=TMP_DIR,
                    copied_cache=copied_cache,
                    ignore_file=w["ign_thread"],   # only file names are appended (into tmp-ignore)
                    add_to_ignore=True,
                    info_for_hash=info_for_hash,   # -> src/copier.py creates info.json & .ready
                )
                for p in new_paths:
                    print(f"    -> COPIED to {p}")
                if added_ign:
                    print(f"    -> {added_ign} name(s) added to {w['ign_thread'].name}")

            # Drain runner status queue
            _drain_status(runner["status"])

            # If no queue items arrived, briefly idle -> CPU friendly while staying "always on"
            if not any_processed:
                time.sleep(0.1)

    finally:
        # Stop runner cleanly
        try:
            runner["stop"].set()
            runner["thread"].join(timeout=5)
        except Exception:
            pass

        graceful_stop(watchers)
        # Optional unmount (only if enabled in config)
        try:
            if bool(_get(cfg, "unmount_on_exit", False)):
                unmount_all_from_cfg(cfg)
        except Exception as e:
            print(f"[WARN] Unmount on shutdown failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
