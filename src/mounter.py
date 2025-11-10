# src/mounter.py
from __future__ import annotations
import os, pathlib, subprocess, tempfile, socket, shutil, time
from typing import Any, Dict

# --------- Helpers ---------
def _which(name: str) -> str | None:
    return shutil.which(name)

def _need_bins():
    for b in ("mount",):
        if not _which(b):
            raise RuntimeError(f"'{b}' nicht gefunden. Bitte installieren.")
    if not (_which("mount.cifs") or pathlib.Path("/sbin/mount.cifs").exists()):
        raise RuntimeError("mount.cifs fehlt. Bitte 'sudo apt install cifs-utils' ausführen.")

def _is_root() -> bool:
    return os.geteuid() == 0

def _mountpoint_active(path: str) -> bool:
    return subprocess.run(["mountpoint","-q",path]).returncode == 0

def _try_list(path: str) -> bool:
    try:
        next(os.scandir(path), None)
        return True
    except Exception:
        return False

def _check_port(host: str, port: int = 445, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

def _ping(host: str) -> bool:
    try:
        return subprocess.run(["ping","-c","1","-w","2",host],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    except Exception:
        return True  # ping optional

def _get(d: Any, key: str, default: Any = None) -> Any:
    if isinstance(d, dict):
        return d.get(key, default)
    return getattr(d, key, default)

def _build_creds(username: str, password: str, domain: str | None) -> str:
    content = [f"username={username}", f"password={password}"]
    if domain:
        content.append(f"domain={domain}")
    tmp = tempfile.NamedTemporaryFile("w", delete=False)
    tmp.write("\n".join(content) + "\n")
    tmp.close()
    os.chmod(tmp.name, 0o600)
    return tmp.name

# --------- Kern: Einzelnes SMB-Mount (Passwort nur aus app.json) ---------
def ensure_smb_mount(entry: Any, *, non_interactive: bool = True) -> str:
    """
    entry: Dict/Objekt mit Feldern:
      name, host, share, mountpoint, username, password,
      domain(optional), vers(optional), file_mode/dir_mode(optional), extra_opts(optional)
    """
    host        = _get(entry, "host")
    share       = _get(entry, "share")
    mountpoint  = pathlib.Path(str(_get(entry, "mountpoint")))
    username    = _get(entry, "username")
    password    = _get(entry, "password")          # <-- Pflichtfeld in app.json
    domain      = _get(entry, "domain")
    vers        = _get(entry, "vers")
    file_mode   = str(_get(entry, "file_mode", "0664"))
    dir_mode    = str(_get(entry, "dir_mode",  "0775"))
    extra_opts  = list(_get(entry, "extra_opts", []) or [])

    if not all([host, share, mountpoint, username, password]):
        raise ValueError("SMB-Eintrag unvollständig: host/share/mountpoint/username/password erforderlich.")

    mountpoint.mkdir(parents=True, exist_ok=True)

    # Netzwerk erreichbar?
    if not _ping(host) or not _check_port(host, 445):
        raise RuntimeError(f"Host {host} nicht erreichbar (Ping/Port 445).")

    # Bereits gemountet?
    if _mountpoint_active(str(mountpoint)):
        if _try_list(str(mountpoint)):
            return str(mountpoint)
        subprocess.run(["umount","-f",str(mountpoint)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    uid, gid = os.getuid(), os.getgid()
    base_opts = [
        f"uid={uid}", f"gid={gid}", "iocharset=utf8",
        f"file_mode={file_mode}", f"dir_mode={dir_mode}",
    ] + extra_opts

    vers_candidates = [vers] if vers else ["3.1.1", "3.0", "2.1"]

    creds_path = _build_creds(username, password, domain)
    try:
        last_err = None
        for v in vers_candidates:
            opts = base_opts + [f"credentials={creds_path}", f"vers={v}"]
            cmd  = ["mount","-t","cifs", f"//{host}/{share}", str(mountpoint), "-o", ",".join(opts)]
            try:
                subprocess.run(cmd, check=True)
                time.sleep(0.2)
                if not _try_list(str(mountpoint)):
                    raise RuntimeError("Mount ok, aber Verzeichnislistung fehlgeschlagen.")
                print(f"[mount] //{host}/{share} -> {mountpoint} (SMB {v})")
                return str(mountpoint)
            except subprocess.CalledProcessError as e:
                last_err = e
            except Exception as e:
                last_err = e
        raise last_err or RuntimeError("Mount fehlgeschlagen.")
    finally:
        try: os.remove(creds_path)
        except FileNotFoundError: pass

# --------- Public API ---------
def ensure_mounts_from_cfg(cfg: Any, *, best_effort: bool = False, non_interactive: bool = True) -> Dict[str, str]:
    """
    Liest 'mounts' aus cfg und mountet alle. Passwort MUSS in jedem Eintrag stehen.
    Rückgabe: {name: "OK" | "FAIL: <msg>"}
    """
    _need_bins()
    if not _is_root():
        raise PermissionError("Bitte als root / via sudo starten (mount erfordert Root-Rechte).")

    mounts = _get(cfg, "mounts", None)
    if not mounts:
        return {}

    result: Dict[str, str] = {}
    for entry in mounts:
        name = _get(entry, "name", f"share@{_get(entry,'host','?')}")
        try:
            ensure_smb_mount(entry, non_interactive=non_interactive)
            result[name] = "OK"
        except Exception as e:
            msg = f"FAIL: {e}"
            result[name] = msg
            if not best_effort:
                raise RuntimeError(f"Mount '{name}' fehlgeschlagen: {e}") from e
            print(f"[warn] {msg}")
    return result

def unmount_all_from_cfg(cfg: Any) -> None:
    mounts = _get(cfg, "mounts", None) or []
    for entry in mounts:
        mp = str(_get(entry, "mountpoint"))
        if _mountpoint_active(mp):
            subprocess.run(["umount", mp], check=False)
            print(f"[umount] {mp}")
