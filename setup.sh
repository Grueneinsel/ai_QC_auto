#!/usr/bin/env bash
set -Eeuo pipefail

# --- re-exec in bash, falls mit sh aufgerufen ---
if [ -z "${BASH_VERSION:-}" ]; then exec /usr/bin/env bash "$0" "$@"; fi

bold(){ printf "\033[1m%s\033[0m\n" "$*"; }
info(){ printf "  • %s\n" "$*"; }
warn(){ printf "\033[33m[WARN]\033[0m %s\n" "$*"; }
err() { printf "\033[31m[ERR]\033[0m  %s\n" "$*"; }
die() { err "$*"; exit 1; }

# --- Root/Sudo ermitteln (ohne $EUID -> portable) ---
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    die "Bitte als root ausführen oder 'sudo' installieren."
  fi
else
  SUDO=""
fi

# --- Projekt-Root bestimmen (Script liegt z. B. in PROJECT_ROOT/scripts) ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

bold "MCQuaC / ai_QC_auto Setup"
info "PROJECT_ROOT = $PROJECT_ROOT"

# -------- Helpers --------
have(){ command -v "$1" >/dev/null 2>&1; }
apt_pkg_exists(){ apt-cache show "$1" >/dev/null 2>&1; }
apt_install_filtered(){
  $SUDO apt-get update -y
  local to_install=() skipped=()
  for pkg in "$@"; do
    if apt_pkg_exists "$pkg"; then to_install+=("$pkg"); else skipped+=("$pkg"); fi
  done
  if ((${#skipped[@]})); then warn "Überspringe nicht verfügbare Pakete: ${skipped[*]}"; fi
  if ((${#to_install[@]})); then
    $SUDO apt-get install -y --no-install-recommends "${to_install[@]}" || {
      warn "apt install fehlgeschlagen, erneuter Versuch…"
      $SUDO apt-get install -y --no-install-recommends "${to_install[@]}"
    }
  fi
}

download(){
  # download URL OUTFILE
  local url="$1" out="$2"
  if have curl;  then curl -fsSL "$url" -o "$out"; return $?; fi
  if have wget;  then wget -q    "$url" -O "$out"; return $?; fi
  return 2
}

# -------- 1) Systempakete --------
bold "1) System-Pakete installieren"
apt_install_filtered ca-certificates curl wget gnupg lsb-release unzip zip jq git \
                     python3-venv python3-pip cifs-utils apt-transport-https

# Java (21 -> 17 -> 11)
if ! have java; then apt_install_filtered openjdk-21-jre-headless || true; fi
if ! have java; then apt_install_filtered openjdk-17-jre-headless || true; fi
if ! have java; then apt_install_filtered openjdk-11-jre-headless || true; fi
have java || warn "Java nicht gefunden – Nextflow benötigt Java ≥ 11."

# -------- 2) WSL systemd (optional) --------
if grep -qi microsoft /proc/version 2>/dev/null; then
  bold "WSL erkannt – systemd aktivieren/prüfen"
  WSL_CONF="/etc/wsl.conf"
  TMP_WSL="$(mktemp)"
  if [[ -f "$WSL_CONF" ]]; then
    cat "$WSL_CONF" > "$TMP_WSL"
    grep -q "^\[boot\]" "$TMP_WSL" || printf "\n[boot]\n" >> "$TMP_WSL"
    if grep -q "^systemd\s*=" "$TMP_WSL"; then
      $SUDO sed -i 's/^systemd\s*=.*/systemd=true/g' "$TMP_WSL"
    else
      printf "systemd=true\n" >> "$TMP_WSL"
    fi
  else
    printf "[boot]\nsystemd=true\n" > "$TMP_WSL"
  fi
  $SUDO cp "$TMP_WSL" "$WSL_CONF"; rm -f "$TMP_WSL"
  info "Hinweis: ggf. 'wsl.exe --shutdown' und Terminal neu öffnen."
fi

# -------- 3) Docker Engine + Compose --------
bold "2) Docker Engine + Compose installieren"
if ! have docker; then
  $SUDO install -m 0755 -d /etc/apt/keyrings
  if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | $SUDO gpg --dearmor -o /etc/apt/keyrings/docker.gpg || true
    $SUDO chmod a+r /etc/apt/keyrings/docker.gpg || true
  fi
  . /etc/os-release
  ARCH="$(dpkg --print-architecture)"
  case "$ID" in
    ubuntu) REPO="deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" ;;
    debian) REPO="deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian ${VERSION_CODENAME} stable" ;;
    *)      REPO="deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian ${VERSION_CODENAME} stable" ;;
  esac
  echo "$REPO" | $SUDO tee /etc/apt/sources.list.d/docker.list >/dev/null
  apt_install_filtered docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
else
  info "Docker bereits vorhanden."
fi

# Docker starten & Gruppe setzen
if have systemctl; then
  $SUDO systemctl enable --now docker || warn "docker.service konnte nicht gestartet werden."
fi
if getent group docker >/dev/null 2>&1; then
  if id -nG "$USER" | grep -qw docker; then
    info "User '$USER' ist in der docker-Gruppe."
  else
    $SUDO usermod -aG docker "$USER" || warn "Konnte User nicht zur docker-Gruppe hinzufügen."
    warn "Bitte neu einloggen/WSL neu starten, damit Docker ohne sudo funktioniert."
  fi
fi
if ! docker info >/dev/null 2>&1; then
  warn "Docker-Daemon nicht erreichbar (Neustart/WSL shutdown nötig?)."
fi

# -------- 3b) Docker Sanity Checks (eingebaut aus deiner Liste) --------
docker_sanity_checks(){
  bold "3b) Docker Sanity Checks"

  # 1) Läuft Docker überhaupt?
  info "#1: docker ps"
  if docker ps >/dev/null 2>&1; then info "Docker-Daemon erreichbar."; else warn "Docker-Daemon NICHT erreichbar."; fi

  # 2) Ist dein User in der 'docker'-Gruppe?
  info "#2: Gruppen von $USER"
  groups_out="$(id -nG "$USER" || true)"; info "Gruppen: $groups_out"
  if ! grep -qw docker <<<"$groups_out"; then
    warn "User '$USER' ist nicht in der docker-Gruppe – füge hinzu…"
    $SUDO groupadd docker 2>/dev/null || true
    $SUDO usermod -aG docker "$USER" || true
    warn "Bitte neu einloggen oder 'newgrp docker' ausführen."
  fi

  # 3) Falls 'docker' nicht in der Liste: hinzufügen -> (oben erledigt)

  # 4) Gruppe sofort aktivieren (oder neu einloggen)
  info "#4: Hinweis: 'newgrp docker' wechselt die Gruppenzugehörigkeit im aktuellen Terminal."

  # 5) Socket-Rechte prüfen
  info "#5: /var/run/docker.sock prüfen"
  if [[ -S /var/run/docker.sock ]]; then
    ls -l /var/run/docker.sock || true
    owner=$(stat -c '%U' /var/run/docker.sock 2>/dev/null || echo '?')
    group=$(stat -c '%G' /var/run/docker.sock 2>/dev/null || echo '?')
    mode=$(stat -c '%a' /var/run/docker.sock 2>/dev/null || echo '?')
    info "Socket owner=$owner group=$group mode=$mode"
    if [[ "$owner" != root || "$group" != docker ]]; then
      warn "Setze Eigentümer temporär auf root:docker"
      $SUDO chown root:docker /var/run/docker.sock || true
    fi
    if [[ "$mode" != 660 && "$mode" != 666 ]]; then
      warn "Setze Modus temporär auf 660"
      $SUDO chmod 660 /var/run/docker.sock || true
    fi
  else
    warn "Docker-Socket nicht gefunden – läuft der Daemon?"
  fi

  # 6) Kurz testen, ob der Comet-Container jetzt läuft
  info "#6: Teste Comet-Container"
  if docker run --rm quay.io/medbioinf/comet-ms:v2024.01.0 comet -p | head -n1; then
    info "Comet-Container läuft."
  else
    warn "Comet-Container-Test fehlgeschlagen."
  fi
}

docker_sanity_checks

# -------- 4) Nextflow installieren (mit Fallback) --------
bold "4) Nextflow installieren (lokal im Projekt)"
NF_BIN="${PROJECT_ROOT}/nextflow"

install_nextflow(){
  local nf_out="$1"
  if [[ -x "$nf_out" ]]; then
    info "Nextflow bereits vorhanden: $nf_out"
    return 0
  fi

  local tmp dir nf_boot="get.nextflow"
  dir="$(mktemp -d)"; pushd "$dir" >/dev/null

  # 1) Primär: Bootstrap-Skript herunterladen und ausführen
  if download "https://get.nextflow.io" "$nf_boot"; then
    chmod +x "$nf_boot" || true
    if bash "./$nf_boot"; then
      if [[ -s "nextflow" ]]; then
        mv "nextflow" "$nf_out"
        chmod +x "$nf_out"
        popd >/dev/null; rm -rf "$dir"
        info "Nextflow via Bootstrap installiert: $nf_out"
        return 0
      else
        warn "Bootstrap lief, aber 'nextflow' wurde nicht erzeugt."
      fi
    else
      warn "Bootstrap-Skript konnte nicht ausgeführt werden."
    fi
  else
    warn "Download von get.nextflow.io fehlgeschlagen."
  fi

  # 2) Fallback: direkte Binary von GitHub (latest release)
  info "Versuche Fallback-Download der Nextflow-Binary…"
  if download "https://github.com/nextflow-io/nextflow/releases/latest/download/nextflow" "nextflow"; then
    if [[ -s "nextflow" ]]; then
      mv "nextflow" "$nf_out"
      chmod +x "$nf_out"
      popd >/dev/null; rm -rf "$dir"
      info "Nextflow via Fallback installiert: $nf_out"
      return 0
    fi
  fi

  popd >/dev/null; rm -rf "$dir"
  return 1
}

if install_nextflow "$NF_BIN"; then
  "$NF_BIN" -version || warn "Nextflow Version konnte nicht ermittelt werden."
else
  die "Nextflow konnte nicht installiert werden. Prüfe Netzwerk/Proxy & Java."
fi

# -------- 5) McQuaC klonen/aktualisieren --------
bold "5) McQuaC Repository holen"
REPO_URL="https://github.com/mpc-bioinformatics/McQuaC.git"
REPO_DIR="${PROJECT_ROOT}/McQuaC"
if [[ -d "$REPO_DIR/.git" ]]; then
  info "McQuaC vorhanden – aktualisiere…"
  git -C "$REPO_DIR" fetch --all --prune
  git -C "$REPO_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$REPO_DIR"
fi

# main.nf finden (Top-Level oder ein Unterordner)
if [[ -f "${REPO_DIR}/main.nf" ]]; then
  MAIN_NF="${REPO_DIR}/main.nf"
else
  MAIN_NF="$(find "$REPO_DIR" -maxdepth 2 -type f -name main.nf | head -n1 || true)"
fi
[[ -n "${MAIN_NF:-}" && -f "$MAIN_NF" ]] || die "Konnte main.nf im McQuaC-Repo nicht finden."
info "Gefundener mcquac_path = $MAIN_NF"

# -------- 6) app.json aktualisieren/erzeugen --------
bold "6) config/app.json aktualisieren"
APP_JSON="${PROJECT_ROOT}/config/app.json"
$SUDO mkdir -p "${PROJECT_ROOT}/config"
if [[ -f "$APP_JSON" ]]; then
  TMP_APP="$(mktemp)"
  jq --arg nfbin "$NF_BIN" --arg mcq "$MAIN_NF" \
     '. + {nextflow_bin: $nfbin, mcquac_path: $mcq}' "$APP_JSON" > "$TMP_APP" \
     || die "jq konnte app.json nicht patchen."
  $SUDO mv "$TMP_APP" "$APP_JSON"
  info "app.json gepatcht."
else
  cat >"$APP_JSON"<<EOF
{
  "interval_minutes": 1,
  "default_pattern": "*std.raw",
  "mcquac_path": "$(printf '%s' "$MAIN_NF")",
  "nextflow_bin": "$(printf '%s' "$NF_BIN")",
  "mounts": [],
  "unmount_on_exit": true,
  "io_pairs": []
}
EOF
  info "Neue app.json erstellt."
fi

# -------- 7) Python venv + requirements --------
bold "7) Python-Venv vorbereiten"
if [[ ! -d "${PROJECT_ROOT}/.venv" ]]; then
  python3 -m venv "${PROJECT_ROOT}/.venv"
  info "Venv erstellt: ${PROJECT_ROOT}/.venv"
fi
# shellcheck disable=SC1091
source "${PROJECT_ROOT}/.venv/bin/activate"
pip install --upgrade pip setuptools wheel >/dev/null
if [[ -f "${PROJECT_ROOT}/requirements.txt" ]]; then
  pip install -r "${PROJECT_ROOT}/requirements.txt"
else
  info "Keine requirements.txt gefunden – überspringe Paketinstallation."
fi
deactivate || true

# -------- 8) Verzeichnisse --------
bold "8) Verzeichnisse anlegen"
mkdir -p "${PROJECT_ROOT}/tmp" "${PROJECT_ROOT}/config/fasta" "${PROJECT_ROOT}/config/spike"
touch "${PROJECT_ROOT}/config/spike/.keep" "${PROJECT_ROOT}/config/fasta/.keep"

# -------- 9) Container-Images aus nextflow.config (optional) --------
bold "9) Container-Images vorziehen (falls definierbar)"
NXF_CFG="$(dirname "$MAIN_NF")/nextflow.config"
if [[ -f "$NXF_CFG" ]]; then
  images="$(grep -Po "(?<=container\s*=\s*['\"]).+?(?=['\"])|(?<=container\s+['\"]).+?(?=['\"])|(?<=container\s*=\s*)[\w./:-]+" "$NXF_CFG" | sort -u || true)"
  if [[ -n "$images" ]]; then
    info "Gefundene Container:"
    printf "    - %s\n" $images
    if have docker && docker info >/dev/null 2>&1; then
      while IFS= read -r img; do
        [[ -z "$img" ]] && continue
        docker pull "$img" || warn "pull fehlgeschlagen: $img"
      done <<< "$images"
    else
      warn "Docker nicht verfügbar – überspringe pulls."
    fi
  else
    info "Keine Container-Zuweisungen gefunden – pull übersprungen."
  fi
else
  warn "nextflow.config neben main.nf nicht gefunden – pull übersprungen."
fi

# -------- 10) Abschluss-Checks --------
bold "10) Abschluss-Checks"
JAVA_OK=$([[ "$(java -version 2>&1 | head -n1)" =~ "version" ]] && echo OK || echo FAIL)
DOCKER_OK=$({ docker info >/dev/null 2>&1 && echo OK; } || echo FAIL)
NXF_OK=$({ "$NF_BIN" -version >/dev/null 2>&1 && echo OK; } || echo FAIL)
printf "\n"
info "Java:        $JAVA_OK"
info "Docker:      $DOCKER_OK"
info "Nextflow:    $NXF_OK"
info "nextflow_bin: $(jq -r '.nextflow_bin' "$APP_JSON" 2>/dev/null || echo '?')"
info "mcquac_path:  $(jq -r '.mcquac_path' "$APP_JSON" 2>/dev/null || echo '?')"
printf "\n"
bold "Fertig!"
info "Test optional: '${NF_BIN}' run hello   (mit Docker: '${NF_BIN}' run hello -with-docker)"
