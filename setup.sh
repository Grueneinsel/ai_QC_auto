#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================
#  MCQuaC / ai_QC_auto Setup (Ubuntu/WSL2)
#  - Docker Engine + Compose
#  - Java (OpenJDK 21)
#  - Nextflow lokal im Projekt (PROJECT_ROOT/nextflow)
#  - Python venv + deps (optional requirements.txt)
#  - app.json: nextflow_bin eintragen (falls fehlt)
#  - Container-Images aus nextflow.config vorziehen
# ============================================

### Farben / Logging
bold() { printf "\033[1m%s\033[0m\n" "$*"; }
info() { printf "  • %s\n" "$*"; }
warn() { printf "\033[33m[WARN]\033[0m %s\n" "$*"; }
err()  { printf "\033[31m[ERR]\033[0m  %s\n" "$*"; }
die()  { err "$*"; exit 1; }

# Root/Sudo ermitteln
if [[ $EUID -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    die "Bitte als root ausführen oder 'sudo' installieren."
  fi
else
  SUDO=""
fi

# Projekt-Root (dieses Script erwartet: scripts/setup.sh)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

bold "MCQuaC / ai_QC_auto Setup"
info "PROJECT_ROOT = $PROJECT_ROOT"

# --- kleine Helfer ---
apt_install() {
  $SUDO apt-get update -y
  # retry gegen flakey mirrors
  $SUDO apt-get install -y --no-install-recommends "$@" || {
    warn "apt install fehlgeschlagen, erneut versuchen…"
    $SUDO apt-get install -y --no-install-recommends "$@"
  }
}

ensure_line_in_file() {
  # $1=file, $2=line
  local f="$1" line="$2"
  [[ -f "$f" ]] || touch "$f"
  grep -Fxq "$line" "$f" || echo "$line" | $SUDO tee -a "$f" >/dev/null
}

# --- Basis-Pakete ---
bold "1) System-Pakete installieren"
apt_install ca-certificates curl gnupg lsb-release unzip zip jq git \
            openjdk-21-jre-headless python3-venv python3-pip \
            cifs-utils apt-transport-https software-properties-common

# --- WSL2: systemd einschalten (für Docker-Dienst) ---
if grep -qi microsoft /proc/version 2>/dev/null; then
  bold "WSL erkannt – systemd prüfen"
  WSL_CONF="/etc/wsl.conf"
  TMP_WSL="$(mktemp)"
  if [[ -f "$WSL_CONF" ]]; then
    cat "$WSL_CONF" > "$TMP_WSL"
    if ! grep -q "^\[boot\]" "$TMP_WSL"; then
      printf "\n[boot]\n" >> "$TMP_WSL"
    fi
    if grep -q "^systemd\s*=" "$TMP_WSL"; then
      $SUDO sed -i 's/^systemd\s*=.*/systemd=true/g' "$TMP_WSL"
    else
      printf "systemd=true\n" >> "$TMP_WSL"
    fi
  else
    cat > "$TMP_WSL" <<EOF
[boot]
systemd=true
EOF
  fi
  $SUDO cp "$TMP_WSL" "$WSL_CONF"
  rm -f "$TMP_WSL"
  info "WSL systemd aktiviert (falls nicht schon aktiv). Hinweis: Ein WSL-Neustart kann nötig sein: 'wsl.exe --shutdown'"
fi

# --- Docker installieren (offizielles Repo) ---
bold "2) Docker Engine + Compose installieren"
if ! command -v docker >/dev/null 2>&1; then
  # Docker GPG Key & Repo
  $SUDO install -m 0755 -d /etc/apt/keyrings
  if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | $SUDO gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    $SUDO chmod a+r /etc/apt/keyrings/docker.gpg
  fi
  UB_CODENAME="$($SUDO bash -c 'source /etc/os-release && echo $VERSION_CODENAME')"
  echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu ${UB_CODENAME} stable" | \
  $SUDO tee /etc/apt/sources.list.d/docker.list >/dev/null

  apt_install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
else
  info "Docker ist bereits installiert."
fi

# Docker-Dienst starten/aktivieren (wenn systemd läuft)
if command -v systemctl >/dev/null 2>&1; then
  $SUDO systemctl enable --now docker || warn "Konnte docker.service nicht starten – evtl. WSL-Neustart nötig."
fi

# Nutzer in docker-Gruppe (damit kein sudo nötig)
if getent group docker >/dev/null 2>&1; then
  if id -nG "$USER" | grep -qw docker; then
    info "User '$USER' ist bereits in der docker-Gruppe."
  else
    $SUDO usermod -aG docker "$USER" || warn "Konnte User nicht zur docker-Gruppe hinzufügen."
    warn "Bitte neu einloggen/WSL-Session neu starten, damit Docker ohne sudo funktioniert."
  fi
fi

# Kurzer Docker-Test (nicht fatal)
if command -v docker >/dev/null 2>&1; then
  if ! docker info >/dev/null 2>&1; then
    warn "Docker Dämon nicht erreichbar. Falls WSL: 'wsl.exe --shutdown' und Terminal neu öffnen."
  fi
else
  warn "docker CLI nicht gefunden – Installation übersprungen?"
fi

# --- Nextflow lokal ins Projekt legen ---
bold "3) Nextflow installieren (lokal im Projekt)"
NF_BIN="${PROJECT_ROOT}/nextflow"
if [[ -x "$NF_BIN" ]]; then
  info "Nextflow bereits vorhanden: $NF_BIN"
else
  # safer tmp install; vermeidet TTY-Pipes in manchen Umgebungen
  TMPDIR="$(mktemp -d)"
  pushd "$TMPDIR" >/dev/null
  curl -fsSL https://get.nextflow.io -o get.nextflow
  chmod +x get.nextflow
  ./get.nextflow
  mv nextflow "$NF_BIN"
  chmod +x "$NF_BIN"
  popd >/dev/null
  rm -rf "$TMPDIR"
  info "Nextflow installiert: $NF_BIN"
fi
# Version anzeigen
"$NF_BIN" -version || warn "Nextflow Version konnte nicht ermittelt werden."

# --- app.json: nextflow_bin eintragen, falls fehlt ---
bold "4) config/app.json aktualisieren (nextflow_bin)"
APP_JSON="${PROJECT_ROOT}/config/app.json"
if [[ -f "$APP_JSON" ]]; then
  if jq -e '.nextflow_bin' "$APP_JSON" >/dev/null 2>&1; then
    info "nextflow_bin existiert bereits in app.json – unverändert gelassen."
  else
    TMP_APP="$(mktemp)"
    jq --arg nf "$NF_BIN" '. + {nextflow_bin: $nf}' "$APP_JSON" > "$TMP_APP" \
      && $SUDO mv "$TMP_APP" "$APP_JSON"
    info "nextflow_bin in app.json ergänzt: $NF_BIN"
  fi
else
  warn "config/app.json wurde nicht gefunden – Überspringe Patch."
fi

# --- Python venv & (optionales) requirements.txt ---
bold "5) Python-Venv vorbereiten"
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

# --- Verzeichnisstruktur anlegen ---
bold "6) Verzeichnisse & Platzhalter anlegen"
mkdir -p "${PROJECT_ROOT}/tmp" \
         "${PROJECT_ROOT}/config/fasta" \
         "${PROJECT_ROOT}/config/spike"
touch "${PROJECT_ROOT}/config/spike/.keep" "${PROJECT_ROOT}/config/fasta/.keep"

# --- Container-Images aus nextflow.config vorziehen ---
bold "7) Container-Images vorziehen (falls definierbar)"
# mcquac_path aus app.json auslesen
MCQUAC_PATH="$(jq -r '.mcquac_path // empty' "$APP_JSON" 2>/dev/null || true)"
if [[ -n "$MCQUAC_PATH" && -f "$MCQUAC_PATH" ]]; then
  NXF_DIR="$(cd -- "$(dirname -- "$MCQUAC_PATH")" && pwd)"
  NXF_CFG="${NXF_DIR}/nextflow.config"
  if [[ -f "$NXF_CFG" ]]; then
    # simple Extract: container = 'image[:tag]' oder "image"
    mapfile -t IMAGES < <(grep -Po "(?<=container\s*=\s*['\"]).+?(?=['\"])|(?<=container\s+['\"]).+?(?=['\"])|(?<=container\s*=\s*)[\w./:-]+" "$NXF_CFG" | sort -u || true)
    if [[ ${#IMAGES[@]} -gt 0 ]]; then
      info "Gefundene Container:"
      printf "    - %s\n" "${IMAGES[@]}"
      for img in "${IMAGES[@]}"; do
        if [[ "$img" =~ ^[-_.a-zA-Z0-9/]+(:[-_.a-zA-Z0-9]+)?$ ]]; then
          if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
            docker pull "$img" || warn "pull fehlgeschlagen: $img"
          else
            warn "Docker nicht verfügbar – Überspringe pull von $img"
          fi
        fi
      done
    else
      info "Keine Container-Zuweisungen in nextflow.config gefunden – pull übersprungen."
    fi
  else
    warn "nextflow.config nicht gefunden neben mcquac_path ($NXF_CFG) – übersprungen."
  fi
else
  warn "mcquac_path nicht (korrekt) in app.json gesetzt – Container-Pull übersprungen."
fi

# --- kleiner Abschluss-Check ---
bold "8) Checks"
JAVA_OK=$([[ "$(java -version 2>&1 | head -n1)" =~ "version" ]] && echo OK || echo FAIL)
DOCKER_OK=$({ docker info >/dev/null 2>&1 && echo OK; } || echo FAIL)
NXF_OK=$({ "$NF_BIN" -version >/dev/null 2>&1 && echo OK; } || echo FAIL)

printf "\n"
info "Java:   $JAVA_OK"
info "Docker: $DOCKER_OK"
info "NXF:    $NXF_OK"
printf "\n"

bold "Fertig!"
info "Falls Docker in WSL frischer aktiviert wurde: WSL einmal neu starten (PowerShell: 'wsl.exe --shutdown')."
info "Nextflow-Binary: $NF_BIN"
info "app.json aktualisiert (nextflow_bin), falls nötig."
