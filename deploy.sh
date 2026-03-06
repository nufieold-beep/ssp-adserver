#!/bin/bash
set -e

REPO="https://github.com/nufieold-beep/ssp-adserver.git"
INSTALL_DIR="/opt/ssp"
SERVICE_NAME="ssp"
GO_VERSION="1.24.0"
GO_MIN_VERSION="1.24"
RUNTIME_CONFIG_DIR="/etc/ssp"
RUNTIME_CONFIG_PATH="${RUNTIME_CONFIG_DIR}/bidders.yaml"
REPO_CONFIG_REL="configs/bidders.yaml"

export PATH="/usr/local/go/bin:${PATH}"

version_ge() {
  [ "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" = "$2" ]
}

install_go() {
  echo "[1/5] Installing Go ${GO_VERSION}..."
  wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
  rm -rf /usr/local/go
  tar -C /usr/local -xzf /tmp/go.tar.gz
  if ! grep -q '/usr/local/go/bin' /etc/profile; then
    echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
  fi
  export PATH="/usr/local/go/bin:${PATH}"
  rm /tmp/go.tar.gz
}

echo "=== SSP Ad Server Deploy Script ==="

# Install or upgrade Go to meet go.mod requirement
if command -v go &> /dev/null; then
  CURRENT_GO_VERSION="$(go version | awk '{print $3}' | sed 's/^go//')"
  if version_ge "$CURRENT_GO_VERSION" "$GO_MIN_VERSION"; then
    echo "[1/5] Go already installed: $(go version)"
  else
    echo "[1/5] Go ${CURRENT_GO_VERSION} is below required ${GO_MIN_VERSION}; upgrading..."
    install_go
  fi
else
  install_go
fi

# Install git if not present
if ! command -v git &> /dev/null; then
  echo "[2/5] Installing git..."
  apt-get update -qq && apt-get install -y -qq git
else
  echo "[2/5] Git already installed"
fi

# Clone or pull repo
if [ -d "${INSTALL_DIR}/.git" ]; then
  echo "[3/5] Updating existing repo..."
  cd "$INSTALL_DIR"

  LOCAL_BIDDERS_BACKUP=""
  if [ -f "${REPO_CONFIG_REL}" ] && ! git diff --quiet -- "${REPO_CONFIG_REL}"; then
    echo "[3/5] Preserving local ${REPO_CONFIG_REL} before update..."
    LOCAL_BIDDERS_BACKUP="/tmp/ssp-bidders.$(date +%s).yaml"
    cp "${REPO_CONFIG_REL}" "${LOCAL_BIDDERS_BACKUP}"
    git checkout -- "${REPO_CONFIG_REL}"
  fi

  git pull --ff-only origin main

  mkdir -p "${RUNTIME_CONFIG_DIR}"
  if [ -n "${LOCAL_BIDDERS_BACKUP}" ]; then
    cp "${LOCAL_BIDDERS_BACKUP}" "${RUNTIME_CONFIG_PATH}"
    rm -f "${LOCAL_BIDDERS_BACKUP}"
  elif [ ! -f "${RUNTIME_CONFIG_PATH}" ] && [ -f "${REPO_CONFIG_REL}" ]; then
    cp "${REPO_CONFIG_REL}" "${RUNTIME_CONFIG_PATH}"
  fi
else
  echo "[3/5] Cloning repo..."
  rm -rf "$INSTALL_DIR"
  git clone "$REPO" "$INSTALL_DIR"
  cd "$INSTALL_DIR"

  mkdir -p "${RUNTIME_CONFIG_DIR}"
  if [ ! -f "${RUNTIME_CONFIG_PATH}" ] && [ -f "${REPO_CONFIG_REL}" ]; then
    cp "${REPO_CONFIG_REL}" "${RUNTIME_CONFIG_PATH}"
  fi
fi

# Build
echo "[4/5] Building SSP binary..."
cd "$INSTALL_DIR"
go build -o ssp ./cmd/ssp/
chmod +x ssp

# Stop old instance
systemctl stop "$SERVICE_NAME" 2>/dev/null || true
pkill ssp 2>/dev/null || true
fuser -k 8080/tcp 2>/dev/null || true

# Create systemd service
echo "[5/5] Setting up systemd service..."
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=SSP Ad Server
After=network.target

[Service]
Type=simple
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/ssp
Environment=SSP_CONFIG_PATH=${RUNTIME_CONFIG_PATH}
Environment=SSP_API_KEY=${SSP_API_KEY:-change-this-to-a-secret}
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl start "$SERVICE_NAME"

echo ""
echo "=== Deploy complete ==="
echo "Status:"
systemctl status "$SERVICE_NAME" --no-pager
echo ""
echo "Server running at http://$(hostname -I | awk '{print $1}'):8080"
echo "Dashboard: http://$(hostname -I | awk '{print $1}'):8080/dashboard"
