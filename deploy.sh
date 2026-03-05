#!/bin/bash
set -e

REPO="https://github.com/nufieold-beep/ssp-adserver.git"
INSTALL_DIR="/opt/ssp"
SERVICE_NAME="ssp"
GO_VERSION="1.22.5"

echo "=== SSP Ad Server Deploy Script ==="

# Install Go if not present
if ! command -v go &> /dev/null; then
  echo "[1/5] Installing Go ${GO_VERSION}..."
  wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
  tar -C /usr/local -xzf /tmp/go.tar.gz
  echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
  export PATH=$PATH:/usr/local/go/bin
  rm /tmp/go.tar.gz
else
  echo "[1/5] Go already installed: $(go version)"
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
  git pull origin main
else
  echo "[3/5] Cloning repo..."
  rm -rf "$INSTALL_DIR"
  git clone "$REPO" "$INSTALL_DIR"
  cd "$INSTALL_DIR"
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
