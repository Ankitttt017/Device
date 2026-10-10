#!/usr/bin/env bash
# ==============================================================================
# QUAD Gateway — Raspberry Pi Systemd Service Installer
# ==============================================================================

set -e

SERVICE_NAME="quad_gateway"
TARGET_SERVICE="/etc/systemd/system/${SERVICE_NAME}.service"

CURRENT_USER=$(id -un)
PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# Detect Python interpreter (virtualenv preferred if present)
if [ -f "${PROJECT_DIR}/venv/bin/python3" ]; then
    PYTHON_EXEC="${PROJECT_DIR}/venv/bin/python3"
elif [ -f "${PROJECT_DIR}/.venv/bin/python3" ]; then
    PYTHON_EXEC="${PROJECT_DIR}/.venv/bin/python3"
else
    PYTHON_EXEC=$(which python3)
fi

echo "================================================================="
echo " Installing ${SERVICE_NAME} systemd service on Raspberry Pi"
echo "================================================================="
echo "User:               ${CURRENT_USER}"
echo "Project Directory:  ${PROJECT_DIR}"
echo "Python Executable:  ${PYTHON_EXEC}"
echo "-----------------------------------------------------------------"

# Write systemd service file with detected parameters
sudo bash -c "cat <<EOF > ${TARGET_SERVICE}
[Unit]
Description=QUAD Gateway Industrial Edge Telemetry Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${PROJECT_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${PYTHON_EXEC} -m quad_gateway.main
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=10

[Install]
WantedBy=multi-user.target
EOF"

echo "Service file written to: ${TARGET_SERVICE}"

# Reload systemd, enable service on boot, and start it
echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "Enabling service to start automatically on boot..."
sudo systemctl enable ${SERVICE_NAME}.service

echo "Starting service..."
sudo systemctl restart ${SERVICE_NAME}.service

echo "================================================================="
echo " Installation Complete!"
echo " Service is now active and will automatically run on every boot."
echo "================================================================="
echo ""
echo "Useful Commands:"
echo "  Check service status:   sudo systemctl status ${SERVICE_NAME}"
echo "  View live service logs: sudo journalctl -u ${SERVICE_NAME} -f"
echo "  View file logs:         tail -f ${PROJECT_DIR}/logs/gateway.log"
echo "  Stop service:           sudo systemctl stop ${SERVICE_NAME}"
echo "  Restart service:        sudo systemctl restart ${SERVICE_NAME}"
echo "================================================================="
