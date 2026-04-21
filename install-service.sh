#!/bin/bash
# Install ig-dm as a systemd service — runs 24/7, auto-restarts on crash, starts on boot.
# Usage: sudo bash install-service.sh

set -e

if [ "$EUID" -ne 0 ]; then
    echo "[!] Please run with sudo: sudo bash install-service.sh"
    exit 1
fi

SERVICE_SRC="$(dirname "$(readlink -f "$0")")/ig-dm.service"
SERVICE_DST="/etc/systemd/system/ig-dm.service"

if [ ! -f "$SERVICE_SRC" ]; then
    echo "[!] ig-dm.service not found next to this script."
    exit 1
fi

echo "[*] Copying service file to $SERVICE_DST"
cp "$SERVICE_SRC" "$SERVICE_DST"

echo "[*] Reloading systemd"
systemctl daemon-reload

echo "[*] Enabling service (auto-start on boot)"
systemctl enable ig-dm.service

echo "[*] Starting service now"
systemctl restart ig-dm.service

sleep 2
echo ""
echo "── Service Status ──"
systemctl status ig-dm.service --no-pager | head -15

echo ""
echo "[✓] Installed! Useful commands:"
echo "    sudo systemctl status ig-dm      # check status"
echo "    sudo systemctl stop ig-dm        # stop"
echo "    sudo systemctl start ig-dm       # start"
echo "    sudo systemctl restart ig-dm     # restart"
echo "    sudo journalctl -u ig-dm -f      # live logs (systemd)"
echo "    tail -f /var/log/ig-dm.log       # live logs (file)"
