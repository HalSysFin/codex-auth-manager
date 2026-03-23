#!/usr/bin/env bash
set -euo pipefail

BIN_DIR="${HOME}/.local/bin"
SYSTEMD_DIR="${HOME}/.config/systemd/user"
LAUNCHER="${BIN_DIR}/auth-manager-agent"
SYSTEMD_UNIT="${SYSTEMD_DIR}/auth-manager-agent.service"

if command -v systemctl >/dev/null 2>&1; then
  systemctl --user disable --now auth-manager-agent >/dev/null 2>&1 || true
  systemctl --user daemon-reload >/dev/null 2>&1 || true
fi

rm -f "${LAUNCHER}" "${SYSTEMD_UNIT}"

echo "Removed headless client launcher and systemd unit."
echo "Local config/state under ~/.config/auth-manager-agent and ~/.local/state/auth-manager-agent were left in place."
