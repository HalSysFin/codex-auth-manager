#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLIENT_DIR="${REPO_ROOT}/headless-client"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/auth-manager-agent"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/auth-manager-agent"
BIN_DIR="${HOME}/.local/bin"
CONFIG_FILE="${CONFIG_DIR}/config.json"
LAUNCHER="${BIN_DIR}/auth-manager-agent"
SYSTEMD_DIR="${HOME}/.config/systemd/user"
SYSTEMD_UNIT="${SYSTEMD_DIR}/auth-manager-agent.service"

command -v node >/dev/null 2>&1 || { echo "Node.js is required"; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "npm is required"; exit 1; }

mkdir -p "${CONFIG_DIR}" "${STATE_DIR}" "${BIN_DIR}" "${SYSTEMD_DIR}"

echo "Installing headless client dependencies..."
(cd "${CLIENT_DIR}" && npm install && npm run build)

if [[ ! -f "${CONFIG_FILE}" ]]; then
  cat > "${CONFIG_FILE}" <<'JSON'
{
  "baseUrl": "http://127.0.0.1:8080",
  "internalApiToken": "",
  "authFilePath": "~/.codex/auth.json",
  "refreshIntervalSeconds": 60,
  "telemetryIntervalSeconds": 300,
  "autoRenew": true,
  "autoRotate": true,
  "allowInsecureLocalhost": true
}
JSON
  echo "Wrote default config to ${CONFIG_FILE}"
fi

cat > "${LAUNCHER}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
exec node "${CLIENT_DIR}/dist/headless-client/src/cli.js" "\$@"
EOF
chmod +x "${LAUNCHER}"

sed "s|__REPO_ROOT__|${REPO_ROOT}|g; s|__HOME__|${HOME}|g" \
  "${REPO_ROOT}/systemd/auth-manager-agent.service" > "${SYSTEMD_UNIT}"

echo
echo "Installed launcher: ${LAUNCHER}"
echo "Config file: ${CONFIG_FILE}"
echo "State dir: ${STATE_DIR}"
echo
echo "Next steps:"
echo "  1. Edit ${CONFIG_FILE} and set your internalApiToken/baseUrl if needed"
echo "  2. Run: auth-manager-agent doctor"
echo "  3. Run: auth-manager-agent ensure"
echo "  4. Optional daemon: systemctl --user enable --now auth-manager-agent"
