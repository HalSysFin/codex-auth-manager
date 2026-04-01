const SETTINGS_KEY = "relaySettings";
const DEFAULT_SETTINGS = {
  authManagerBaseUrl: "http://localhost:8080",
  authManagerBearerToken: "",
  relayEnabled: true
};

const baseUrlEl = document.getElementById("baseUrl");
const bearerTokenEl = document.getElementById("bearerToken");
const saveBtn = document.getElementById("saveBtn");
const statusEl = document.getElementById("status");

saveBtn.addEventListener("click", async () => {
  const authManagerBaseUrl = normalizeBaseUrl(baseUrlEl.value);
  const authManagerBearerToken = String(bearerTokenEl.value || "").trim();

  if (!/^https?:\/\//i.test(authManagerBaseUrl)) {
    setStatus("Base URL must start with http:// or https://", "err");
    return;
  }

  await chrome.storage.sync.set({
    [SETTINGS_KEY]: {
      authManagerBaseUrl,
      authManagerBearerToken
    }
  });

  setStatus("Settings saved.", "ok");
});

async function loadSettings() {
  const data = await chrome.storage.sync.get(SETTINGS_KEY);
  const saved = data[SETTINGS_KEY] || {};
  const settings = {
    authManagerBaseUrl: saved.authManagerBaseUrl || DEFAULT_SETTINGS.authManagerBaseUrl,
    authManagerBearerToken:
      saved.authManagerBearerToken || DEFAULT_SETTINGS.authManagerBearerToken,
    relayEnabled:
      typeof saved.relayEnabled === "boolean"
        ? saved.relayEnabled
        : DEFAULT_SETTINGS.relayEnabled
  };

  baseUrlEl.value = settings.authManagerBaseUrl;
  bearerTokenEl.value = settings.authManagerBearerToken;
}

function normalizeBaseUrl(url) {
  return String(url || DEFAULT_SETTINGS.authManagerBaseUrl).trim().replace(/\/+$/, "");
}

function setStatus(text, cls) {
  statusEl.textContent = text;
  statusEl.className = cls;
}

void loadSettings();
