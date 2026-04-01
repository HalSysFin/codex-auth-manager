const startBtn = document.getElementById("startBtn");
const optionsBtn = document.getElementById("optionsBtn");
const statusEl = document.getElementById("status");
const relayEnabledEl = document.getElementById("relayEnabled");
const relayStateEl = document.getElementById("relayState");

startBtn.addEventListener("click", async () => {
  setStatus("Starting relay login...", "");
  startBtn.disabled = true;

  try {
    const response = await chrome.runtime.sendMessage({ type: "start-login-flow" });
    if (!response?.ok) {
      throw new Error(response?.error || "Failed to start login flow");
    }

    const authUrl = response.result?.auth_url;
    setStatus(
      authUrl
        ? `Auth flow started. Opened:\n${authUrl}`
        : "Auth flow started.",
      "ok"
    );
  } catch (error) {
    setStatus(String(error.message || error), "err");
  } finally {
    startBtn.disabled = false;
  }
});

optionsBtn.addEventListener("click", () => {
  if (chrome.runtime.openOptionsPage) {
    chrome.runtime.openOptionsPage();
  }
});

relayEnabledEl.addEventListener("change", async () => {
  const enabled = relayEnabledEl.checked;
  setToggleState(enabled);
  setStartAvailability(enabled);
  setStatus(
    enabled ? "Relay enabled. Localhost callbacks will be captured." : "Relay disabled. Callback capture is paused.",
    enabled ? "ok" : ""
  );

  try {
    const response = await chrome.runtime.sendMessage({
      type: "set-relay-enabled",
      enabled
    });
    if (!response?.ok) {
      throw new Error(response?.error || "Could not update relay state");
    }
    const settings = response.settings || {};
    setToggleState(Boolean(settings.relayEnabled));
    setStartAvailability(Boolean(settings.relayEnabled));
  } catch (error) {
    relayEnabledEl.checked = !enabled;
    setToggleState(!enabled);
    setStartAvailability(!enabled);
    setStatus(`Toggle failed: ${String(error.message || error)}`, "err");
  }
});

async function loadSettingsPreview() {
  try {
    const response = await chrome.runtime.sendMessage({ type: "get-relay-settings" });
    if (!response?.ok) {
      throw new Error(response?.error || "Could not read settings");
    }
    const enabled = Boolean(response.settings?.relayEnabled ?? true);
    setToggleState(enabled);
    setStartAvailability(enabled);
    const base = response.settings?.authManagerBaseUrl || "(not set)";
    setStatus(
      enabled ? `Relay is on.\nauth-manager: ${base}` : `Relay is off.\nauth-manager: ${base}`,
      enabled ? "" : ""
    );
  } catch (error) {
    setStatus(`Settings error: ${String(error.message || error)}`, "err");
  }
}

function setStartAvailability(enabled) {
  startBtn.disabled = !enabled;
}

function setToggleState(enabled) {
  relayEnabledEl.checked = enabled;
  relayStateEl.textContent = enabled ? "On" : "Off";
}

function setStatus(text, cls) {
  statusEl.textContent = text;
  statusEl.className = `status ${cls}`.trim();
}

void loadSettingsPreview();
