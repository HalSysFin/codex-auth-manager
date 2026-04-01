const SESSION_KEY = "activeRelaySession";
const SETTINGS_KEY = "relaySettings";
const DEFAULT_SETTINGS = {
  authManagerBaseUrl: "http://localhost:8080",
  authManagerBearerToken: "",
  relayEnabled: true
};
const CALLBACK_URL_FILTER = {
  url: [
    { urlMatches: "^http://127\\.0\\.0\\.1:1445/.*" },
    { urlMatches: "^http://localhost:1445/.*" },
    { urlMatches: "^http://127\\.0\\.0\\.1:1455/.*" },
    { urlMatches: "^http://localhost:1455/.*" }
  ]
};
const inFlightCallbackTabs = new Set();

chrome.commands.onCommand.addListener((command) => {
  if (command === "start-relay-login") {
    void startLoginFlow({ openErrorTab: true });
  }
});

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type === "start-login-flow") {
    startLoginFlow({ openErrorTab: false })
      .then((result) => sendResponse({ ok: true, result }))
      .catch((error) =>
        sendResponse({ ok: false, error: String(error?.message || error) })
      );
    return true;
  }

  if (message?.type === "get-relay-settings") {
    getRelaySettings()
      .then((settings) => sendResponse({ ok: true, settings }))
      .catch((error) =>
        sendResponse({ ok: false, error: String(error?.message || error) })
      );
    return true;
  }

  if (message?.type === "set-relay-enabled") {
    setRelayEnabled(Boolean(message.enabled))
      .then((settings) => sendResponse({ ok: true, settings }))
      .catch((error) =>
        sendResponse({ ok: false, error: String(error?.message || error) })
      );
    return true;
  }
});

chrome.webNavigation.onBeforeNavigate.addListener(
  (details) => {
    if (details.frameId !== 0) {
      return;
    }
    void maybeHandleLocalhostCallback(details.tabId, details.url);
  },
  CALLBACK_URL_FILTER
);

chrome.webNavigation.onErrorOccurred.addListener(
  (details) => {
    if (details.frameId !== 0) {
      return;
    }
    void maybeHandleLocalhostCallback(details.tabId, details.url);
  },
  CALLBACK_URL_FILTER
);

async function maybeHandleLocalhostCallback(tabId, callbackUrl) {
  if (inFlightCallbackTabs.has(tabId)) {
    return;
  }

  inFlightCallbackTabs.add(tabId);
  try {
    await handleLocalhostCallback(tabId, callbackUrl);
  } finally {
    inFlightCallbackTabs.delete(tabId);
  }
}

async function startLoginFlow({ openErrorTab }) {
  const settings = await getRelaySettings();
  if (!settings.relayEnabled) {
    throw new Error("Relay is turned off. Enable it in the popup before starting login.");
  }
  const baseUrl = normalizeBaseUrl(settings.authManagerBaseUrl);
  const token = settings.authManagerBearerToken || "";

  try {
    let response = await fetch(`${baseUrl}/auth/login/start-relay`, {
      method: "POST",
      headers: requestHeaders(token),
      body: "{}"
    });
    if (response.status === 404) {
      // Backward compatibility with older auth-manager versions.
      response = await fetch(`${baseUrl}/auth/login/start`, {
        method: "POST",
        headers: requestHeaders(token),
        body: "{}"
      });
    }

    if (!response.ok) {
      throw new Error(await readError(response, "Failed to start login"));
    }

    const data = await response.json();
    const session = data.session || {};

    if (!session.session_id || !session.relay_token || !data.auth_url) {
      throw new Error("auth-manager response missing session_id, relay_token, or auth_url");
    }

    await chrome.storage.session.set({
      [SESSION_KEY]: {
        session_id: session.session_id,
        relay_token: session.relay_token,
        expires_at: session.expires_at,
        auth_url: data.auth_url,
        auth_manager_base_url: baseUrl
      }
    });

    await chrome.tabs.create({ url: data.auth_url });
    return {
      session_id: session.session_id,
      auth_url: data.auth_url
    };
  } catch (error) {
    if (openErrorTab) {
      await openErrorPage(`Login start failed: ${String(error.message || error)}`);
    }
    throw error;
  }
}

async function handleLocalhostCallback(tabId, callbackUrl) {
  const currentSettings = await getRelaySettings();
  if (!currentSettings.relayEnabled) {
    return;
  }

  const storageData = await chrome.storage.session.get(SESSION_KEY);
  const session = storageData[SESSION_KEY];

  if (!session || !session.session_id || !session.relay_token) {
    await chrome.tabs.update(tabId, {
      url: chrome.runtime.getURL("error.html#reason=no-active-session")
    });
    return;
  }

  const callback = new URL(callbackUrl);
  const settings = await getRelaySettings();
  const baseUrl = session.auth_manager_base_url || normalizeBaseUrl(settings.authManagerBaseUrl);
  const token = settings.authManagerBearerToken || "";
  const payload = {
    session_id: session.session_id,
    relay_token: session.relay_token,
    code: callback.searchParams.get("code"),
    state: callback.searchParams.get("state"),
    error: callback.searchParams.get("error"),
    error_description: callback.searchParams.get("error_description"),
    full_url: callbackUrl
  };

  try {
    const response = await fetch(`${baseUrl}/auth/relay-callback`, {
      method: "POST",
      headers: requestHeaders(token),
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      throw new Error(await readError(response, "Relay callback failed"));
    }

    const data = await response.json();
    const relaySummary = summarizeRelayResponse(data);
    if (!relaySummary.ok) {
      throw new Error(relaySummary.message);
    }

    await chrome.storage.session.remove(SESSION_KEY);
    await chrome.tabs.update(tabId, {
      url: chrome.runtime.getURL(
        `success.html#message=${encodeURIComponent(relaySummary.message)}`
      )
    });
  } catch (error) {
    const reason = encodeURIComponent(String(error.message || error));
    await chrome.tabs.update(tabId, {
      url: chrome.runtime.getURL(`error.html#reason=${reason}`)
    });
  }
}

async function readError(response, fallback) {
  const raw = await response.text();
  try {
    const data = JSON.parse(raw);
    if (typeof data.detail === "string") {
      return data.detail;
    }
    if (data.detail && typeof data.detail.message === "string") {
      return data.detail.message;
    }
    if (typeof data.message === "string") {
      return data.message;
    }
  } catch (_err) {
    // Ignore and use raw text.
  }
  return raw || fallback;
}

function requestHeaders(token) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return headers;
}

function summarizeRelayResponse(data) {
  const handoff = data?.handoff || {};
  const autoPersist = data?.auto_persist || {};

  if (handoff.completed !== true) {
    return {
      ok: false,
      message:
        handoff.message ||
        "Auth callback was received, but the token exchange did not complete."
    };
  }

  if (autoPersist.attempted === true) {
    if (autoPersist.status === "persisted") {
      const label = autoPersist.label ? ` as ${autoPersist.label}` : "";
      const action = autoPersist.created_new_profile
        ? "saved as a new profile"
        : "updated in the matching profile";
      return {
        ok: true,
        message: `Login completed and ${action}${label}.`
      };
    }

    if (autoPersist.status === "skipped" && autoPersist.reason === "up_to_date") {
      const label = autoPersist.label ? ` (${autoPersist.label})` : "";
      return {
        ok: true,
        message: `Login completed. Saved profile was already up to date${label}.`
      };
    }

    if (autoPersist.status === "error") {
      return {
        ok: false,
        message:
          autoPersist.error ||
          "Auth finalized, but saving it to the profile failed."
      };
    }
  }

  return {
    ok: true,
    message: handoff.message || "Login completed."
  };
}

async function openErrorPage(reason) {
  const encoded = encodeURIComponent(reason);
  await chrome.tabs.create({
    url: chrome.runtime.getURL(`error.html#reason=${encoded}`)
  });
}

async function getRelaySettings() {
  const data = await chrome.storage.sync.get(SETTINGS_KEY);
  const stored = data[SETTINGS_KEY] || {};
  return {
    authManagerBaseUrl: stored.authManagerBaseUrl || DEFAULT_SETTINGS.authManagerBaseUrl,
    authManagerBearerToken:
      stored.authManagerBearerToken || DEFAULT_SETTINGS.authManagerBearerToken,
    relayEnabled:
      typeof stored.relayEnabled === "boolean"
        ? stored.relayEnabled
        : DEFAULT_SETTINGS.relayEnabled
  };
}

async function setRelayEnabled(enabled) {
  const settings = await getRelaySettings();
  const next = {
    ...settings,
    relayEnabled: enabled
  };
  await chrome.storage.sync.set({
    [SETTINGS_KEY]: next
  });
  if (!enabled) {
    await chrome.storage.session.remove(SESSION_KEY);
  }
  return next;
}

function normalizeBaseUrl(url) {
  return String(url || DEFAULT_SETTINGS.authManagerBaseUrl).trim().replace(/\/+$/, "");
}
