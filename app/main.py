from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .accounts import list_profiles
from .codex_switch import CodexSwitchError, save_label
from .config import settings

app = FastAPI(title="Codex Auth Manager", version="0.1.0")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_render_index())


@app.get("/ui")
async def ui() -> HTMLResponse:
    return HTMLResponse(_render_index())


@app.get("/oauth/callback")
async def oauth_callback(request: Request) -> JSONResponse:
    received = dict(request.query_params)
    stored_at = _store_callback(received)
    return JSONResponse(
        {
            "received": received,
            "stored_at": str(stored_at),
            "next": "POST /auth/exchange with code + code_verifier (optional)",
        }
    )


@app.post("/oauth/callback")
async def oauth_callback_post(payload: dict[str, Any]) -> JSONResponse:
    stored_at = _store_callback(payload)

    label = payload.get("label")
    auth_json = payload.get("auth_json")

    if label and auth_json:
        _persist_auth_and_save(label, auth_json)
        return JSONResponse(
            {
                "stored_at": str(stored_at),
                "saved_label": label,
                "message": "Auth saved and codex-switch profile updated.",
            }
        )

    return JSONResponse(
        {
            "stored_at": str(stored_at),
            "message": "Callback captured. To save, POST /auth/save.",
        }
    )


@app.get("/auth/callback")
async def auth_callback(request: Request) -> JSONResponse:
    return await oauth_callback(request)


@app.post("/auth/callback")
async def auth_callback_post(payload: dict[str, Any]) -> JSONResponse:
    return await oauth_callback_post(payload)


@app.post("/auth/save")
async def save_auth(payload: dict[str, Any]) -> JSONResponse:
    label = payload.get("label")
    auth_json = payload.get("auth_json")

    if not label or not auth_json:
        raise HTTPException(status_code=400, detail="label and auth_json are required")

    _persist_auth_and_save(label, auth_json)

    return JSONResponse({"saved_label": label, "message": "Auth saved."})


@app.post("/auth/exchange")
async def exchange_code(payload: dict[str, Any]) -> JSONResponse:
    code = payload.get("code")
    code_verifier = payload.get("code_verifier")
    label = payload.get("label")
    redirect_uri = payload.get("redirect_uri") or settings.openai_redirect_uri

    if not code or not code_verifier:
        raise HTTPException(
            status_code=400, detail="code and code_verifier are required"
        )

    token_response = await _exchange_code_for_token(code, code_verifier, redirect_uri)

    stored_at = _store_callback(
        {
            "type": "token_response",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "token_response": token_response,
        }
    )

    if label:
        _persist_auth_and_save(label, token_response)
        return JSONResponse(
            {
                "stored_at": str(stored_at),
                "saved_label": label,
                "token_response": token_response,
            }
        )

    return JSONResponse({"stored_at": str(stored_at), "token_response": token_response})


@app.get("/api/accounts")
async def api_accounts(request: Request) -> JSONResponse:
    _require_internal_auth(request)
    profiles = list_profiles()
    results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=10) as client:
        for profile in profiles:
            if profile.access_token:
                rate_info = await _fetch_rate_limits(client, profile.access_token)
            else:
                rate_info = {"error": "No access token found"}

            results.append(
                {
                    "label": profile.label,
                    "email": profile.email,
                    "rate_limits": rate_info,
                }
            )

    return JSONResponse({"accounts": results})


@app.get("/internal/auths")
async def internal_auths(request: Request, label: str | None = None) -> JSONResponse:
    _require_internal_auth(request)
    profiles = list_profiles()

    if label:
        for profile in profiles:
            if profile.label == label:
                return JSONResponse(
                    {"label": profile.label, "auth_json": profile.auth}
                )
        raise HTTPException(status_code=404, detail="Label not found")

    return JSONResponse(
        {
            "accounts": [
                {"label": profile.label, "auth_json": profile.auth}
                for profile in profiles
            ]
        }
    )


def _persist_auth_and_save(label: str, auth_json: Any) -> None:
    auth_path = settings.codex_auth_file()
    _write_json(auth_path, auth_json)

    try:
        save_label(label)
    except CodexSwitchError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _store_callback(payload: Any) -> Path:
    callback_dir = settings.callback_dir()
    callback_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"callback-{timestamp}.json"
    path = callback_dir / filename
    _write_json(path, payload)
    return path


async def _exchange_code_for_token(
    code: str, code_verifier: str, redirect_uri: str | None
) -> dict[str, Any]:
    if not settings.openai_token_url or not settings.openai_client_id:
        raise HTTPException(
            status_code=400,
            detail="OPENAI_TOKEN_URL and OPENAI_CLIENT_ID must be configured",
        )

    data: dict[str, str] = {
        "grant_type": "authorization_code",
        "client_id": settings.openai_client_id,
        "code": code,
        "code_verifier": code_verifier,
    }

    if redirect_uri:
        data["redirect_uri"] = redirect_uri

    if settings.openai_client_secret:
        data["client_secret"] = settings.openai_client_secret

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            settings.openai_token_url,
            data=data,
            headers={"Accept": "application/json"},
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text.strip() or "Token exchange failed",
        )

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(status_code=500, detail="Invalid token response") from exc


def _require_internal_auth(request: Request) -> None:
    if not settings.internal_api_token:
        return
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    token = auth_header.split(" ", 1)[1].strip()
    if token != settings.internal_api_token:
        raise HTTPException(status_code=403, detail="Invalid token")


async def _fetch_rate_limits(
    client: httpx.AsyncClient, token: str
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"}
    if settings.openai_organization:
        headers["OpenAI-Organization"] = settings.openai_organization
    if settings.openai_project:
        headers["OpenAI-Project"] = settings.openai_project

    try:
        response = await client.get(settings.rate_limit_probe_url, headers=headers)
    except httpx.RequestError as exc:
        return {"error": str(exc)}

    rate_headers = {
        key.lower(): value
        for key, value in response.headers.items()
        if key.lower().startswith("x-ratelimit-")
    }

    requests_remaining = _parse_int(rate_headers.get("x-ratelimit-remaining-requests"))
    requests_limit = _parse_int(rate_headers.get("x-ratelimit-limit-requests"))
    tokens_remaining = _parse_int(rate_headers.get("x-ratelimit-remaining-tokens"))
    tokens_limit = _parse_int(rate_headers.get("x-ratelimit-limit-tokens"))

    return {
        "status": response.status_code,
        "requests": _format_limit(
            requests_remaining,
            requests_limit,
            rate_headers.get("x-ratelimit-reset-requests"),
        ),
        "tokens": _format_limit(
            tokens_remaining,
            tokens_limit,
            rate_headers.get("x-ratelimit-reset-tokens"),
        ),
        "raw_headers": rate_headers,
        "error": response.text.strip() if response.status_code >= 400 else None,
    }


def _format_limit(
    remaining: int | None, limit: int | None, reset: str | None
) -> dict[str, Any] | None:
    if remaining is None and limit is None and reset is None:
        return None
    percent = None
    if remaining is not None and limit:
        percent = round((remaining / limit) * 100, 1)
    return {
        "remaining": remaining,
        "limit": limit,
        "percent": percent,
        "reset": reset,
    }


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _render_index() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Codex Auth Manager</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&display=swap" rel="stylesheet" />
    <style>
      :root {
        --bg: #0b0e12;
        --panel: #171b22;
        --panel-2: #1f2430;
        --text: #f4f6fb;
        --muted: #9aa4b2;
        --accent: #86e1c0;
        --accent-2: #6aa8ff;
        --border: rgba(255,255,255,0.08);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "Space Grotesk", sans-serif;
        color: var(--text);
        background: radial-gradient(circle at 20% 10%, rgba(106,168,255,0.15), transparent 40%),
                    radial-gradient(circle at 80% 20%, rgba(134,225,192,0.18), transparent 45%),
                    var(--bg);
        min-height: 100vh;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 32px 18px;
      }
      .shell {
        width: min(960px, 100%);
        display: grid;
        gap: 20px;
      }
      header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
      }
      h1 {
        font-size: 28px;
        margin: 0;
      }
      .subtitle {
        color: var(--muted);
        font-size: 14px;
      }
      .token-box {
        display: flex;
        gap: 8px;
        align-items: center;
        background: var(--panel);
        border: 1px solid var(--border);
        padding: 10px 12px;
        border-radius: 12px;
      }
      .token-box input {
        background: transparent;
        border: none;
        color: var(--text);
        font-size: 13px;
        width: 160px;
        outline: none;
      }
      .token-box button {
        background: var(--panel-2);
        color: var(--text);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 6px 10px;
        font-size: 12px;
        cursor: pointer;
      }
      .grid {
        display: grid;
        gap: 14px;
      }
      .card {
        background: var(--panel);
        border-radius: 18px;
        border: 1px solid var(--border);
        padding: 16px;
        box-shadow: 0 18px 30px rgba(0,0,0,0.35);
      }
      .card-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 10px;
      }
      .label {
        font-size: 16px;
        font-weight: 600;
      }
      .email {
        font-size: 12px;
        color: var(--muted);
      }
      .pill {
        padding: 4px 10px;
        border-radius: 999px;
        background: var(--panel-2);
        font-size: 12px;
        color: var(--muted);
      }
      .limit-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 8px 0;
        border-top: 1px solid var(--border);
      }
      .limit-row:first-of-type { border-top: none; }
      .limit-title {
        font-size: 14px;
        font-weight: 500;
      }
      .limit-sub {
        font-size: 12px;
        color: var(--muted);
      }
      .percent {
        font-size: 14px;
        font-weight: 600;
        color: var(--accent);
      }
      .percent.alt { color: var(--accent-2); }
      .empty {
        color: var(--muted);
        font-size: 14px;
      }
      .status {
        font-size: 12px;
        color: var(--muted);
        text-align: right;
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <header>
        <div>
          <h1>Codex Auth Manager</h1>
          <div class="subtitle">Connected accounts and rate limits remaining</div>
        </div>
        <div class="token-box">
          <input id="tokenInput" type="password" placeholder="Bearer token" />
          <button id="tokenSave" type="button">Save</button>
          <button id="tokenClear" type="button">Clear</button>
        </div>
      </header>
      <div id="accounts" class="grid"></div>
      <div id="status" class="status"></div>
    </div>
    <script>
      const accountsEl = document.getElementById("accounts");
      const statusEl = document.getElementById("status");
      const tokenInput = document.getElementById("tokenInput");

      function setStatus(text) {
        statusEl.textContent = text || "";
      }

      function renderLimit(label, data, extraClass) {
        if (!data) {
          return `<div class="limit-row"><div><div class="limit-title">${label}</div><div class="limit-sub">No data</div></div><div class="percent ${extraClass}">--</div></div>`;
        }
        const percent = data.percent !== null && data.percent !== undefined ? data.percent + "%" : "--";
        const remaining = data.remaining ?? "--";
        const limit = data.limit ?? "--";
        const reset = data.reset ? `Reset ${data.reset}` : "Reset unknown";
        return `<div class="limit-row"><div><div class="limit-title">${label}</div><div class="limit-sub">${remaining} / ${limit} • ${reset}</div></div><div class="percent ${extraClass}">${percent}</div></div>`;
      }

      function renderCard(account) {
        const email = account.email ? ` • ${account.email}` : "";
        const rate = account.rate_limits || {};
        return `<div class="card"><div class="card-head"><div><div class="label">${account.label}</div><div class="email">Rate limits remaining${email}</div></div><div class="pill">${rate.error ? "Error" : "Active"}</div></div>${renderLimit("Requests", rate.requests, "")}${renderLimit("Tokens", rate.tokens, "alt")}</div>`;
      }

      async function loadAccounts() {
        const token = localStorage.getItem("internalToken") || "";
        tokenInput.value = token;
        try {
          const res = await fetch("/api/accounts", {
            headers: token ? { "Authorization": "Bearer " + token } : {},
          });
          if (res.status === 401 || res.status === 403) {
            accountsEl.innerHTML = `<div class="card empty">Bearer token required.</div>`;
            setStatus("Provide INTERNAL_API_TOKEN to load accounts.");
            return;
          }
          const data = await res.json();
          const accounts = data.accounts || [];
          if (!accounts.length) {
            accountsEl.innerHTML = `<div class="card empty">No connected accounts found.</div>`;
            setStatus("No accounts found.");
            return;
          }
          accountsEl.innerHTML = accounts.map(renderCard).join("");
          setStatus(`Connected accounts: ${accounts.length}`);
        } catch (err) {
          accountsEl.innerHTML = `<div class="card empty">Failed to load accounts.</div>`;
          setStatus("Request failed.");
        }
      }

      document.getElementById("tokenSave").addEventListener("click", () => {
        localStorage.setItem("internalToken", tokenInput.value.trim());
        loadAccounts();
      });

      document.getElementById("tokenClear").addEventListener("click", () => {
        localStorage.removeItem("internalToken");
        tokenInput.value = "";
        loadAccounts();
      });

      loadAccounts();
      setInterval(loadAccounts, 20000);
    </script>
  </body>
</html>"""
