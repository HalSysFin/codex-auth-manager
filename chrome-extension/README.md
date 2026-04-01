# Codex Auth Relay Chrome Extension

This extension captures the localhost OAuth callback used during Codex/OpenAI login and relays it back to the Auth Manager server.

It is intended to help when the login flow opens a browser tab that returns to a localhost callback URL and you want the callback handed back to Auth Manager instead of getting stuck in the browser.

## What It Does

- starts a relay-backed login flow from the browser
- watches for the localhost callback URL
- posts the callback payload to Auth Manager
- lets Auth Manager perform the full token exchange and saved-profile update server-side
- helps complete the Add Account / login relay flow without copying data manually every time

## Folder

This extension lives in:

`chrome-extension/`

## Install (Unpacked)

1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked**
4. Select the `chrome-extension/` folder from this repo

## Configure

1. Open the extension popup or Options page
2. Set **Auth Manager Base URL**
   - Example: `https://your-domain`
   - Local example: `http://localhost:8080`
3. If your server requires `INTERNAL_API_TOKEN`, set the **Internal API Bearer Token**
4. Save

## Normal Usage

1. Open the extension popup
2. Click **Start Relay Login**
3. Complete the browser login flow
4. The extension captures the localhost callback and sends it to:

   `POST /auth/relay-callback`

5. Auth Manager then continues the login/relay flow for that session

## If the Callback Is Returned But Not Finalized

If the browser lands on a localhost callback URL and nothing happens:

1. Copy the full callback URL
2. Open Auth Manager
3. Use **Add Account**
4. Paste the callback URL into the modal
5. Submit it so Auth Manager can relay/finalize it manually

## Important: Localhost Port Conflicts

If VS Code or another app is already listening on localhost callback ports such as `1445` or `1455`, the auth relay can fail or hang.

Common symptoms:

- the auth flow hangs
- the browser shows a localhost callback URL but the relay does not complete
- the callback is captured but Auth Manager does not finalize the profile

Best practice:

- close or disable tools that may intercept localhost auth callbacks before starting relay login
- especially stop VS Code auth-related flows if they are using the same localhost callback ports

If there is a conflict:

- stop the conflicting app
- retry **Start Relay Login**
- or paste the returned callback URL into **Add Account** in Auth Manager

## Shortcuts

- Open popup: `Ctrl+Shift+Y` (`Command+Shift+Y` on macOS)
- Start relay login: `Ctrl+Shift+L` (`Command+Shift+L` on macOS)

## Related Server Endpoints

The extension is designed to work with these Auth Manager endpoints:

- `POST /auth/login/start-relay`
- `POST /auth/relay-callback`
- `GET /auth/login/status`

## Notes

- This extension does not store your saved auth profiles itself; Auth Manager does that.
- The extension is only responsible for capturing and relaying the browser callback during login.
- If your backend is protected, make sure the configured bearer token matches your server settings.
