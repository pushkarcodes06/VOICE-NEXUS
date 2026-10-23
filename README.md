# VoiceNexus

A household assistant built on two open standards:

- **MCP over Streamable HTTP** (spec 2025-11-25): one endpoint, `/mcp`, handling POST, GET and DELETE.
- **Agent Skills**: each capability is a `skills/<name>/SKILL.md` folder (name and description frontmatter). The server loads them and serves them as `skill://<name>/SKILL.md` MCP resources, and the web app builds its suggestions and skill cards from them.

It is a simulation. Devices, weather and music are fake, and the web app understands commands with simple rules rather than an AI model.

## Run it

Needs Node 18 or newer and nothing else (no `npm install`).

```bash
node server.mjs            # http://127.0.0.1:8787
```

Open http://127.0.0.1:8787 and try "set a timer for 10 seconds", "add milk to my shopping list", "dim the bedroom light to 30", "what's the weather in Mumbai", "play some jazz".

Tests (server running):

```bash
node test/conformance.mjs   # 41 protocol checks over raw HTTP
node test/browser.mjs       # 36 end-to-end checks in headless Chromium (needs Playwright)
```

## What is where

| File | Purpose |
|---|---|
| `server.mjs` | MCP server: transport, sessions, 12 tools, resources, timer push |
| `skills/*/SKILL.md` | Five Agent Skills: timers, lists, smart-home, weather, music |
| `public/` | Web app (MCP client built on `fetch`) |
| `test/` | Conformance and browser tests |

## How the transport is used

- `initialize` returns an `Mcp-Session-Id`. The client sends it, plus `MCP-Protocol-Version`, on every later request.
- Notifications and client responses get `202 Accepted`. `tools/call` answers with a `text/event-stream` (a primed event, a log notification, then the result). Other requests answer with plain JSON.
- `GET /mcp` is the server-push stream. Timer completion arrives there as `notifications/message`, and state changes as `notifications/resources/updated` for `nexus://state`, which the UI uses to refresh.
- `DELETE /mcp` ends the session. An unknown session returns 404 and the client re-initializes.
- Security: binds to 127.0.0.1, validates both the `Host` and `Origin` headers (DNS rebinding protection), rejects batches, and limits body size. There is no authentication, so do not expose it to a network as is. For another host or origin set `ALLOWED_HOSTS` and `ALLOWED_ORIGINS`.

## Limits

- Not implemented: stream resumption (`Last-Event-ID` is sent but not replayed), authentication, MCP Apps UI resources.
- The transport is hand-written because the npm registry could not be reached while building. Swapping in the official TypeScript SDK would be straightforward.
- State lives in memory and resets when the server restarts.
