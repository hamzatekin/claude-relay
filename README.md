# claude-relay

`claude -p` over HTTPS at **`https://ai.hamzatekin.dev`**, so the notes app (Cloudflare Worker)
can use the owner's Claude subscription. **Personal use only**: a subscription token must not
serve other people's requests. If the app ever gets other users, swap in an API key.

## API

`GET /health` → `{"ok":true}` (no auth)

`POST /v1/complete` with `Authorization: Bearer <RELAY_TOKEN>`:

| field | type | default |
|---|---|---|
| `prompt` | string, required | — |
| `system` | string | built-in "notes assistant, reply with only the result" prompt |
| `model` | `sonnet` / `opus` / `haiku` / full id | `sonnet` |
| `effort` | `low` / `medium` / `high` / `xhigh` / `max` | `low` |
| `json_schema` | JSON Schema object → parsed result in `structured` | — |
| `stream` | bool → SSE: `delta` `{text}` … then `done` `{…}` or `error` `{error}` | `false` |

Response: `{"text", "structured"?, "model", "duration_ms", "usage"}`. Errors are
`{"error": "..."}` with 400 / 401 / 413 / 429 (queue full) / 502 (claude failed) / 504 (timeout).

Claude runs with **all tools disabled** (`--tools ""`), no MCP, no settings files, no session
persistence, as a non-root user in an empty directory. Note content can't make it do anything;
the worst a prompt injection can do is change the text it returns.

## POST /v1/chat

Multi-step agent runs: one long-lived `claude` process per session, so each step doesn't
respawn Claude or resend the conversation. Same Bearer auth and body limit as `/v1/complete`.

Body `{"session": string | null, "prompt": string, "system"?, "model"?, "effort"?, "json_schema"?}`.
`null` (or missing) starts a new session; a session id sends the next message in the same process.
`system` / `model` / `effort` / `json_schema` take the same values and defaults as on
`/v1/complete`, but only when starting a session (with a session id they are a 400). Response:
`{"session", "text", "structured"?, "model", "duration_ms", "usage"}` (`duration_ms` and `usage`
are per turn). With `json_schema`, **every** reply in the session carries a validated `structured`
object — read that, not `text`; prompting for "JSON only" is not enough (the model can append a
second, corrected object to `text`, which breaks `JSON.parse`).
Same lockdown as `/v1/complete`.

| status | meaning |
|---|---|
| 400 / 401 / 413 | bad body (empty prompt, bad JSON, tuning fields on an existing session) / no token / body too large |
| 409 | `session busy`: a turn is already in flight on that session |
| 410 | `session expired`: unknown, idle-killed, evicted, turn-capped or dead. Start over with the full conversation |
| 502 | claude reported an error (session stays alive) or the process died |
| 503 | `too many chat sessions`: all slots are mid-turn. Back off and retry |
| 504 | turn timed out (session is killed) |

Limits (optional env, each an integer >= 1 or the server refuses to start): `CHAT_IDLE_MS`
(300000, kill after 5 min without a message), `CHAT_MAX_SESSIONS` (4; a new session evicts the
least-recently-used *idle* one, never a busy one), `CHAT_TURN_TIMEOUT_MS` (90000, half of
`/v1/complete`'s; `opus` or high `effort` turns can hit it, and a 504 ends the session),
`CHAT_MAX_TURNS` (30; the session is killed after that many replies, so the next message is a
410). A client disconnect mid-turn kills the session. Chat sessions don't use the
`/v1/complete` concurrency queue. `system` / `model` / `effort` / `json_schema` belong to the session, so send
them again on every `session: null` request, restarts after a 410 included.

```ts
// Worker side: keep a session per agent run. The history holds both sides so a restart
// (410, or a session that hit the turn cap) resumes with the whole conversation.
type ChatState = { session: string | null; history: string[] }

async function chatStep(env: { CLAUDE_RELAY_TOKEN: string }, state: ChatState, message: string) {
  state.history.push(`User: ${message}`)
  for (let attempt = 0; attempt < 4; attempt++) {
    const prompt = state.session ? message : state.history.join("\n\n") // new session gets everything so far
    const res = await fetch("https://ai.hamzatekin.dev/v1/chat", {
      method: "POST",
      headers: { authorization: `Bearer ${env.CLAUDE_RELAY_TOKEN}`, "content-type": "application/json" },
      body: JSON.stringify({ session: state.session, prompt }),
    })
    const raw = await res.text() // an HTML 502 from the proxy isn't JSON
    let body: { session?: string; text?: string; error?: string } = {}
    try { body = JSON.parse(raw) } catch { /* keep {} */ }

    if (res.status === 410) { state.session = null; continue }
    if (res.status === 503) { await new Promise((r) => setTimeout(r, 2000 * (attempt + 1))); continue }
    if (!res.ok || !body.session || body.text === undefined) {
      state.history.pop() // don't replay an unanswered message on the next restart
      throw new Error(`claude-relay ${res.status}: ${body.error ?? raw.slice(0, 200)}`)
    }
    state.session = body.session
    state.history.push(`Assistant: ${body.text}`)
    return body.text
  }
  state.history.pop()
  throw new Error("claude-relay: gave up after repeated 410/503")
}
```

## Calling it from the Worker

```bash
wrangler secret put CLAUDE_RELAY_TOKEN   # value: cat ~/dev/claude-relay/.relay-token on the box
```

```ts
export async function askClaude(
  env: { CLAUDE_RELAY_TOKEN: string },
  prompt: string,
  opts: { system?: string; model?: string; effort?: string; json_schema?: object } = {},
) {
  const res = await fetch("https://ai.hamzatekin.dev/v1/complete", {
    method: "POST",
    headers: { authorization: `Bearer ${env.CLAUDE_RELAY_TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify({ prompt, ...opts }),
  })
  const body = await res.json<{ text?: string; structured?: unknown; error?: string }>()
  if (!res.ok) throw new Error(`claude-relay ${res.status}: ${body.error}`)
  return body
}

// Streaming: forward the SSE body straight to the browser.
// const upstream = await fetch(..., { body: JSON.stringify({ prompt, stream: true }) })
// return new Response(upstream.body, { headers: { "content-type": "text/event-stream" } })
```

Keep the token in the Worker. Never send it to the browser.

## Operating it

- **Deploys:** push to `main`. Coolify (app id **8**, project `claude-relay`) builds the
  `Dockerfile` from this repo via the `avvos-hels` GitHub App webhook. Don't edit the build in
  the Coolify UI; change it here. `~/coolify-deploy.sh relay` triggers a deploy without a push.
- **Secrets** (Coolify env, runtime-only, never build args): `RELAY_TOKEN` (copy in
  `.relay-token`, 0600) and `CLAUDE_CODE_OAUTH_TOKEN`.
- **Tunables** (optional env): `DEFAULT_MODEL`, `DEFAULT_EFFORT`, `MAX_CONCURRENCY` (2),
  `MAX_QUEUE` (8), `TIMEOUT_MS` (180000), plus the `CHAT_*` ones above. Memory is capped at 2 GB in Coolify.
- **Updating Claude Code:** it's installed at build time, so run `~/coolify-deploy.sh relay --force`.
- **Local test build:** `docker build -t claude-relay:test .`
- **Logs:** one line per `/v1/complete` request or chat turn (model, sizes, tokens, timing;
  never content), plus chat lifecycle lines (started, idle-expired, evicted, died, turn cap):
  `docker logs $(docker ps -qf name=9tpmhecw3ano37mbdbhrxfdn)`
