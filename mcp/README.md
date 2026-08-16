# Codex Session MCP

Read-only MCP server for indexing local Codex session JSONL files and retrieving exact prior conversation context after context compression.

The server mirrors Codex session events into a local SQLite index. It does not modify Codex session source files.

Codex-facing processes remain stdio MCP servers, but they are lightweight proxies. Each proxy keeps one long-lived loopback HTTP connection to a shared backend. If no backend is listening, every concurrent proxy may start one child; fixed-port binding selects the single winner and the other children exit. The backend owns SQLite, indexing, file watching, and periodic append checks. A proxy reconnects and restores its MCP initialization after a backend restart. The backend exits after 60 seconds with no connected proxies.

## Features

- Locate the current Codex session with a tool-output marker token.
- Search exact prior messages, tool calls, tool outputs, and raw events.
- List active or archived Codex sessions without exposing local file paths.
- Keep a local SQLite mirror under `~/.codex/session-mcp/index.sqlite` by default.
- Pair with the plugin skill in `../skills/context-advanced-management/`.

## Requirements

- Node.js 20 or newer. Node.js 22 is recommended.
- npm.
- Local Codex session files under `~/.codex/sessions` or a custom `CODEX_HOME`.

## Install From GitHub

Clone and build:

```bash
git clone https://github.com/l89669/CodexSession.git
cd CodexSession/mcp
npm install
npm run build
```

Or install the cloned MCP package globally from the repository root:

```bash
cd ..
npm install -g ./mcp
```

The package install runs `npm run build` through the `prepare` script.

## Codex MCP Configuration

For a cloned checkout, add this to `~/.codex/config.toml` and adjust the path:

```toml
[mcp_servers.codex_session]
command = "node"
args = ["/absolute/path/to/CodexSession/mcp/dist/src/server.js"]
cwd = "/absolute/path/to/CodexSession/mcp"
enabled = true
startup_timeout_sec = 120
```

For a global npm install:

```toml
[mcp_servers.codex_session]
command = "codex-session-mcp"
args = []
enabled = true
startup_timeout_sec = 120
```

On Windows, a cloned checkout usually looks like:

```toml
[mcp_servers.codex_session]
command = "node"
args = ["C:\\Users\\you\\src\\CodexSession\\mcp\\dist\\src\\server.js"]
cwd = "C:\\Users\\you\\src\\CodexSession\\mcp"
enabled = true
startup_timeout_sec = 120
```

## Environment

- `CODEX_HOME`: Codex home directory. Defaults to the current user's `.codex` directory.
- `CODEX_SESSION_MCP_DB`: SQLite index path. Defaults to `~/.codex/session-mcp/index.sqlite`.
- `CODEX_SESSION_MCP_PORT`: loopback backend port. Defaults to `49321`.
- `CODEX_SESSION_MCP_IDLE_TIMEOUT_MS`: no-client lifetime before the backend exits. Defaults to `60000`.
- `CODEX_SESSION_MCP_STARTUP_TIMEOUT_MS`: maximum backend startup wait. Defaults to `20000`.

The health handshake verifies the service, bridge protocol, plugin version, and a hash of `CODEX_HOME` plus the index path before a proxy connects, so a same-port process for another configured index is never used accidentally.

## Companion Skill

The plugin root already includes the companion skill. For a standalone MCP setup, copy it into the Codex skills directory:

```bash
mkdir -p ~/.codex/skills
cp -R ../skills/context-advanced-management ~/.codex/skills/
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.codex\skills" | Out-Null
Copy-Item -Recurse -Force "..\skills\context-advanced-management" "$env:USERPROFILE\.codex\skills\"
```

## Tools

- `codex_session_status`
- `codex_session_sync`
- `codex_session_list`
- `codex_session_get_session_token`
- `codex_session_get_session_by_token`
- `codex_session_find_by_text`
- `codex_session_messages`
- `codex_session_recent_user_inputs`
- `codex_session_tool_calls`
- `codex_session_keyword_search`
- `codex_session_publish_task`
- `codex_session_get_task`

## Development

```bash
npm install
npm test
npm run simulate:mcp
```

Run a smoke test against the real local Codex home:

```bash
npm run smoke:real
```

## Safety

The server treats Codex session JSONL files as read-only source data. Runtime writes are limited to its SQLite mirror and published task records under the configured session MCP storage path.
