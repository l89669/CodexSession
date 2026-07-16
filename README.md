# Codex Session Context

A Codex plugin that keeps an agent's effective inputs recoverable across context compression and parent-to-agent task handoff.

The plugin combines two components:

- `skills/context-advanced-management/` defines the recovery and token-backed handoff workflow.
- `mcp/` indexes local Codex session JSONL into SQLite and exposes the `codex_session_*` tools.

## Build

Node.js 20 or newer is required. Build and test the MCP before installing the plugin from a local marketplace source:

```bash
cd mcp
npm ci
npm test
```

The plugin MCP manifest uses paths relative to the plugin root, so the built repository can be copied or cached at any location.

## Handoff workflow

`codex_session_publish_task` stores an exact instruction and returns a ready-to-send `prompt`. Send that prompt verbatim to the receiving agent. The agent retrieves the instruction with `codex_session_get_task`; later instruction updates use the same publish-and-prompt flow.

Successful task retrievals are indexed as `published_task_retrieval` inputs. After compression, `codex_session_recent_user_inputs` recovers both ordinary user messages and retrieved agent instructions without conflating their types.

## MCP tools

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

## Storage and safety

Session JSONL files are read-only source data. The MCP writes only its SQLite mirror and lease state under `~/.codex/session-mcp/` by default. Set `CODEX_HOME` or `CODEX_SESSION_MCP_DB` to override the default locations.

Standalone MCP development details are in [`mcp/README.md`](mcp/README.md).
