---
name: context-advanced-management
description: Keep an agent's effective inputs recoverable across context boundaries. Reconstruct working context after compression from local Codex session history, and use token-backed task handoff to make a parent-published task recoverable as agent input in the child session.
---

# Context Advanced Management

Use the `codex_session_*` MCP tools as a local transcript and delegated-task database. Retrieve only the amount of history needed to satisfy the user's current instruction.

## Delegated Task Handoff

1. The parent agent calls `codex_session_publish_task` with the complete instruction and receives `data.token`, `data.prompt`, and `data.comment`.
2. The parent agent sends `data.prompt` verbatim as the complete agent message. Its fixed format is: `token: <token>, use codex_session_get_task tool to retrieve exact instruction`.
3. The sub-agent calls `codex_session_get_task` and performs the returned task.
4. Every later instruction update sent to the agent, including through `send_message` or `followup_task`, uses the same handoff: publish the complete new instruction with `codex_session_publish_task`, then send its returned `data.prompt` verbatim.
5. After context compression, the sub-agent locates its current session through the Compression Recovery Workflow, calls `codex_session_recent_user_inputs`, and recovers the task from the most recent `published_task_retrieval` record.

## Compression Recovery Workflow

1. If the user asks for a specific historical phrase, reply, or decision, search for that phrase first. Do not run a broad recovery workflow unless the specific lookup is insufficient.
2. If the current `session_id` is already known, use it directly.
3. If the current `session_id` is unknown and the session is still active, call `codex_session_get_session_token`, then call `codex_session_get_session_by_token` with the returned `token`.
4. If token lookup returns `pending`, retry once after a short delay. If it still cannot locate the session, say that the current session is not indexed yet instead of guessing.
5. Use `codex_session_find_by_text` only as a fallback for older sessions or when token lookup is unavailable. It searches ordinary messages, published task text, and published task tokens; inspect the returned `input_type` to distinguish `published_task_retrieval` from messages. Prefer a distinctive original snippet of 20-300 characters. If it returns `ambiguous` or `not_found`, use a better snippet or ask for a narrower time/session clue.
6. After locating the right session, choose the narrowest useful query:
   - latest inputs: `codex_session_recent_user_inputs` with a small `limit`. Read `data.inputs` from newest to oldest:
     - `input_type: user_message` is an ordinary user message. Its text is in `content_text`.
     - `input_type: published_task_retrieval` is an agent input created when that session retrieves a task through `codex_session_get_task`. Its text is in `task`.
   - exact phrase or constraint: `codex_session_keyword_search`.
   - nearby transcript: `codex_session_messages`.
   - implementation/debug evidence: `codex_session_tool_calls`.
7. When a search result has a useful `sequence`, use `codex_session_messages` to retrieve the surrounding transcript. Treat `around_sequence` as a nearby-window helper, not a guarantee of exactly N prior/next messages. If the continuation looks incomplete, repeat with a wider `index_from`/`index_to` range.

## Rules

- Treat the tools as a database, not as an agent or summarizer.
- Preserve exact wording for user constraints, decisions, and requested historical replies.
- When the user gives a clear, narrow instruction, answer that instruction directly after retrieval. Do not add a full project-status report or implementation commentary unless asked.
- Do not use archived sessions unless the task explicitly needs them; default `archive_scope` is `active`.
- Do not request raw JSON unless schema/debugging detail is needed.
- Keep result sizes bounded with `limit` and `max_chars`. For `codex_session_recent_user_inputs`, `max_chars` limits both `content_text` and `task`; set it high enough when the complete delegated task is required.
- If multiple matches appear during session location, consider the locator failed and use a better snippet.
