import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parseSessionFile } from "../src/parser.js";
import { truncateText } from "../src/util.js";
import {
  createFixtureHome,
  openFixture,
  samplePath,
  writeMiniSession,
  writeRecoverySession,
  writeTaskInputSession
} from "./test-fixtures.js";

test("indexes the committed session fixture", async (t) => {
  const env = createFixtureHome(t);
  const parsed = parseSessionFile(samplePath);
  assert.deepEqual(
    {
      session_id: parsed.meta.id,
      raw_events: parsed.rawEvents.length,
      messages: parsed.messages.length,
      tool_calls: parsed.toolCalls.length
    },
    { session_id: "sample-session-001", raw_events: 6, messages: 3, tool_calls: 1 }
  );
  fs.copyFileSync(samplePath, path.join(env.activeDir, "current.jsonl"));
  fs.writeFileSync(
    path.join(env.codexHome, "session_index.jsonl"),
    `${JSON.stringify({ id: parsed.meta.id, thread_name: "redacted sample", updated_at: "2026-06-07T10:00:00.000Z" })}\n`,
    "utf8"
  );
  const { queries, indexer } = openFixture(env);

  const sync = await indexer.sync({ rebuild: true, force: true });
  assert.deepEqual(
    {
      files_seen: sync.files_seen,
      files_indexed: sync.files_indexed,
      events_indexed: sync.events_indexed,
      messages_indexed: sync.messages_indexed,
      tool_calls_indexed: sync.tool_calls_indexed
    },
    {
      files_seen: 1,
      files_indexed: 1,
      events_indexed: 6,
      messages_indexed: 3,
      tool_calls_indexed: 1
    }
  );

  const listed = await queries.listSessions({ archive_scope: "active" });
  assert.deepEqual((listed.data as any).sessions, [
    {
      id: parsed.meta.id,
      thread_name: "redacted sample",
      cwd: parsed.meta.cwd,
      updated_at: "2026-06-07T10:00:00.000Z",
      archive_scope: "active"
    }
  ]);
});

test("compression recovery locates a session and returns its latest effective inputs", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "context-recovery-session";
  writeRecoverySession(path.join(env.activeDir, "recovery.jsonl"), sessionId);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const located = await queries.findByText({
    text: "A new MCP process failed with SqliteError: database is locked.",
    archive_scope: "active"
  });
  assert.equal(located.status, "ok");
  assert.equal((located.data as any).session_id, sessionId);
  assert.equal((located.data as any).match.input_type, "user_message");

  const recent = await queries.recentUserInputs({ session_id: sessionId, limit: 3 });
  assert.deepEqual(
    (recent.data as any).inputs.map((input: any) => input.content_text),
    [
      "The tool stalled; inspect the lock handling.",
      "A new MCP process failed with SqliteError: database is locked.",
      "PLEASE IMPLEMENT THIS PLAN:\nBuild the local session index."
    ]
  );
});

test("context drill-down queries preserve filtering and raw-data boundaries", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "context-drill-down-session";
  writeRecoverySession(path.join(env.activeDir, "recovery.jsonl"), sessionId);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const around = await queries.messages({
    session_id: sessionId,
    around_sequence: 4,
    before_count: 1,
    after_count: 3,
    order: "asc"
  });
  assert.deepEqual(
    (around.data as any).messages.map((message: any) => ({ sequence: message.sequence, content_text: message.content_text })),
    [
      { sequence: 3, content_text: "PLEASE IMPLEMENT THIS PLAN:\nBuild the local session index." },
      { sequence: 4, content_text: "A new MCP process failed with SqliteError: database is locked." },
      { sequence: 7, content_text: "The tool stalled; inspect the lock handling." }
    ]
  );
  assert.equal((around.data as any).messages.some((message: any) => "raw_json" in message && message.raw_json !== undefined), false);

  const aroundRaw = await queries.messages({
    session_id: sessionId,
    around_sequence: 4,
    before_count: 0,
    after_count: 0,
    include_raw: true
  });
  assert.equal(typeof (aroundRaw.data as any).messages[0].raw_json, "string");

  const calls = await queries.toolCalls({
    session_id: sessionId,
    tool_name_contains: "shell_command",
    keyword: "database is locked",
    has_output: true,
    limit: 5
  });
  assert.deepEqual(
    (calls.data as any).tool_calls.map((call: any) => ({ call_id: call.call_id, tool_name: call.tool_name, output_text: call.output_text })),
    [
      {
        call_id: `${sessionId}-lock-check`,
        tool_name: "functions.shell_command",
        output_text: "SqliteError: database is locked; connection closed before initialize response"
      }
    ]
  );
});

test("keyword search reports the literal keywords matched by each result", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "keyword-search-session";
  writeRecoverySession(path.join(env.activeDir, "recovery.jsonl"), sessionId);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const result = await queries.keywordSearch({
    session_id: sessionId,
    keywords: ["Remember", "database is locked", "not present"],
    limit: 10
  });
  assert.deepEqual(
    (result.data as any).results.map((row: any) => ({ sequence: row.sequence, matched_keywords: row.matched_keywords })),
    [
      { sequence: 4, matched_keywords: ["database is locked"] },
      { sequence: 2, matched_keywords: ["Remember"] }
    ]
  );
});

test("find_by_text returns exact candidates when a message is not unique", async (t) => {
  const env = createFixtureHome(t);
  writeMiniSession(path.join(env.activeDir, "one.jsonl"), "session-one", "same locator phrase appears here");
  writeMiniSession(path.join(env.activeDir, "two.jsonl"), "session-two", "same locator phrase appears here");
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const withoutCandidates = await queries.findByText({ text: "same locator phrase", archive_scope: "active" });
  assert.deepEqual(
    { status: withoutCandidates.status, match_count: (withoutCandidates as any).match_count, hasCandidates: "candidates" in withoutCandidates },
    { status: "ambiguous", match_count: 2, hasCandidates: false }
  );

  const withCandidates = await queries.findByText({
    text: "same locator phrase appears here",
    archive_scope: "active",
    include_candidates: true
  });
  assert.equal(withCandidates.status, "ambiguous");
  assert.deepEqual(
    ((withCandidates as any).candidates as any[]).map((candidate) => candidate.session_id).sort(),
    ["session-one", "session-two"]
  );
});

test("recent user inputs merge task retrievals without masquerading as messages", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "task-input-session";
  const token = "11111111-1111-4111-8111-111111111111";
  const task = `  Execute this delegated task.\n${"x".repeat(4_500)}\nPreserve **Markdown** exactly.  `;
  writeTaskInputSession(path.join(env.activeDir, "task-input.jsonl"), sessionId, token, task);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const recent = await queries.recentUserInputs({ session_id: sessionId, limit: 3, max_chars: 20 });
  assert.deepEqual((recent.data as any).inputs, [
    {
      sequence: 4,
      timestamp: "2026-06-07T00:00:03.000Z",
      input_type: "user_message",
      role: "user",
      content_text: truncateText("ordinary input after task", 20)
    },
    {
      sequence: 3,
      timestamp: "2026-06-07T00:00:02.000Z",
      input_type: "published_task_retrieval",
      token,
      task: truncateText(task, 20),
      call_id: `${sessionId}-get-task`,
      tool_name: "codex_session_get_task"
    },
    {
      sequence: 2,
      timestamp: "2026-06-07T00:00:01.000Z",
      input_type: "user_message",
      role: "user",
      content_text: truncateText("ordinary input before task", 20)
    }
  ]);

  const recentWithRaw = await queries.recentUserInputs({
    session_id: sessionId,
    limit: 1,
    max_chars: 20,
    include_raw: true
  });
  const rawInput = (recentWithRaw.data as any).inputs[0];
  assert.deepEqual(Object.keys(rawInput).sort(), ["content_text", "input_type", "raw_json", "role", "sequence", "timestamp"].sort());
  assert.equal(typeof rawInput.raw_json, "string");
});

test("find_by_text locates published tasks by text and token", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "task-search-session";
  const token = "11111111-1111-4111-8111-111111111111";
  const task = "Execute the delegated task while preserving **Markdown** exactly.";
  writeTaskInputSession(path.join(env.activeDir, "task-input.jsonl"), sessionId, token, task);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  for (const text of ["preserving **Markdown** exactly", token]) {
    const located = await queries.findByText({ text, archive_scope: "active", max_chars: 80 });
    assert.equal(located.status, "ok");
    assert.equal((located.data as any).session_id, sessionId);
    assert.deepEqual(
      {
        input_type: (located.data as any).match.input_type,
        token: (located.data as any).match.token,
        call_id: (located.data as any).match.call_id,
        tool_name: (located.data as any).match.tool_name,
        hasLegacyMessage: "message" in (located.data as any)
      },
      {
        input_type: "published_task_retrieval",
        token,
        call_id: `${sessionId}-get-task`,
        tool_name: "codex_session_get_task",
        hasLegacyMessage: false
      }
    );
  }
});

test("find_by_text collapses repeated retrievals of the same published task", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "repeated-task-input-session";
  const token = "22222222-2222-4222-8222-222222222222";
  const task = "Repeatable delegated instruction with one distinctive recovery phrase.";
  writeTaskInputSession(path.join(env.activeDir, "task-input.jsonl"), sessionId, token, task, true);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  for (const text of ["one distinctive recovery phrase", token]) {
    const located = await queries.findByText({ text, archive_scope: "active" });
    assert.equal(located.status, "ok");
    assert.deepEqual(
      {
        session_id: (located.data as any).session_id,
        input_type: (located.data as any).match.input_type,
        token: (located.data as any).match.token,
        occurrences: (located.data as any).match.occurrences,
        sequence: (located.data as any).match.sequence,
        call_id: (located.data as any).match.call_id
      },
      {
        session_id: sessionId,
        input_type: "published_task_retrieval",
        token,
        occurrences: 2,
        sequence: 5,
        call_id: `${sessionId}-get-task-repeat`
      }
    );
  }
});
