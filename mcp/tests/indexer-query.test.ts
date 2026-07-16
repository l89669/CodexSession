import assert from "node:assert/strict";
import Database from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { openDatabase } from "../src/db.js";
import { CodexSessionIndexer } from "../src/indexer.js";
import { parseSessionFile } from "../src/parser.js";
import { resolveRuntimePaths } from "../src/paths.js";
import { CodexSessionQueries } from "../src/query.js";
import { ensureDir, truncateText } from "../src/util.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(here, "../..");
const samplePath = path.join(projectRoot, "samples", "current-session.redacted.jsonl");

test("indexes redacted current session sample and supports core queries", async () => {
  assert.ok(fs.existsSync(samplePath), "missing committed synthetic sample fixture");
  const env = createFixtureHome();
  const parsed = parseSessionFile(samplePath);
  fs.copyFileSync(samplePath, path.join(env.activeDir, "current.jsonl"));
  fs.writeFileSync(
    path.join(env.codexHome, "session_index.jsonl"),
    `${JSON.stringify({ id: parsed.meta.id, thread_name: "redacted sample", updated_at: "2026-06-07T10:00:00.000Z" })}\n`,
    "utf8"
  );

  const { queries, indexer } = openFixture(env.codexHome, env.dbPath);
  const sync = await indexer.sync({ rebuild: true, force: true });
  assert.equal(sync.files_seen, 1);
  assert.ok(sync.messages_indexed > 0);

  const listed = await queries.listSessions({ archive_scope: "active", limit: 5 });
  assert.equal(listed.status, "ok");
  const session = (listed.data as any).sessions[0];
  assert.equal(session.id, parsed.meta.id);
  assert.deepEqual(Object.keys(session).sort(), ["archive_scope", "cwd", "id", "thread_name", "updated_at"].sort());

  const recent = await queries.recentUserInputs({ session_id: parsed.meta.id, limit: 3 });
  assert.equal(recent.status, "ok");
  assert.ok((recent.data as any).inputs.length > 0);
  assert.equal((recent.data as any).inputs[0].input_type, "user_message");

  const toolCalls = await queries.toolCalls({ session_id: parsed.meta.id, limit: 5 });
  assert.equal(toolCalls.status, "ok");
  assert.ok((toolCalls.data as any).tool_calls.length > 0);

  const search = await queries.keywordSearch({ session_id: parsed.meta.id, keywords: ["记住", "工具", "session"], limit: 20 });
  assert.equal(search.status, "ok");
  assert.ok((search.data as any).results.length > 0);
  assert.ok((search.data as any).results.some((row: any) => Array.isArray(row.matched_keywords) && row.matched_keywords.length > 0));

  const message = (recent.data as any).inputs[0];
  const around = await queries.messages({ session_id: parsed.meta.id, around_sequence: message.sequence, before_count: 2, after_count: 2, order: "asc" });
  assert.equal(around.status, "ok");
  assert.ok((around.data as any).messages.length > 0);
});

test("supports the compression recovery workflow for this MCP task", async () => {
  const env = createFixtureHome();
  const sessionId = "context-recovery-session";
  writeRecoveryScenarioSession(path.join(env.activeDir, "recovery.jsonl"), sessionId);
  fs.writeFileSync(
    path.join(env.codexHome, "session_index.jsonl"),
    `${JSON.stringify({ id: sessionId, thread_name: "Codex Session MCP", updated_at: "2026-06-07T13:45:00.000Z" })}\n`,
    "utf8"
  );

  const { queries, indexer } = openFixture(env.codexHome, env.dbPath);
  await indexer.sync({ rebuild: true, force: true });

  const located = await queries.findByText({
    text: "新拉起的 codex_session 在启动时抛 SqliteError: database is locked",
    archive_scope: "active"
  });
  assert.equal(located.status, "ok");
  assert.equal((located.data as any).session_id, sessionId);
  assert.equal((located.data as any).match.input_type, "user_message");

  const recent = await queries.recentUserInputs({ session_id: sessionId, limit: 3 });
  assert.equal(recent.status, "ok");
  assert.deepEqual(
    (recent.data as any).inputs.map((input: any) => input.content_text.split("\n")[0]),
    [
      "工具卡住了，我觉得你没有完全做好测试情形覆盖",
      "独立测试失败点出来了：新拉起的 codex_session 在启动时抛 SqliteError: database is locked",
      "PLEASE IMPLEMENT THIS PLAN:"
    ]
  );

  const keyword = await queries.keywordSearch({
    session_id: sessionId,
    keywords: ["记住", "工具", "database is locked"],
    limit: 10
  });
  assert.equal(keyword.status, "ok");
  assert.ok((keyword.data as any).results.some((row: any) => row.matched_keywords.includes("database is locked")));
  assert.ok((keyword.data as any).results.some((row: any) => row.matched_keywords.includes("工具")));

  const sequence = (located.data as any).message.sequence;
  const around = await queries.messages({
    session_id: sessionId,
    around_sequence: sequence,
    before_count: 1,
    after_count: 3,
    order: "asc"
  });
  assert.equal(around.status, "ok");
  assert.equal((around.data as any).messages.length, 3);
  assert.ok(!(around.data as any).messages.some((message: any) => "raw_json" in message && message.raw_json !== undefined));

  const aroundRaw = await queries.messages({
    session_id: sessionId,
    around_sequence: sequence,
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
  assert.equal(calls.status, "ok");
  assert.equal((calls.data as any).tool_calls.length, 1);
  assert.match((calls.data as any).tool_calls[0].output_text, /database is locked/);
});

test("find_by_text requires a unique message match", async () => {
  const env = createFixtureHome();
  writeMiniSession(path.join(env.activeDir, "one.jsonl"), "session-one", "same locator phrase appears here");
  writeMiniSession(path.join(env.activeDir, "two.jsonl"), "session-two", "same locator phrase appears here");
  const { queries, indexer } = openFixture(env.codexHome, env.dbPath);
  await indexer.sync({ rebuild: true, force: true });

  const ambiguous = await queries.findByText({ text: "same locator phrase", archive_scope: "active" });
  assert.equal(ambiguous.status, "ambiguous");

  const located = await queries.findByText({ text: "same locator phrase appears here", archive_scope: "active", include_candidates: true });
  assert.equal(located.status, "ambiguous");
  assert.ok(!("candidates" in ambiguous));
  assert.ok("candidates" in located);
});

test("forked session files keep their first session_meta id and store lineage", async () => {
  const env = createFixtureHome();
  const filePath = path.join(env.activeDir, "forked.jsonl");
  writeForkedSession(filePath, "fork-session", "parent-session");

  const parsed = parseSessionFile(filePath);
  assert.equal(parsed.meta.id, "fork-session");
  assert.equal(parsed.meta.forked_from_id, "parent-session");

  const { db, queries, indexer } = openFixture(env.codexHome, env.dbPath);
  await indexer.sync({ rebuild: true, force: true });

  const listed = await queries.listSessions({ archive_scope: "active" });
  assert.deepEqual((listed.data as any).sessions.map((s: any) => s.id), ["fork-session"]);
  const row = db.prepare("SELECT session_id, forked_from_id FROM sessions").get() as
    | { session_id: string; forked_from_id: string | null }
    | undefined;
  assert.deepEqual(row, { session_id: "fork-session", forked_from_id: "parent-session" });

  const located = await queries.findByText({ text: "fork-only message text", archive_scope: "active" });
  assert.equal(located.status, "ok");
  assert.equal((located.data as any).session_id, "fork-session");
});

test("session locator token tools resolve the current session through indexed tool calls", async () => {
  const env = createFixtureHome();
  const { db, queries, indexer } = openFixture(env.codexHome, env.dbPath);
  const tokenResult = await queries.getSessionToken();
  assert.equal(tokenResult.status, "ok");
  const token = (tokenResult.data as any).token as string;
  const marker = (tokenResult.data as any).marker as string;
  assert.match(token, /^[0-9a-f-]{36}$/);
  assert.equal(marker, `codex-session-locator:${token}`);

  writeLocatorTokenSession(path.join(env.activeDir, "locator.jsonl"), "locator-session", token, marker);
  await indexer.sync({ rebuild: true, force: true });

  const located = await queries.getSessionByToken({ token });
  assert.equal(located.status, "ok");
  assert.equal((located.data as any).session_id, "locator-session");
  assert.equal((located.data as any).occurrences.length, 3);
  assert.deepEqual(
    (located.data as any).occurrences.map((occurrence: any) => occurrence.source).sort(),
    ["tool_arguments", "tool_output", "tool_output"]
  );

  const indexed = db.prepare("SELECT COUNT(*) AS n FROM session_locator_tokens WHERE token = ?").get(token) as { n: number };
  assert.equal(indexed.n, 3);
});

test("recent user inputs exposes get_task results as typed task retrievals", async () => {
  const env = createFixtureHome();
  const sessionId = "task-input-session";
  const token = "11111111-1111-4111-8111-111111111111";
  const task = `  执行这份子任务。\n${"x".repeat(4_500)}\nPreserve **Markdown** exactly.  `;
  writeTaskInputSession(path.join(env.activeDir, "task-input.jsonl"), sessionId, token, task);

  const { db, queries, indexer } = openFixture(env.codexHome, env.dbPath);
  await indexer.sync({ rebuild: true, force: true });

  const recent = await queries.recentUserInputs({ session_id: sessionId, limit: 3, max_chars: 20 });
  assert.equal(recent.status, "ok");
  const inputs = (recent.data as any).inputs;
  assert.deepEqual(inputs.map((input: any) => input.input_type), [
    "user_message",
    "published_task_retrieval",
    "user_message"
  ]);
  assert.deepEqual(inputs[1], {
    sequence: 3,
    timestamp: "2026-06-07T00:00:02.000Z",
    input_type: "published_task_retrieval",
    token,
    task: truncateText(task, 20),
    call_id: `${sessionId}-get-task`,
    tool_name: "codex_session_get_task"
  });
  assert.equal("role" in inputs[1], false);
  assert.equal("content_text" in inputs[1], false);
  assert.equal("task" in inputs[0], false);
  assert.equal(inputs.some((input: any) => "content_json" in input), false);

  const recentWithRaw = await queries.recentUserInputs({
    session_id: sessionId,
    limit: 1,
    max_chars: 20,
    include_raw: true
  });
  const rawInput = (recentWithRaw.data as any).inputs[0];
  assert.equal(rawInput.content_text, truncateText("ordinary input after task", 20));
  assert.equal(typeof rawInput.raw_json, "string");
  assert.equal("content_json" in rawInput, false);

  const indexed = db.prepare("SELECT token, task_text FROM session_task_inputs WHERE session_id = ?").get(sessionId);
  assert.deepEqual(indexed, { token, task_text: task });

  const locatedByTask = await queries.findByText({
    text: "Preserve **Markdown** exactly",
    archive_scope: "active",
    max_chars: 80
  });
  assert.equal(locatedByTask.status, "ok");
  assert.equal((locatedByTask.data as any).session_id, sessionId);
  assert.deepEqual(
    {
      input_type: (locatedByTask.data as any).match.input_type,
      sequence: (locatedByTask.data as any).match.sequence,
      token: (locatedByTask.data as any).match.token,
      call_id: (locatedByTask.data as any).match.call_id,
      tool_name: (locatedByTask.data as any).match.tool_name
    },
    {
      input_type: "published_task_retrieval",
      sequence: 3,
      token,
      call_id: `${sessionId}-get-task`,
      tool_name: "codex_session_get_task"
    }
  );
  assert.match((locatedByTask.data as any).match.snippet, /Preserve \*\*Markdown\*\* exactly/);
  assert.equal("message" in (locatedByTask.data as any), false);

  const locatedByToken = await queries.findByText({ text: token, archive_scope: "active" });
  assert.equal(locatedByToken.status, "ok");
  assert.equal((locatedByToken.data as any).session_id, sessionId);
  assert.equal((locatedByToken.data as any).match.input_type, "published_task_retrieval");
  assert.equal((locatedByToken.data as any).match.token, token);
});

test("find_by_text collapses repeated retrievals of the same published task", async () => {
  const env = createFixtureHome();
  const sessionId = "repeated-task-input-session";
  const token = "22222222-2222-4222-8222-222222222222";
  const task = "Repeatable delegated instruction with one distinctive recovery phrase.";
  writeTaskInputSession(path.join(env.activeDir, "repeated-task-input.jsonl"), sessionId, token, task, true);

  const { queries, indexer } = openFixture(env.codexHome, env.dbPath);
  await indexer.sync({ rebuild: true, force: true });

  for (const text of ["one distinctive recovery phrase", token]) {
    const located = await queries.findByText({ text, archive_scope: "active" });
    assert.equal(located.status, "ok");
    assert.equal((located.data as any).session_id, sessionId);
    assert.equal((located.data as any).match.input_type, "published_task_retrieval");
    assert.equal((located.data as any).match.token, token);
    assert.equal((located.data as any).match.occurrences, 2);
    assert.equal((located.data as any).match.sequence, 5);
    assert.equal((located.data as any).match.call_id, `${sessionId}-get-task-repeat`);
    assert.equal("message" in (located.data as any), false);
  }
});

test("archive_scope isolates active and archived sessions and deletion follows disk state", async () => {
  const env = createFixtureHome();
  const activeFile = path.join(env.activeDir, "active.jsonl");
  const archivedFile = path.join(env.archivedDir, "archived.jsonl");
  const movedArchivedFile = path.join(env.archivedDir, "active-moved.jsonl");
  writeMiniSession(activeFile, "active-session", "active only keyword");
  writeMiniSession(archivedFile, "archived-session", "archived only keyword");
  const { queries, indexer } = openFixture(env.codexHome, env.dbPath);
  await indexer.sync({ rebuild: true, force: true });

  const active = await queries.listSessions({ archive_scope: "active" });
  const archived = await queries.listSessions({ archive_scope: "archived" });
  assert.deepEqual((active.data as any).sessions.map((s: any) => s.id), ["active-session"]);
  assert.deepEqual((archived.data as any).sessions.map((s: any) => s.id), ["archived-session"]);

  fs.renameSync(activeFile, movedArchivedFile);
  await indexer.sync({ force: true });
  const afterMoveActive = await queries.listSessions({ archive_scope: "active" });
  const afterMoveArchived = await queries.listSessions({ archive_scope: "archived", order: "asc" });
  assert.deepEqual((afterMoveActive.data as any).sessions.map((s: any) => s.id), []);
  assert.deepEqual((afterMoveArchived.data as any).sessions.map((s: any) => s.id).sort(), ["active-session", "archived-session"]);

  fs.unlinkSync(movedArchivedFile);
  await indexer.sync({ force: true });
  const afterDeleteArchived = await queries.listSessions({ archive_scope: "archived" });
  assert.deepEqual((afterDeleteArchived.data as any).sessions.map((s: any) => s.id), ["archived-session"]);
});

test("indexer startup tolerates a locked sqlite writer", async () => {
  const env = createFixtureHome();
  const setupDb = openDatabase(env.dbPath, { busyTimeoutMs: 20 });
  setupDb.close();

  const blocker = new Database(env.dbPath, { timeout: 20 });
  blocker.pragma("busy_timeout = 20");
  blocker.exec("BEGIN IMMEDIATE");

  const paths = resolveRuntimePaths({ codexHome: env.codexHome, indexDbPath: env.dbPath });
  const db = openDatabase(paths.indexDbPath, { busyTimeoutMs: 20 });
  const indexer = new CodexSessionIndexer(db, paths);
  try {
    assert.doesNotThrow(() => indexer.start());
    await assert.rejects(() => indexer.sync({ force: true }), /SQLite index database is locked/);
  } finally {
    await indexer.stop();
    db.close();
    blocker.exec("ROLLBACK");
    blocker.close();
  }
});

test("indexer stop releases its leader lease", async () => {
  const env = createFixtureHome();
  const paths = resolveRuntimePaths({ codexHome: env.codexHome, indexDbPath: env.dbPath });
  const db = openDatabase(paths.indexDbPath);
  const indexer = new CodexSessionIndexer(db, paths);

  indexer.start();
  assert.equal(indexer.status().is_leader, true);
  await indexer.stop();

  const lease = db.prepare("SELECT * FROM leader_lease WHERE holder_id = ?").get(indexer.holderId);
  assert.equal(lease, undefined);
  db.close();
});

test("leader renewal does not repeatedly start incremental sync", async () => {
  const env = createFixtureHome();
  const paths = resolveRuntimePaths({ codexHome: env.codexHome, indexDbPath: env.dbPath });
  const db = openDatabase(paths.indexDbPath);
  const indexer = new CodexSessionIndexer(db, paths, {
    runSyncInProcess: true,
    leaderRenewIntervalMs: 20,
    syncCheckIntervalMs: 60_000
  });

  try {
    indexer.start();
    await eventually(() => {
      assert.equal(indexer.status().indexing, false);
      assert.ok(indexer.status().started_at);
    }, 1_000);
    const startedAt = indexer.status().started_at;
    await new Promise((resolve) => setTimeout(resolve, 120));
    assert.equal(indexer.status().started_at, startedAt);
  } finally {
    await indexer.stop();
    db.close();
  }
});

test("query readiness check is throttled after a completed sync", async () => {
  const env = createFixtureHome();
  writeMiniSession(path.join(env.activeDir, "one.jsonl"), "session-one", "stable query throttle text");
  const { queries, indexer } = openFixture(env.codexHome, env.dbPath, {
    waitForIdleMs: 20,
    syncCheckIntervalMs: 60_000
  });
  await indexer.sync({ rebuild: true, force: true });
  const startedAt = indexer.status().started_at;

  const first = await queries.listSessions({ archive_scope: "active" });
  const second = await queries.listSessions({ archive_scope: "active" });

  assert.equal(first.status, "ok");
  assert.equal(second.status, "ok");
  assert.equal(indexer.status().started_at, startedAt);
});

test("stdio server lists tools instead of closing when the index database is locked", async () => {
  const env = createFixtureHome();
  const setupDb = openDatabase(env.dbPath, { busyTimeoutMs: 20 });
  setupDb.close();

  const blocker = new Database(env.dbPath, { timeout: 20 });
  blocker.pragma("busy_timeout = 20");
  blocker.exec("BEGIN IMMEDIATE");

  const client = new Client({ name: "locked-db-test", version: "0.1.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(projectRoot, "dist", "src", "server.js")],
    cwd: projectRoot,
    env: {
      ...process.env,
      CODEX_HOME: env.codexHome,
      CODEX_SESSION_MCP_DB: env.dbPath,
      CODEX_SESSION_MCP_BUSY_TIMEOUT_MS: "20"
    },
    stderr: "pipe"
  });

  try {
    await withTimeout(client.connect(transport), 2_000, "MCP initialize should complete while sqlite is locked");
    const tools = await withTimeout(client.listTools(), 2_000, "MCP listTools should complete while sqlite is locked");
    assert.ok(tools.tools.some((tool) => tool.name === "codex_session_status"));
  } finally {
    await client.close().catch(() => undefined);
    blocker.exec("ROLLBACK");
    blocker.close();
  }
});

test("stdio server releases the leader lease when the client closes", async () => {
  const env = createFixtureHome();
  const client = new Client({ name: "lease-release-test", version: "0.1.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(projectRoot, "dist", "src", "server.js")],
    cwd: projectRoot,
    env: {
      ...process.env,
      CODEX_HOME: env.codexHome,
      CODEX_SESSION_MCP_DB: env.dbPath
    },
    stderr: "pipe"
  });

  await withTimeout(client.connect(transport), 2_000, "MCP initialize should complete");
  const status = await callToolJson(client, "codex_session_status", {});
  assert.equal(status.is_leader, true);
  await client.close();

  const db = openDatabase(env.dbPath);
  try {
    await eventually(() => {
      const lease = db.prepare("SELECT * FROM leader_lease WHERE singleton_key = 'codex-session-mcp'").get();
      assert.equal(lease, undefined);
    }, 2_000);
  } finally {
    db.close();
  }
});

test("stdio get_task adds a context recovery comment from the second retrieval", async () => {
  const env = createFixtureHome();
  const client = new Client({ name: "repeated-task-retrieval-test", version: "0.1.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(projectRoot, "dist", "src", "server.js")],
    cwd: projectRoot,
    env: {
      ...process.env,
      CODEX_HOME: env.codexHome,
      CODEX_SESSION_MCP_DB: env.dbPath
    },
    stderr: "pipe"
  });

  try {
    await withTimeout(client.connect(transport), 2_000, "MCP initialize should complete");
    const published = await callToolJson(client, "codex_session_publish_task", { task: "Recover this task." });
    assert.deepEqual(published.data, {
      token: published.data.token,
      prompt: `token: ${published.data.token}, use codex_session_get_task tool to retrieve exact instruction`,
      comment:
        "For every later instruction update to the agent, use the same handoff: call codex_session_publish_task, then send the returned prompt verbatim."
    });
    const first = await callToolJson(client, "codex_session_get_task", { token: published.data.token });
    const second = await callToolJson(client, "codex_session_get_task", { token: published.data.token });

    assert.deepEqual(first.data, { token: published.data.token, task: "Recover this task." });
    assert.equal(second.data.token, published.data.token);
    assert.equal(second.data.task, "Recover this task.");
    assert.equal(typeof second.data.comment, "string");
    assert.ok(second.data.comment.trim().length > 0);
  } finally {
    await client.close().catch(() => undefined);
  }
});

test("query tools return indexing envelope when local sync is still running", async () => {
  const env = createFixtureHome();
  const { db, queries, indexer } = openFixture(env.codexHome, env.dbPath, { waitForIdleMs: 20 });
  db.prepare(
    `INSERT INTO sync_status (singleton_key, indexing, started_at, completed_at, files_seen, files_indexed, events_indexed, error)
     VALUES ('main', 1, '2026-06-07T00:00:00.000Z', NULL, 0, 0, 0, NULL)`
  ).run();
  (indexer as any).syncPromise = new Promise(() => undefined);

  const listed = await withTimeout(queries.listSessions({ archive_scope: "active" }), 500, "listSessions should not hang behind local sync");
  assert.equal(listed.status, "indexing");
  assert.equal((listed.data as any).indexing, true);

  const sync = await withTimeout(queries.sync({}), 500, "sync should return indexing before the MCP request timeout");
  assert.equal(sync.status, "indexing");
  assert.equal((sync.data as any).indexing, true);
});

function createFixtureHome(): { root: string; codexHome: string; activeDir: string; archivedDir: string; dbPath: string } {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-session-mcp-test-"));
  const codexHome = path.join(root, ".codex");
  const activeDir = path.join(codexHome, "sessions", "2026", "06", "07");
  const archivedDir = path.join(codexHome, "archived_sessions");
  ensureDir(activeDir);
  ensureDir(archivedDir);
  return { root, codexHome, activeDir, archivedDir, dbPath: path.join(root, "index.sqlite") };
}

function openFixture(
  codexHome: string,
  dbPath: string,
  options: { waitForIdleMs?: number; syncCheckIntervalMs?: number } = {}
): { db: Database.Database; indexer: CodexSessionIndexer; queries: CodexSessionQueries } {
  const paths = resolveRuntimePaths({ codexHome, indexDbPath: dbPath });
  const db = openDatabase(paths.indexDbPath);
  const indexer = new CodexSessionIndexer(db, paths, { syncCheckIntervalMs: options.syncCheckIntervalMs });
  const queries = new CodexSessionQueries({ db, indexer, waitForIdleMs: options.waitForIdleMs });
  return { db, indexer, queries };
}

function writeMiniSession(filePath: string, sessionId: string, userText: string): void {
  const lines = [
    {
      timestamp: "2026-06-07T00:00:00.000Z",
      type: "session_meta",
      payload: { id: sessionId, timestamp: "2026-06-07T00:00:00.000Z", cwd: "C:\\Users\\<USER>\\project" }
    },
    {
      timestamp: "2026-06-07T00:00:01.000Z",
      type: "response_item",
      payload: { type: "message", role: "user", content: [{ type: "input_text", text: userText }] }
    },
    {
      timestamp: "2026-06-07T00:00:02.000Z",
      type: "response_item",
      payload: { type: "function_call", name: "functions.shell_command", arguments: "{\"command\":\"date\"}", call_id: `${sessionId}-call` }
    },
    {
      timestamp: "2026-06-07T00:00:03.000Z",
      type: "response_item",
      payload: { type: "function_call_output", call_id: `${sessionId}-call`, output: "tool output text" }
    }
  ];
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function writeForkedSession(filePath: string, sessionId: string, parentSessionId: string): void {
  const lines = [
    {
      timestamp: "2026-06-07T00:00:00.000Z",
      type: "session_meta",
      payload: {
        id: sessionId,
        forked_from_id: parentSessionId,
        timestamp: "2026-06-07T00:00:00.000Z",
        cwd: "C:\\Users\\<USER>\\fork"
      }
    },
    {
      timestamp: "2026-06-06T23:00:00.000Z",
      type: "session_meta",
      payload: {
        id: parentSessionId,
        timestamp: "2026-06-06T23:00:00.000Z",
        cwd: "C:\\Users\\<USER>\\parent"
      }
    },
    {
      timestamp: "2026-06-07T00:00:01.000Z",
      type: "response_item",
      payload: { type: "message", role: "user", content: [{ type: "input_text", text: "fork-only message text" }] }
    }
  ];
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function writeLocatorTokenSession(filePath: string, sessionId: string, token: string, marker: string): void {
  const lines = [
    {
      timestamp: "2026-06-07T00:00:00.000Z",
      type: "session_meta",
      payload: { id: sessionId, timestamp: "2026-06-07T00:00:00.000Z", cwd: "C:\\Users\\<USER>\\locator" }
    },
    {
      timestamp: "2026-06-07T00:00:01.000Z",
      type: "response_item",
      payload: {
        type: "custom_tool_call",
        name: "mcp__codex_session.codex_session_get_session_token",
        input: "{}",
        call_id: `${sessionId}-token-call`
      }
    },
    {
      timestamp: "2026-06-07T00:00:02.000Z",
      type: "response_item",
      payload: {
        type: "custom_tool_call_output",
        call_id: `${sessionId}-token-call`,
        output: JSON.stringify({ status: "ok", data: { token, marker } })
      }
    },
    {
      timestamp: "2026-06-07T00:00:02.500Z",
      type: "event_msg",
      payload: {
        type: "mcp_tool_call_end",
        call_id: `${sessionId}-plugin-token-call`,
        invocation: {
          server: "codex_session_context",
          tool: "codex_session_get_session_token",
          arguments: {}
        },
        result: {
          Ok: {
            content: [{ type: "text", text: JSON.stringify({ status: "ok", data: { token, marker } }) }]
          }
        }
      }
    },
    {
      timestamp: "2026-06-07T00:00:03.000Z",
      type: "response_item",
      payload: {
        type: "custom_tool_call",
        name: "mcp__codex_session.codex_session_get_session_by_token",
        input: JSON.stringify({ token }),
        call_id: `${sessionId}-locate-call`
      }
    }
  ];
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function writeTaskInputSession(filePath: string, sessionId: string, token: string, task: string, repeatTask = false): void {
  const lines: any[] = [
    {
      timestamp: "2026-06-07T00:00:00.000Z",
      type: "session_meta",
      payload: { id: sessionId, timestamp: "2026-06-07T00:00:00.000Z", cwd: "C:\\Users\\<USER>\\task-input" }
    },
    {
      timestamp: "2026-06-07T00:00:01.000Z",
      type: "response_item",
      payload: { type: "message", role: "user", content: [{ type: "input_text", text: "ordinary input before task" }] }
    },
    {
      timestamp: "2026-06-07T00:00:02.000Z",
      type: "event_msg",
      payload: {
        type: "mcp_tool_call_end",
        call_id: `${sessionId}-get-task`,
        invocation: {
          server: "codex_session_context",
          tool: "codex_session_get_task",
          arguments: { token }
        },
        result: {
          Ok: {
            content: [{ type: "text", text: JSON.stringify({ status: "ok", data: { token, task } }) }]
          }
        }
      }
    },
    {
      timestamp: "2026-06-07T00:00:03.000Z",
      type: "response_item",
      payload: { type: "message", role: "user", content: [{ type: "input_text", text: "ordinary input after task" }] }
    }
  ];
  if (repeatTask) {
    lines.push({
      timestamp: "2026-06-07T00:00:04.000Z",
      type: "event_msg",
      payload: {
        type: "mcp_tool_call_end",
        call_id: `${sessionId}-get-task-repeat`,
        invocation: {
          server: "codex_session_context",
          tool: "codex_session_get_task",
          arguments: { token }
        },
        result: {
          Ok: {
            content: [{ type: "text", text: JSON.stringify({ status: "ok", data: { token, task } }) }]
          }
        }
      }
    });
  }
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function writeRecoveryScenarioSession(filePath: string, sessionId: string): void {
  const lines = [
    {
      timestamp: "2026-06-07T13:00:00.000Z",
      type: "session_meta",
      payload: { id: sessionId, timestamp: "2026-06-07T13:00:00.000Z", cwd: "C:\\Users\\<USER>\\project" }
    },
    {
      timestamp: "2026-06-07T13:01:00.000Z",
      type: "response_item",
      payload: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "想一个事情：我们能不能做一个mcp server，专门监控当前codex session json文件，建立索引，记住关键事实" }]
      }
    },
    {
      timestamp: "2026-06-07T13:02:00.000Z",
      type: "response_item",
      payload: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "PLEASE IMPLEMENT THIS PLAN:\n实现本地只读 stdio MCP server，提供固定查询工具。" }]
      }
    },
    {
      timestamp: "2026-06-07T13:03:00.000Z",
      type: "response_item",
      payload: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "独立测试失败点出来了：新拉起的 codex_session 在启动时抛 SqliteError: database is locked" }]
      }
    },
    {
      timestamp: "2026-06-07T13:04:00.000Z",
      type: "response_item",
      payload: {
        type: "function_call",
        name: "functions.shell_command",
        arguments: "{\"command\":\"Get-Process node\"}",
        call_id: `${sessionId}-lock-check`
      }
    },
    {
      timestamp: "2026-06-07T13:04:01.000Z",
      type: "response_item",
      payload: {
        type: "function_call_output",
        call_id: `${sessionId}-lock-check`,
        output: "SqliteError: database is locked; connection closed before initialize response"
      }
    },
    {
      timestamp: "2026-06-07T13:05:00.000Z",
      type: "response_item",
      payload: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "工具卡住了，我觉得你没有完全做好测试情形覆盖" }]
      }
    }
  ];
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

async function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(message)), ms);
      })
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function callToolJson(client: Client, name: string, args: Record<string, unknown>): Promise<any> {
  const result = await withTimeout(client.callTool({ name, arguments: args }), 2_000, `${name} should return`);
  const content = result.content as Array<{ type: string; text?: string }>;
  assert.equal(content[0]?.type, "text");
  return JSON.parse(content[0]?.text ?? "{}");
}

async function eventually(assertion: () => void, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() <= deadline) {
    try {
      assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  if (lastError) throw lastError;
}

function walkJson(value: unknown, visit: (key: string, value: unknown) => void): void {
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    for (const item of value) walkJson(item, visit);
    return;
  }
  for (const [key, inner] of Object.entries(value)) {
    visit(key, inner);
    walkJson(inner, visit);
  }
}
