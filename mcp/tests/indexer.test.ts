import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { INDEX_SCHEMA_VERSION, openDatabase } from "../src/db.js";
import { CodexSessionIndexer } from "../src/indexer.js";
import { parseSessionFile } from "../src/parser.js";
import { resolveRuntimePaths, RuntimePaths } from "../src/paths.js";
import type { SyncResult } from "../src/types.js";
import {
  appendLocatorMarkerEvent,
  createFixtureHome,
  deferDatabase,
  openFixture,
  writeForkedSession,
  writeMiniSession,
  writeSessionMeta
} from "./test-fixtures.js";

class CountingIndexer extends CodexSessionIndexer {
  syncCalls = 0;

  override sync(options: { rebuild?: boolean; force?: boolean } = {}): Promise<SyncResult> {
    this.syncCalls += 1;
    return super.sync(options);
  }
}

class ManualScheduler {
  private readonly callbacks = new Map<NodeJS.Timeout, () => void>();

  setTimeout(callback: () => void): NodeJS.Timeout {
    const handle = {} as NodeJS.Timeout;
    this.callbacks.set(handle, callback);
    return handle;
  }

  clearTimeout(handle: NodeJS.Timeout): void {
    this.callbacks.delete(handle);
  }

  runAll(): void {
    const callbacks = [...this.callbacks.values()];
    this.callbacks.clear();
    for (const callback of callbacks) callback();
  }
}

test("forked session files retain their own id and parent lineage", async (t) => {
  const env = createFixtureHome(t);
  const filePath = path.join(env.activeDir, "forked.jsonl");
  writeForkedSession(filePath, "fork-session", "parent-session");

  const parsed = parseSessionFile(filePath);
  assert.deepEqual(
    { id: parsed.meta.id, forked_from_id: parsed.meta.forked_from_id },
    { id: "fork-session", forked_from_id: "parent-session" }
  );

  const { db, queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });
  const listed = await queries.listSessions({ archive_scope: "active" });
  assert.deepEqual((listed.data as any).sessions.map((session: any) => session.id), ["fork-session"]);
  const storedLineage = db.prepare("SELECT session_id, forked_from_id FROM sessions").get();
  assert.deepEqual(storedLineage, { session_id: "fork-session", forked_from_id: "parent-session" });

  const located = await queries.findByText({ text: "fork-only message text", archive_scope: "active" });
  assert.equal((located.data as any).session_id, "fork-session");
});

test("token lookup incrementally indexes a newly written locator marker", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "locator-session";
  const filePath = path.join(env.activeDir, "locator.jsonl");
  writeSessionMeta(filePath, sessionId);
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const tokenResult = await queries.getSessionToken();
  const token = (tokenResult.data as any).token as string;
  const marker = (tokenResult.data as any).marker as string;
  const beforeWrite = await queries.getSessionByToken({ token });
  assert.deepEqual(
    { status: beforeWrite.status, token: (beforeWrite.data as any).token },
    { status: "pending", token }
  );

  appendLocatorMarkerEvent(filePath, sessionId, token, marker);
  const located = await queries.getSessionByToken({ token });
  assert.equal(located.status, "ok");
  assert.deepEqual(
    {
      token: (located.data as any).token,
      session_id: (located.data as any).session_id,
      occurrences: (located.data as any).occurrences
    },
    {
      token,
      session_id: sessionId,
      occurrences: [
        {
          sequence: 2,
          timestamp: "2026-06-07T00:00:01.000Z",
          call_id: `${sessionId}-token-call`,
          tool_name: "codex_session_get_session_token",
          source: "tool_output"
        }
      ]
    }
  );
});

test("item_completed MCP calls populate recovery indexes across a schema rebuild", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
  const locatorToken = "11111111-2222-4333-8444-555555555555";
  const taskToken = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
  const filePath = path.join(env.activeDir, `rollout-${sessionId}.jsonl`);
  writeLines(filePath, [
    sessionMetaLine(sessionId, undefined, "2026-06-07T00:00:00.000Z"),
    eventLine("task_started", { turn_id: "new-format-turn" }),
    completedMcpCallLine(
      "new-format-token-call",
      "codex_session_get_session_token",
      {},
      { status: "ok", data: { token: locatorToken, marker: `codex-session-locator:${locatorToken}` } }
    ),
    completedMcpCallLine(
      "new-format-task-call",
      "codex_session_get_task",
      { token: taskToken },
      { status: "ok", data: { token: taskToken, task: "task recovered from new event format" } }
    ),
    eventLine("task_complete", { turn_id: "new-format-turn" })
  ]);

  const { db, queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const located = await queries.getSessionByToken({ token: locatorToken });
  assert.equal((located.data as any).session_id, sessionId);
  const recent = await queries.recentUserInputs({ session_id: sessionId, limit: 10 });
  assert.equal((recent.data as any).inputs[0].task, "task recovered from new event format");
  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS locators FROM session_locator_tokens").get(),
    { locators: 1 }
  );
  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS task_inputs, COUNT(turn_ref) AS turn_refs FROM session_task_inputs").get(),
    { task_inputs: 1, turn_refs: 1 }
  );

  db.exec("DELETE FROM session_locator_tokens; DELETE FROM session_task_inputs;");
  db.pragma("user_version = 7");
  await indexer.sync();

  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS locators FROM session_locator_tokens").get(),
    { locators: 1 }
  );
  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS task_inputs, COUNT(turn_ref) AS turn_refs FROM session_task_inputs").get(),
    { task_inputs: 1, turn_refs: 1 }
  );
  assert.equal(db.pragma("user_version", { simple: true }), INDEX_SCHEMA_VERSION);
});

test("locator lookup chooses the original token-generating call over inherited copies", async (t) => {
  const env = createFixtureHome(t);
  const originalSession = "11111111-aaaa-4111-8111-111111111111";
  const copiedSession = "22222222-bbbb-4222-8222-222222222222";
  const token = "33333333-cccc-4333-8333-333333333333";
  const callId = "shared-token-call";
  const originalCall = completedMcpCallLine(
    callId,
    "codex_session_get_session_token",
    {},
    { status: "ok", data: { token, marker: `codex-session-locator:${token}` } }
  );
  const copiedCall = structuredClone(originalCall);
  originalCall.timestamp = "2026-06-07T00:00:01.000Z";
  copiedCall.timestamp = "2026-06-07T01:00:01.000Z";
  writeLines(path.join(env.activeDir, "original.jsonl"), [
    sessionMetaLine(originalSession, undefined, "2026-06-07T00:00:00.000Z"),
    originalCall
  ]);
  writeLines(path.join(env.activeDir, "copy.jsonl"), [
    sessionMetaLine(copiedSession, undefined, "2026-06-07T01:00:00.000Z"),
    copiedCall
  ]);

  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });
  const located = await queries.getSessionByToken({ token });
  assert.equal(located.status, "ok");
  assert.equal((located.data as any).session_id, originalSession);
  assert.equal((located.data as any).occurrences.length, 1);
});

test("paginated rollout history follows immutable history_base cutoffs", async (t) => {
  const env = createFixtureHome(t);
  const threadId = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";
  const middleRolloutId = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb";
  const currentRolloutId = "cccccccc-3333-4333-8333-cccccccccccc";
  const token = "dddddddd-4444-4444-8444-dddddddddddd";
  const rootFile = path.join(env.activeDir, `rollout-2026-06-07T00-00-00-${threadId}.jsonl`);
  const middleFile = path.join(
    env.activeDir,
    `rollout-2026-06-07T00-01-00-${threadId}_${middleRolloutId}.jsonl`
  );
  const currentFile = path.join(
    env.activeDir,
    `rollout-2026-06-07T00-02-00-${threadId}_${currentRolloutId}.jsonl`
  );

  const rootLines = [
    paginatedMetaLine(threadId, 0),
    { ...messageLine("user", "root retained"), ordinal: 1 },
    { ...messageLine("user", "root replaced"), ordinal: 2 }
  ];
  writeLines(rootFile, rootLines);
  const middleLines = [
    paginatedMetaLine(threadId, 2, {
      thread_id: threadId,
      end_ordinal_exclusive: 2,
      end_byte_offset: serializedPrefixBytes(rootLines, 2)
    }),
    { ...messageLine("user", "middle retained"), ordinal: 3 },
    { ...messageLine("user", "middle replaced"), ordinal: 4 }
  ];
  writeLines(middleFile, middleLines);
  const currentLines = [
    paginatedMetaLine(threadId, 4, {
      thread_id: middleRolloutId,
      end_ordinal_exclusive: 4,
      end_byte_offset: serializedPrefixBytes(middleLines, 2)
    }),
    {
      ...completedMcpCallLine(
        "paginated-token-call",
        "codex_session_get_session_token",
        {},
        { status: "ok", data: { token, marker: `codex-session-locator:${token}` } }
      ),
      ordinal: 5
    },
    { ...messageLine("user", "current input"), ordinal: 6 }
  ];
  writeLines(currentFile, currentLines);
  fs.utimesSync(rootFile, new Date("2026-06-07T00:00:00Z"), new Date("2026-06-07T00:00:00Z"));
  fs.utimesSync(middleFile, new Date("2026-06-07T00:01:00Z"), new Date("2026-06-07T00:01:00Z"));
  fs.utimesSync(currentFile, new Date("2026-06-07T00:02:00Z"), new Date("2026-06-07T00:02:00Z"));

  const { db, queries, indexer } = openFixture(env);
  const rebuilt = await indexer.sync({ rebuild: true, force: true });
  assert.equal(rebuilt.files_indexed, 3);
  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS sessions FROM sessions").get(),
    { sessions: 1 }
  );
  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS rollouts FROM rollouts").get(),
    { rollouts: 3 }
  );
  assert.deepEqual(
    db.prepare("SELECT COUNT(*) AS edges FROM rollout_history").get(),
    { edges: 2 }
  );

  const recent = await queries.recentUserInputs({ session_id: threadId, limit: 10 });
  assert.deepEqual(
    (recent.data as any).inputs.map((input: any) => [input.sequence, input.content_text]),
    [[6, "current input"], [3, "middle retained"], [1, "root retained"]]
  );
  const replaced = await queries.findByText({ text: "middle replaced" });
  assert.equal(replaced.status, "not_found");
  const located = await queries.getSessionByToken({ token });
  assert.equal((located.data as any).session_id, threadId);
  assert.equal((await indexer.sync()).files_indexed, 0);
});

test("a legacy index is rebuilt and physically compacted once", async (t) => {
  const env = createFixtureHome(t);
  writeMiniSession(path.join(env.activeDir, "legacy.jsonl"), "legacy-session", "legacy input");
  const { db, indexer } = openFixture(env);
  await indexer.sync({ force: true });

  db.exec(`
    CREATE TABLE upgrade_ballast (content BLOB);
    INSERT INTO upgrade_ballast VALUES (zeroblob(8388608));
    DROP TABLE upgrade_ballast;
  `);
  db.pragma("wal_checkpoint(TRUNCATE)");
  const legacyBytes = fs.statSync(env.dbPath).size;
  db.pragma("user_version = 5");
  const observer = openDatabase(env.dbPath);
  assert.equal(observer.pragma("user_version", { simple: true }), 5);
  observer.close();

  const upgraded = await indexer.sync();
  const compactedBytes = fs.statSync(env.dbPath).size;
  assert.deepEqual(
    {
      userVersion: db.pragma("user_version", { simple: true }),
      indexVersion: (db.prepare("SELECT index_version FROM rollouts").get() as { index_version: number }).index_version,
      rawEventsRebuilt: (db.prepare("SELECT COUNT(*) AS count FROM raw_events").get() as { count: number }).count > 0,
      filesIndexed: upgraded.files_indexed,
      fileShrank: compactedBytes < legacyBytes
    },
    {
      userVersion: INDEX_SCHEMA_VERSION,
      indexVersion: 4,
      rawEventsRebuilt: true,
      filesIndexed: 1,
      fileShrank: true
    }
  );

  const nextSync = await indexer.sync();
  assert.equal(nextSync.files_indexed, 0);
});

test("fork history is shared at the rollback boundary and later file changes append only", async (t) => {
  const env = createFixtureHome(t);
  const parentId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const childId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
  const parentFile = path.join(env.activeDir, `rollout-${parentId}.jsonl`);
  const childFile = path.join(env.activeDir, `rollout-${childId}.jsonl`);
  const parentLines = forkParentLines(parentId);
  const childLines = [
    sessionMetaLine(childId, parentId, "2026-06-07T01:00:00.000Z"),
    ...parentLines.map((line) => ({ ...line, timestamp: "2026-06-07T01:00:00.001Z" })),
    eventLine("thread_settings_applied"),
    eventLine("token_count"),
    eventLine("thread_rolled_back", { num_turns: 2 }),
    eventLine("task_started", { turn_id: "child-turn" }),
    messageLine("user", "child input"),
    messageLine("assistant", "child answer"),
    eventLine("task_complete", { turn_id: "child-turn" })
  ];
  writeLines(parentFile, parentLines);
  writeLines(childFile, childLines);

  const { db, queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const lineage = db
    .prepare(
      `SELECT base_rollout_id, replay_parent_sequence, local_start_sequence,
              parent_cutoff_sequence
       FROM rollout_history
       WHERE rollout_id = ?`
    )
    .get(childId);
  assert.deepEqual(lineage, {
    base_rollout_id: parentId,
    replay_parent_sequence: parentLines.length,
    local_start_sequence: parentLines.length + 2,
    parent_cutoff_sequence: 5
  });
  assert.equal(
    (db.prepare("SELECT COUNT(*) AS count FROM raw_events WHERE session_id = ?").get(childId) as { count: number }).count,
    8
  );

  const initialInputs = await queries.recentUserInputs({ session_id: childId, limit: 10 });
  assert.deepEqual(
    (initialInputs.data as any).inputs.map((input: any) => input.content_text),
    ["child input", "retained input"]
  );
  assert.equal((initialInputs.data as any).parent_history_status, "included");

  const firstChildRawId = (db
    .prepare("SELECT MIN(id) AS id FROM raw_events WHERE session_id = ?")
    .get(childId) as { id: number }).id;
  appendLines(childFile, [
    eventLine("task_started", { turn_id: "appended-turn" }),
    messageLine("user", "appended input"),
    messageLine("assistant", "appended answer"),
    eventLine("task_complete", { turn_id: "appended-turn" })
  ]);
  const appendSync = await indexer.sync();
  assert.deepEqual(
    {
      files_indexed: appendSync.files_indexed,
      events_indexed: appendSync.events_indexed,
      firstChildRawId: (db
        .prepare("SELECT MIN(id) AS id FROM raw_events WHERE session_id = ?")
        .get(childId) as { id: number }).id
    },
    { files_indexed: 1, events_indexed: 4, firstChildRawId }
  );

  fs.writeFileSync(
    parentFile,
    fs.readFileSync(parentFile, "utf8").replace("retained answer", "tampered answer"),
    "utf8"
  );
  const afterBoundaryChange = await queries.recentUserInputs({ session_id: childId, limit: 10 });
  assert.deepEqual(
    (afterBoundaryChange.data as any).inputs.map((input: any) => input.content_text),
    ["appended input", "child input"]
  );
  assert.equal((afterBoundaryChange.data as any).parent_history_status, "boundary_mismatch");
});

test("rewind hides the completed turn including steer inputs and outputs", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
  const filePath = path.join(env.activeDir, `rollout-${sessionId}.jsonl`);
  const taskToken = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
  writeLines(filePath, [
    sessionMetaLine(sessionId, undefined, "2026-06-07T02:00:00.000Z"),
    eventLine("task_started", { turn_id: "original-turn" }),
    messageLine("user", "original input"),
    messageLine("assistant", "first original output"),
    messageLine("user", "steer input inside original turn"),
    toolCallLine("original-call"),
    toolOutputLine("original-call", "original tool output"),
    taskRetrievalLine(sessionId, taskToken, "task recovered inside original turn"),
    messageLine("assistant", "final original output"),
    eventLine("task_complete", { turn_id: "original-turn" })
  ]);

  const { db, queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });
  const beforeRewind = await queries.recentUserInputs({ session_id: sessionId, limit: 10 });
  assert.deepEqual(
    (beforeRewind.data as any).inputs.map((input: any) => input.task ?? input.content_text),
    ["task recovered inside original turn", "steer input inside original turn", "original input"]
  );

  appendLines(filePath, [
    eventLine("thread_rolled_back", { num_turns: 1 }),
    eventLine("task_started", { turn_id: "replacement-turn" }),
    messageLine("user", "edited replacement input"),
    messageLine("assistant", "replacement answer"),
    eventLine("task_complete", { turn_id: "replacement-turn" }),
    eventLine("task_started", { turn_id: "aborted-turn" }),
    messageLine("user", "input in later aborted turn"),
    messageLine("user", "steer in later aborted turn"),
    messageLine("assistant", "partial later output"),
    eventLine("turn_aborted", { turn_id: "aborted-turn" })
  ]);
  await indexer.sync();

  const turns = db
    .prepare("SELECT turn_id, rewound FROM session_turns WHERE session_id = ? ORDER BY start_sequence")
    .all(sessionId);
  assert.deepEqual(turns, [
    { turn_id: "original-turn", rewound: 1 },
    { turn_id: "replacement-turn", rewound: 0 },
    { turn_id: "aborted-turn", rewound: 0 }
  ]);

  const recent = await queries.recentUserInputs({ session_id: sessionId, limit: 10 });
  assert.deepEqual(
    (recent.data as any).inputs.map((input: any) => input.task ?? input.content_text),
    ["steer in later aborted turn", "input in later aborted turn", "edited replacement input"]
  );
  const messages = await queries.messages({ session_id: sessionId, order: "asc", limit: 20 });
  assert.deepEqual(
    (messages.data as any).messages.map((message: any) => message.content_text),
    [
      "edited replacement input",
      "replacement answer",
      "input in later aborted turn",
      "steer in later aborted turn",
      "partial later output"
    ]
  );
  const toolCalls = await queries.toolCalls({ session_id: sessionId, limit: 10 });
  assert.deepEqual((toolCalls.data as any).tool_calls, []);
  const oldTask = await queries.findByText({ text: "task recovered inside original turn" });
  assert.equal(oldTask.status, "not_found");
  const raw = await queries.keywordSearch({
    session_id: sessionId,
    query: "steer input inside original turn",
    scope: "raw_events"
  });
  assert.equal((raw.data as any).results.length, 1);
});

test("archive scope follows session file moves and deletions", async (t) => {
  const env = createFixtureHome(t);
  const activeFile = path.join(env.activeDir, "active.jsonl");
  const archivedFile = path.join(env.archivedDir, "archived.jsonl");
  const movedArchivedFile = path.join(env.archivedDir, "active-moved.jsonl");
  writeMiniSession(activeFile, "active-session", "active only keyword");
  writeMiniSession(archivedFile, "archived-session", "archived only keyword");
  const { queries, indexer } = openFixture(env);
  await indexer.sync({ rebuild: true, force: true });

  const active = await queries.listSessions({ archive_scope: "active" });
  const archived = await queries.listSessions({ archive_scope: "archived" });
  assert.deepEqual((active.data as any).sessions.map((session: any) => session.id), ["active-session"]);
  assert.deepEqual((archived.data as any).sessions.map((session: any) => session.id), ["archived-session"]);

  fs.renameSync(activeFile, movedArchivedFile);
  await indexer.sync({ force: true });
  const afterMoveActive = await queries.listSessions({ archive_scope: "active" });
  const afterMoveArchived = await queries.listSessions({ archive_scope: "archived", order: "asc" });
  assert.deepEqual((afterMoveActive.data as any).sessions.map((session: any) => session.id), []);
  assert.deepEqual((afterMoveArchived.data as any).sessions.map((session: any) => session.id).sort(), ["active-session", "archived-session"]);

  fs.unlinkSync(movedArchivedFile);
  await indexer.sync({ force: true });
  const afterDeleteArchived = await queries.listSessions({ archive_scope: "archived" });
  assert.deepEqual((afterDeleteArchived.data as any).sessions.map((session: any) => session.id), ["archived-session"]);
});

test("periodic polling indexes appended JSONL bytes even when mtime does not change", async (t) => {
  const env = createFixtureHome(t);
  const sessionId = "polling-session";
  const filePath = path.join(env.activeDir, "polling.jsonl");
  writeMiniSession(filePath, sessionId, "before polling append");
  const scheduler = new ManualScheduler();
  const { db, indexer } = openFixture(env, {
    createIndexer(db) {
      return new CountingIndexer(db, runtimePaths(env), { scheduler });
    }
  });

  indexer.start();
  assert.equal(await indexer.waitForIdle(1_000), true);
  await Promise.resolve();
  const originalTimes = fs.statSync(filePath);
  appendLines(filePath, [messageLine("user", "appended without mtime")]);
  fs.utimesSync(filePath, originalTimes.atime, originalTimes.mtime);

  scheduler.runAll();
  assert.equal(await indexer.waitForIdle(1_000), true);
  const texts = db
    .prepare("SELECT content_text FROM messages WHERE session_id = ? ORDER BY sequence")
    .all(sessionId) as Array<{ content_text: string }>;
  assert.deepEqual(texts.map((row) => row.content_text), ["before polling append", "appended without mtime"]);
});

test("query readiness starts at most one sync per check interval", async (t) => {
  const env = createFixtureHome(t);
  writeMiniSession(path.join(env.activeDir, "one.jsonl"), "session-one", "stable query throttle text");
  let now = 1_000;
  let countingIndexer: CountingIndexer | undefined;
  const { queries, indexer } = openFixture(env, {
    createIndexer(db) {
      countingIndexer = new CountingIndexer(db, runtimePaths(env), {
        syncCheckIntervalMs: 60_000,
        nowMs: () => now
      });
      return countingIndexer;
    }
  });
  await indexer.sync({ rebuild: true, force: true });
  assert.equal(countingIndexer?.syncCalls, 1);

  now += 1_000;
  await queries.listSessions({ archive_scope: "active" });
  await queries.listSessions({ archive_scope: "active" });
  assert.equal(countingIndexer?.syncCalls, 1);

  now += 60_000;
  await queries.listSessions({ archive_scope: "active" });
  assert.equal(countingIndexer?.syncCalls, 2);
});

function runtimePaths(env: { codexHome: string; dbPath: string }): RuntimePaths {
  return resolveRuntimePaths({ codexHome: env.codexHome, indexDbPath: env.dbPath });
}

function forkParentLines(sessionId: string): Record<string, unknown>[] {
  return [
    sessionMetaLine(sessionId, undefined, "2026-06-07T00:00:00.000Z"),
    eventLine("task_started", { turn_id: "retained-turn" }),
    messageLine("user", "retained input"),
    messageLine("assistant", "retained answer"),
    eventLine("task_complete", { turn_id: "retained-turn" }),
    eventLine("task_started", { turn_id: "removed-turn-1" }),
    messageLine("user", "removed input one"),
    messageLine("assistant", "removed answer one"),
    eventLine("task_complete", { turn_id: "removed-turn-1" }),
    eventLine("task_started", { turn_id: "removed-turn-2" }),
    messageLine("user", "removed input two"),
    messageLine("assistant", "removed answer two"),
    eventLine("task_complete", { turn_id: "removed-turn-2" })
  ];
}

function sessionMetaLine(
  sessionId: string,
  parentSessionId: string | undefined,
  timestamp: string
): Record<string, unknown> {
  return {
    timestamp,
    type: "session_meta",
    payload: {
      id: sessionId,
      ...(parentSessionId ? { forked_from_id: parentSessionId } : {}),
      timestamp,
      cwd: "C:\\fixture"
    }
  };
}

function eventLine(type: string, payload: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T01:00:00.002Z",
    type: "event_msg",
    payload: { type, ...payload }
  };
}

function messageLine(role: "user" | "assistant", text: string): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T01:00:00.003Z",
    type: "response_item",
    payload: {
      type: "message",
      role,
      content: [{ type: role === "user" ? "input_text" : "output_text", text }]
    }
  };
}

function toolCallLine(callId: string): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T01:00:00.003Z",
    type: "response_item",
    payload: {
      type: "function_call",
      name: "functions.shell_command",
      arguments: "{\"command\":\"date\"}",
      call_id: callId
    }
  };
}

function toolOutputLine(callId: string, output: string): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T01:00:00.003Z",
    type: "response_item",
    payload: { type: "function_call_output", call_id: callId, output }
  };
}

function taskRetrievalLine(
  sessionId: string,
  token: string,
  task: string
): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T01:00:00.003Z",
    type: "event_msg",
    payload: {
      type: "mcp_tool_call_end",
      call_id: `${sessionId}-task-retrieval`,
      invocation: {
        server: "codex_session_context",
        tool: "codex_session_get_task",
        arguments: { token }
      },
      result: {
        Ok: {
          content: [
            { type: "text", text: JSON.stringify({ status: "ok", data: { token, task } }) }
          ]
        }
      }
    }
  };
}

function completedMcpCallLine(
  id: string,
  tool: string,
  args: Record<string, unknown>,
  result: Record<string, unknown>
): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T01:00:00.003Z",
    type: "event_msg",
    payload: {
      type: "item_completed",
      turn_id: "new-format-turn",
      item: {
        type: "McpToolCall",
        id,
        server: "codex_session_context",
        tool,
        arguments: args,
        status: "completed",
        result: {
          content: [{ type: "text", text: JSON.stringify(result) }]
        }
      }
    }
  };
}

function paginatedMetaLine(
  threadId: string,
  ordinal: number,
  historyBase?: { thread_id: string; end_ordinal_exclusive: number; end_byte_offset: number }
): Record<string, unknown> {
  return {
    timestamp: "2026-06-07T00:00:00.000Z",
    ordinal,
    type: "session_meta",
    payload: {
      session_id: threadId,
      id: threadId,
      timestamp: "2026-06-07T00:00:00.000Z",
      history_mode: "paginated",
      history_base: historyBase
    }
  };
}

function serializedPrefixBytes(lines: Record<string, unknown>[], count: number): number {
  return Buffer.byteLength(`${lines.slice(0, count).map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function writeLines(filePath: string, lines: Record<string, unknown>[]): void {
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function appendLines(filePath: string, lines: Record<string, unknown>[]): void {
  fs.appendFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}
