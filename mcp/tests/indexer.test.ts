import assert from "node:assert/strict";
import Database from "better-sqlite3";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { openDatabase } from "../src/db.js";
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

  setInterval(callback: () => void): NodeJS.Timeout {
    const handle = {} as NodeJS.Timeout;
    this.callbacks.set(handle, callback);
    return handle;
  }

  clearInterval(handle: NodeJS.Timeout): void {
    this.callbacks.delete(handle);
  }

  renewAll(): void {
    for (const callback of [...this.callbacks.values()]) callback();
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

test("indexer startup survives a locked sqlite writer and reports sync failure", async (t) => {
  const env = createFixtureHome(t);
  const setupDb = openDatabase(env.dbPath, { busyTimeoutMs: 20 });
  setupDb.close();

  const blocker = new Database(env.dbPath, { timeout: 20 });
  blocker.pragma("busy_timeout = 20");
  blocker.exec("BEGIN IMMEDIATE");
  env.defer(() => {
    if (!blocker.open) return;
    blocker.exec("ROLLBACK");
    blocker.close();
  });

  const paths = resolveRuntimePaths({ codexHome: env.codexHome, indexDbPath: env.dbPath });
  const db = openDatabase(paths.indexDbPath, { busyTimeoutMs: 20 });
  deferDatabase(env, db);
  const indexer = new CodexSessionIndexer(db, paths);
  env.defer(() => indexer.stop());

  assert.doesNotThrow(() => indexer.start());
  await assert.rejects(() => indexer.sync({ force: true }), /SQLite index database is locked/);
});

test("indexer stop releases its leader lease", async (t) => {
  const env = createFixtureHome(t);
  const { db, indexer } = openFixture(env);
  indexer.start();
  assert.equal(indexer.status().is_leader, true);

  await indexer.stop();
  const lease = db.prepare("SELECT * FROM leader_lease WHERE holder_id = ?").get(indexer.holderId);
  assert.equal(lease, undefined);
});

test("leader renewal does not start another sync", async (t) => {
  const env = createFixtureHome(t);
  const scheduler = new ManualScheduler();
  let countingIndexer: CountingIndexer | undefined;
  const { indexer } = openFixture(env, {
    createIndexer(db) {
      countingIndexer = new CountingIndexer(db, runtimePaths(env), {
        runSyncInProcess: true,
        scheduler
      });
      return countingIndexer;
    }
  });

  indexer.start();
  assert.equal(await indexer.waitForIdle(1_000), true);
  assert.equal(countingIndexer?.syncCalls, 1);

  scheduler.renewAll();
  scheduler.renewAll();
  assert.equal(countingIndexer?.syncCalls, 1);
});

test("query readiness starts at most one sync per check interval", async (t) => {
  const env = createFixtureHome(t);
  writeMiniSession(path.join(env.activeDir, "one.jsonl"), "session-one", "stable query throttle text");
  let now = 1_000;
  let countingIndexer: CountingIndexer | undefined;
  const { queries, indexer } = openFixture(env, {
    createIndexer(db) {
      countingIndexer = new CountingIndexer(db, runtimePaths(env), {
        runSyncInProcess: true,
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
