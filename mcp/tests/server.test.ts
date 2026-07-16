import assert from "node:assert/strict";
import Database from "better-sqlite3";
import path from "node:path";
import test from "node:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { openDatabase } from "../src/db.js";
import { CodexSessionQueries } from "../src/query.js";
import type { SyncResult } from "../src/types.js";
import {
  callToolJson,
  createFixtureHome,
  deferClient,
  deferDatabase,
  projectRoot,
  withTimeout
} from "./test-fixtures.js";

test("stdio server lists tools while the index database has a locked writer", async (t) => {
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

  const client = new Client({ name: "locked-db-test", version: "0.1.0" });
  deferClient(env, client);
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

  await withTimeout(client.connect(transport), 2_000, "MCP initialize should complete while sqlite is locked");
  const tools = await withTimeout(client.listTools(), 2_000, "MCP listTools should complete while sqlite is locked");
  assert.equal(tools.tools.some((tool) => tool.name === "codex_session_status"), true);
});

test("stdio server releases its leader lease when the client closes", async (t) => {
  const env = createFixtureHome(t);
  const client = new Client({ name: "lease-release-test", version: "0.1.0" });
  deferClient(env, client);
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
  deferDatabase(env, db);
  await assertEventually(() => {
    const lease = db.prepare("SELECT * FROM leader_lease WHERE singleton_key = 'codex-session-mcp'").get();
    assert.equal(lease, undefined);
  }, 2_000);
});

test("stdio get_task adds recovery guidance only after the first retrieval", async (t) => {
  const env = createFixtureHome(t);
  const client = new Client({ name: "repeated-task-retrieval-test", version: "0.1.0" });
  deferClient(env, client);
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
  const published = await callToolJson(client, "codex_session_publish_task", { task: "Recover this task." });
  assert.equal(published.data.prompt, `token: ${published.data.token}, use codex_session_get_task tool to retrieve exact instruction`);
  assert.equal(typeof published.data.comment, "string");
  assert.ok(published.data.comment.trim().length > 0);

  const first = await callToolJson(client, "codex_session_get_task", { token: published.data.token });
  const second = await callToolJson(client, "codex_session_get_task", { token: published.data.token });
  assert.deepEqual(first.data, { token: published.data.token, task: "Recover this task." });
  assert.equal(second.data.token, published.data.token);
  assert.equal(second.data.task, "Recover this task.");
  assert.equal(typeof second.data.comment, "string");
  assert.ok(second.data.comment.trim().length > 0);
});

test("queries return an indexing envelope without waiting for an active sync", async (t) => {
  const env = createFixtureHome(t);
  const db = openDatabase(env.dbPath);
  deferDatabase(env, db);
  const neverCompletes = new Promise<SyncResult>(() => undefined);
  const status = {
    indexing: true,
    started_at: "2026-06-07T00:00:00.000Z",
    completed_at: null,
    files_seen: 0,
    files_indexed: 0,
    events_indexed: 0,
    error: null,
    leader: undefined,
    holder_id: "test-holder",
    is_leader: true,
    database_busy: false,
    database_busy_at: null
  };
  const indexer = {
    status: () => status,
    sync: () => neverCompletes,
    syncIfNeeded: () => undefined,
    waitForIdle: async () => false
  };
  const queries = new CodexSessionQueries({ db, indexer, waitForIdleMs: 20 });

  const listed = await withTimeout(queries.listSessions({ archive_scope: "active" }), 200, "listSessions should not wait for sync");
  const sync = await withTimeout(queries.sync({}), 200, "sync should return before the request timeout");
  assert.deepEqual(listed, { status: "indexing", data: status });
  assert.deepEqual(sync, { status: "indexing", data: status });
});

async function assertEventually(assertion: () => void, timeoutMs: number): Promise<void> {
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
