import assert from "node:assert/strict";
import net from "node:net";
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

test("stdio clients share one backend, reconnect after backend exit, and release it when idle", async (t) => {
  const env = createFixtureHome(t);
  const port = await availablePort();
  const clientA = createStdioClient(env, port, "bridge-client-a");
  const clientB = createStdioClient(env, port, "bridge-client-b");
  deferClient(env, clientA.client);
  deferClient(env, clientB.client);

  await Promise.all([
    withTimeout(clientA.client.connect(clientA.transport), 5_000, "first MCP initialize should complete"),
    withTimeout(clientB.client.connect(clientB.transport), 5_000, "second MCP initialize should complete")
  ]);
  const [statusA, statusB] = await Promise.all([
    callToolJson(clientA.client, "codex_session_status", {}),
    callToolJson(clientB.client, "codex_session_status", {})
  ]);
  assert.equal(statusA.backend_pid, statusB.backend_pid);

  const published = await callToolJson(clientA.client, "codex_session_publish_task", { task: "survives backend restart" });
  process.kill(statusA.backend_pid);
  const restarted = await waitForHealth(port, (health) => health.pid !== statusA.backend_pid, 5_000);
  assert.notEqual(restarted.pid, statusA.backend_pid);
  const recovered = await retryToolCall(
    () => callToolJson(clientB.client, "codex_session_get_task", { token: published.data.token }),
    5_000
  );
  assert.equal(recovered.data.task, "survives backend restart");

  await Promise.all([clientA.client.close(), clientB.client.close()]);
  await waitForProcessExit(restarted.pid, 3_000);
});

test("stdio get_task adds recovery guidance only after the first retrieval", async (t) => {
  const env = createFixtureHome(t);
  const port = await availablePort();
  const { client, transport } = createStdioClient(env, port, "repeated-task-retrieval-test");
  deferClient(env, client);

  await withTimeout(client.connect(transport), 5_000, "MCP initialize should complete");
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
  const status = await callToolJson(client, "codex_session_status", {});
  await client.close();
  await waitForProcessExit(status.backend_pid, 3_000);
});

test("queries return an indexing envelope without waiting for an active sync", async (t) => {
  const env = createFixtureHome(t);
  const db = openDatabase(env.dbPath);
  deferDatabase(env, db);
  const neverCompletes = new Promise<SyncResult>(() => undefined);
  let localIdle = false;
  const status = {
    indexing: true,
    started_at: "2026-06-07T00:00:00.000Z",
    completed_at: null,
    files_seen: 0,
    files_indexed: 0,
    events_indexed: 0,
    error: null,
    backend_pid: process.pid
  };
  const indexer = {
    status: () => status,
    sync: () => neverCompletes,
    syncIfNeeded: () => undefined,
    waitForIdle: async () => localIdle
  };
  const queries = new CodexSessionQueries({ db, indexer, waitForIdleMs: 20 });

  const listed = await withTimeout(queries.listSessions({ archive_scope: "active" }), 200, "listSessions should not wait for sync");
  localIdle = true;
  const listedFromNonLeader = await withTimeout(
    queries.listSessions({ archive_scope: "active" }),
    200,
    "listSessions should honor another process's active sync"
  );
  const sync = await withTimeout(queries.sync({}), 200, "sync should return before the request timeout");
  assert.deepEqual(listed, { status: "indexing", data: status });
  assert.deepEqual(listedFromNonLeader, { status: "indexing", data: status });
  assert.deepEqual(sync, { status: "indexing", data: status });
});

function createStdioClient(env: ReturnType<typeof createFixtureHome>, port: number, name: string): {
  client: Client;
  transport: StdioClientTransport;
} {
  return {
    client: new Client({ name, version: "0.1.0" }),
    transport: new StdioClientTransport({
      command: process.execPath,
      args: [path.join(projectRoot, "dist", "src", "server.js")],
      cwd: projectRoot,
      env: {
        ...process.env,
        CODEX_HOME: env.codexHome,
        CODEX_SESSION_MCP_DB: env.dbPath,
        CODEX_SESSION_MCP_PORT: String(port),
        CODEX_SESSION_MCP_IDLE_TIMEOUT_MS: "250"
      },
      stderr: "pipe"
    })
  };
}

async function availablePort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("could not allocate a local test port");
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return address.port;
}

async function waitForHealth(
  port: number,
  accept: (health: { pid: number }) => boolean,
  timeoutMs: number
): Promise<{ pid: number }> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      const health = await response.json() as { pid: number };
      if (response.ok && accept(health)) return health;
    } catch {}
    await delay(50);
  }
  throw new Error("backend health did not reach the expected state");
}

async function waitForProcessExit(pid: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      return;
    }
    await delay(50);
  }
  throw new Error("backend process did not exit after the idle timeout");
}

async function retryToolCall<T>(call: () => Promise<T>, timeoutMs: number): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      return await call();
    } catch (error) {
      lastError = error;
      await delay(50);
    }
  }
  throw lastError;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
