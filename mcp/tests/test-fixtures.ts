import Database from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import type { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { openDatabase } from "../src/db.js";
import { CodexSessionIndexer } from "../src/indexer.js";
import { resolveRuntimePaths } from "../src/paths.js";
import { CodexSessionQueries } from "../src/query.js";
import { ensureDir } from "../src/util.js";

type Cleanup = () => void | Promise<void>;

export interface FixtureHome {
  root: string;
  codexHome: string;
  activeDir: string;
  archivedDir: string;
  dbPath: string;
  defer(cleanup: Cleanup): void;
}

export const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const samplePath = path.join(projectRoot, "samples", "current-session.redacted.jsonl");

export function createFixtureHome(t: TestContext): FixtureHome {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-session-mcp-test-"));
  const codexHome = path.join(root, ".codex");
  const activeDir = path.join(codexHome, "sessions", "2026", "06", "07");
  const archivedDir = path.join(codexHome, "archived_sessions");
  const cleanups: Cleanup[] = [];
  ensureDir(activeDir);
  ensureDir(archivedDir);

  t.after(async () => {
    let cleanupError: unknown;
    for (const cleanup of cleanups.reverse()) {
      try {
        await cleanup();
      } catch (error) {
        cleanupError ??= error;
      }
    }
    try {
      fs.rmSync(root, { recursive: true, force: true });
    } catch (error) {
      cleanupError ??= error;
    }
    if (cleanupError) throw cleanupError;
  });

  return {
    root,
    codexHome,
    activeDir,
    archivedDir,
    dbPath: path.join(root, "index.sqlite"),
    defer(cleanup) {
      cleanups.push(cleanup);
    }
  };
}

export function openFixture(
  env: FixtureHome,
  options: {
    waitForIdleMs?: number;
    indexerOptions?: ConstructorParameters<typeof CodexSessionIndexer>[2];
    createIndexer?: (db: Database.Database) => CodexSessionIndexer;
  } = {}
): {
  db: Database.Database;
  indexer: CodexSessionIndexer;
  queries: CodexSessionQueries;
} {
  const paths = resolveRuntimePaths({ codexHome: env.codexHome, indexDbPath: env.dbPath });
  const db = openDatabase(paths.indexDbPath);
  const indexer = options.createIndexer?.(db) ?? new CodexSessionIndexer(db, paths, options.indexerOptions);
  const queries = new CodexSessionQueries({ db, indexer, waitForIdleMs: options.waitForIdleMs });
  let disposed = false;
  env.defer(async () => {
    if (disposed) return;
    disposed = true;
    try {
      await indexer.stop();
    } finally {
      db.close();
    }
  });
  return { db, indexer, queries };
}

export function deferDatabase(env: FixtureHome, db: Database.Database): void {
  env.defer(() => {
    if (db.open) db.close();
  });
}

export function deferClient(env: FixtureHome, client: Client): void {
  env.defer(() => client.close().catch(() => undefined));
}

export function writeMiniSession(filePath: string, sessionId: string, userText: string): void {
  writeJsonl(filePath, [
    sessionMeta(sessionId, "project"),
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
  ]);
}

export function writeForkedSession(filePath: string, sessionId: string, parentSessionId: string): void {
  writeJsonl(filePath, [
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
  ]);
}

export function writeSessionMeta(filePath: string, sessionId: string): void {
  writeJsonl(filePath, [sessionMeta(sessionId, "locator")]);
}

export function appendLocatorMarkerEvent(filePath: string, sessionId: string, token: string, marker: string): void {
  appendJsonl(filePath, {
    timestamp: "2026-06-07T00:00:01.000Z",
    type: "event_msg",
    payload: {
      type: "mcp_tool_call_end",
      call_id: `${sessionId}-token-call`,
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
  });
}

export function writeTaskInputSession(filePath: string, sessionId: string, token: string, task: string, repeatTask = false): void {
  const lines: unknown[] = [
    sessionMeta(sessionId, "task-input"),
    {
      timestamp: "2026-06-07T00:00:01.000Z",
      type: "response_item",
      payload: { type: "message", role: "user", content: [{ type: "input_text", text: "ordinary input before task" }] }
    },
    taskRetrievalEvent(sessionId, token, task, 2, "get-task"),
    {
      timestamp: "2026-06-07T00:00:03.000Z",
      type: "response_item",
      payload: { type: "message", role: "user", content: [{ type: "input_text", text: "ordinary input after task" }] }
    }
  ];
  if (repeatTask) lines.push(taskRetrievalEvent(sessionId, token, task, 4, "get-task-repeat"));
  writeJsonl(filePath, lines);
}

export function writeRecoverySession(filePath: string, sessionId: string): void {
  writeJsonl(filePath, [
    sessionMeta(sessionId, "recovery", "2026-06-07T13:00:00.000Z"),
    userMessage("2026-06-07T13:01:00.000Z", "Remember the initial context."),
    userMessage("2026-06-07T13:02:00.000Z", "PLEASE IMPLEMENT THIS PLAN:\nBuild the local session index."),
    userMessage("2026-06-07T13:03:00.000Z", "A new MCP process failed with SqliteError: database is locked."),
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
    userMessage("2026-06-07T13:05:00.000Z", "The tool stalled; inspect the lock handling.")
  ]);
}

export async function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
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

export async function callToolJson(client: Client, name: string, args: Record<string, unknown>): Promise<any> {
  const result = await withTimeout(client.callTool({ name, arguments: args }), 2_000, `${name} should return`);
  const content = result.content as Array<{ type: string; text?: string }>;
  if (content[0]?.type !== "text") throw new Error(`${name} did not return JSON text content`);
  return JSON.parse(content[0].text ?? "{}");
}

function sessionMeta(sessionId: string, directory: string, timestamp = "2026-06-07T00:00:00.000Z"): Record<string, unknown> {
  return {
    timestamp,
    type: "session_meta",
    payload: { id: sessionId, timestamp, cwd: `C:\\Users\\<USER>\\${directory}` }
  };
}

function userMessage(timestamp: string, text: string): Record<string, unknown> {
  return {
    timestamp,
    type: "response_item",
    payload: { type: "message", role: "user", content: [{ type: "input_text", text }] }
  };
}

function taskRetrievalEvent(sessionId: string, token: string, task: string, second: number, suffix: string): Record<string, unknown> {
  return {
    timestamp: `2026-06-07T00:00:0${second}.000Z`,
    type: "event_msg",
    payload: {
      type: "mcp_tool_call_end",
      call_id: `${sessionId}-${suffix}`,
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
  };
}

function writeJsonl(filePath: string, lines: unknown[]): void {
  fs.writeFileSync(filePath, `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`, "utf8");
}

function appendJsonl(filePath: string, line: unknown): void {
  fs.appendFileSync(filePath, `${JSON.stringify(line)}\n`, "utf8");
}
