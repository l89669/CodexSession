import fs from "node:fs";
import path from "node:path";
import { Worker } from "node:worker_threads";
import { watch, FSWatcher } from "chokidar";
import { Db, deleteFileRows, deleteSessionRows, isSqliteBusy } from "./db.js";
import { RuntimePaths, toPortablePath } from "./paths.js";
import { parseSessionFile } from "./parser.js";
import { readSessionIndex } from "./session-index.js";
import { ArchiveScope, IndexingStatus, SessionFile, SyncResult } from "./types.js";
import { ensureDir, listJsonlFiles, nowIso } from "./util.js";
import { LeaderLease } from "./leader.js";

export class CodexSessionIndexer {
  private readonly db: Db;
  private readonly paths: RuntimePaths;
  private readonly lease: LeaderLease;
  private readonly runSyncInProcess: boolean;
  private readonly syncCheckIntervalMs: number;
  private readonly leaderRenewIntervalMs: number;
  private watcher: FSWatcher | undefined;
  private syncPromise: Promise<SyncResult> | undefined;
  private activeWorker: Worker | undefined;
  private leaderTimer: NodeJS.Timeout | undefined;
  private debounceTimer: NodeJS.Timeout | undefined;
  private leadershipActive = false;
  private lastSyncCheckMs = 0;
  private databaseBusyAt: string | null = null;
  private databaseBusyError: string | null = null;

  constructor(
    db: Db,
    paths: RuntimePaths,
    options: {
      holderId?: string;
      leaseMs?: number;
      runSyncInProcess?: boolean;
      syncCheckIntervalMs?: number;
      leaderRenewIntervalMs?: number;
    } = {}
  ) {
    this.db = db;
    this.paths = paths;
    this.lease = new LeaderLease(db, { holderId: options.holderId, leaseMs: options.leaseMs });
    this.runSyncInProcess = Boolean(options.runSyncInProcess);
    this.syncCheckIntervalMs = options.syncCheckIntervalMs ?? 5_000;
    this.leaderRenewIntervalMs = options.leaderRenewIntervalMs ?? 5_000;
  }

  get holderId(): string {
    return this.lease.holderId;
  }

  start(): void {
    this.tryBecomeLeader();
    this.leaderTimer = setInterval(() => {
      this.tryBecomeLeader();
    }, this.leaderRenewIntervalMs);
  }

  async stop(): Promise<void> {
    if (this.leaderTimer) clearInterval(this.leaderTimer);
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    if (this.watcher) await this.watcher.close();
    const runningSync = this.syncPromise;
    if (this.activeWorker) {
      await this.activeWorker.terminate();
    }
    if (runningSync) {
      await runningSync.catch(() => undefined);
      this.recordSyncError(new Error("index sync stopped before completion"));
    }
    try {
      this.lease.release();
    } catch (error) {
      if (!isSqliteBusy(error)) throw error;
      this.recordBusy(error);
    }
  }

  status(): IndexingStatus & {
    leader: Record<string, unknown> | undefined;
    holder_id: string;
    is_leader: boolean;
    database_busy: boolean;
    database_busy_at: string | null;
  } {
    try {
      const row = this.db.prepare("SELECT * FROM sync_status WHERE singleton_key = 'main'").get() as
        | {
            indexing: number;
            started_at: string | null;
            completed_at: string | null;
            files_seen: number;
            files_indexed: number;
            events_indexed: number;
            error: string | null;
          }
        | undefined;
      const leader = this.lease.current();
      const isLeader = this.lease.isLeader();
      const wasBusyAt = this.databaseBusyAt;
      this.clearBusy();
      return {
        indexing: Boolean(row?.indexing),
        started_at: row?.started_at ?? null,
        completed_at: row?.completed_at ?? null,
        files_seen: row?.files_seen ?? 0,
        files_indexed: row?.files_indexed ?? 0,
        events_indexed: row?.events_indexed ?? 0,
        error: row?.error ?? null,
        leader,
        holder_id: this.holderId,
        is_leader: isLeader,
        database_busy: false,
        database_busy_at: wasBusyAt
      };
    } catch (error) {
      if (!isSqliteBusy(error)) throw error;
      return this.busyStatus(error);
    }
  }

  async waitForIdle(maxMs = 5_000): Promise<boolean> {
    if (!this.syncPromise) return true;
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        this.syncPromise,
        new Promise((resolve) => {
          timer = setTimeout(resolve, maxMs);
        })
      ]);
      return !this.syncPromise;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  async sync(options: { rebuild?: boolean; force?: boolean } = {}): Promise<SyncResult> {
    this.lastSyncCheckMs = Date.now();
    let canWrite: boolean;
    try {
      canWrite = this.lease.isLeader() || this.lease.acquireOrRenew();
    } catch (error) {
      if (!isSqliteBusy(error)) throw error;
      this.recordBusy(error);
      throw new Error(`SQLite index database is locked; retry after the current writer releases it: ${this.databaseBusyError}`);
    }
    if (!canWrite) {
      const status = this.status();
      throw new Error(`Another MCP server instance holds the index writer lease: ${JSON.stringify(status.leader)}`);
    }
    if (this.syncPromise) return this.syncPromise;
    this.syncPromise = this.runSyncInBackground(options).finally(() => {
      this.lastSyncCheckMs = Date.now();
      this.syncPromise = undefined;
    });
    return this.syncPromise;
  }

  syncIfNeeded(): void {
    const now = Date.now();
    if (now - this.lastSyncCheckMs < this.syncCheckIntervalMs) return;
    this.lastSyncCheckMs = now;
    let canWrite: boolean;
    try {
      canWrite = this.lease.isLeader() || this.lease.acquireOrRenew();
    } catch (error) {
      if (!isSqliteBusy(error)) throw error;
      this.recordBusy(error);
      return;
    }
    if (!canWrite) return;
    if (this.syncPromise) return;
    void this.sync({ force: false }).catch((error) => this.recordSyncError(error));
  }

  private tryBecomeLeader(): void {
    const wasLeader = this.leadershipActive;
    let hasLeadership: boolean;
    try {
      hasLeadership = this.lease.acquireOrRenew();
    } catch (error) {
      if (!isSqliteBusy(error)) throw error;
      this.recordBusy(error);
      return;
    }
    this.leadershipActive = hasLeadership;
    if (!hasLeadership) return;
    this.clearBusy();
    if (!this.watcher) this.startWatcher();
    if (wasLeader) return;
    void this.sync({ force: false }).catch((error) => this.recordSyncError(error));
  }

  private startWatcher(): void {
    ensureDir(this.paths.indexDir);
    const roots = [this.paths.sessionsDir, this.paths.archivedSessionsDir].filter((root) => fs.existsSync(root));
    if (roots.length === 0) return;
    this.watcher = watch(roots, {
      ignoreInitial: true,
      depth: 10,
      awaitWriteFinish: { stabilityThreshold: 750, pollInterval: 100 }
    });
    const schedule = () => {
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(() => {
        void this.sync({ force: false }).catch((error) => this.recordSyncError(error));
      }, 300);
    };
    this.watcher.on("add", schedule);
    this.watcher.on("change", schedule);
    this.watcher.on("unlink", schedule);
  }

  private runSyncInBackground(options: { rebuild?: boolean; force?: boolean }): Promise<SyncResult> {
    if (this.runSyncInProcess) {
      return Promise.resolve().then(() => this.runSync(options));
    }

    return new Promise((resolve, reject) => {
      const worker = new Worker(new URL("./sync-worker.js", import.meta.url), {
        workerData: {
          codexHome: this.paths.codexHome,
          indexDbPath: this.paths.indexDbPath,
          holderId: this.holderId,
          options
        }
      });
      this.activeWorker = worker;
      let settled = false;

      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
        if (this.activeWorker === worker) this.activeWorker = undefined;
        callback();
      };

      worker.once("message", (message: unknown) => {
        const response = message as { ok?: boolean; result?: SyncResult; error?: string };
        if (response?.ok && response.result) {
          finish(() => resolve(response.result as SyncResult));
          return;
        }
        finish(() => reject(new Error(response?.error ?? "index sync worker failed without an error message")));
      });
      worker.once("error", (error) => {
        finish(() => reject(error));
      });
      worker.once("exit", (code) => {
        if (code === 0) {
          finish(() => reject(new Error("index sync worker exited without returning a result")));
          return;
        }
        finish(() => reject(new Error(`index sync worker exited with code ${code}`)));
      });
    });
  }

  private runSync(options: { rebuild?: boolean; force?: boolean }): SyncResult {
    const started = nowIso();
    const result: SyncResult = {
      started_at: started,
      completed_at: started,
      files_seen: 0,
      files_indexed: 0,
      files_deleted: 0,
      events_indexed: 0,
      messages_indexed: 0,
      tool_calls_indexed: 0
    };

    try {
      this.markSyncStart(started);
      if (options.rebuild) {
        this.db.exec("DELETE FROM sessions;");
      }

      const discovered = this.discoverFiles();
      result.files_seen = discovered.length;
      const discoveredPaths = new Set(discovered.map((file) => file.filePath));
      const indexedFiles = this.db.prepare("SELECT file_path FROM session_files").all() as { file_path: string }[];
      for (const indexed of indexedFiles) {
        if (!discoveredPaths.has(indexed.file_path)) {
          deleteFileRows(this.db, indexed.file_path);
          result.files_deleted += 1;
        }
      }

      const indexEntries = readSessionIndex(this.paths.codexHome);
      for (const file of discovered) {
        const known = this.db.prepare("SELECT size, mtime_ms FROM session_files WHERE file_path = ?").get(file.filePath) as
          | { size: number; mtime_ms: number }
          | undefined;
        const changed = options.force || options.rebuild || !known || known.size !== file.size || known.mtime_ms !== file.mtimeMs;
        if (!changed) continue;

        const parsed = parseSessionFile(file.filePath);
        const sessionId = parsed.meta.id;
        const existing = this.db.prepare("SELECT file_path FROM sessions WHERE session_id = ?").get(sessionId) as
          | { file_path: string }
          | undefined;
        if (existing && existing.file_path !== file.filePath) {
          deleteSessionRows(this.db, sessionId);
        } else {
          deleteFileRows(this.db, file.filePath);
        }

        const indexEntry = indexEntries.get(sessionId);
        const updatedAt = indexEntry?.updated_at ? new Date(indexEntry.updated_at).toISOString() : new Date(file.mtimeMs).toISOString();
        const insertTransaction = this.db.transaction(() => {
          this.db
            .prepare(
              `INSERT INTO sessions (session_id, file_path, archive_scope, forked_from_id, created_at, updated_at, thread_name, cwd, meta_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
            )
            .run(
              sessionId,
              file.filePath,
              file.archiveScope,
              parsed.meta.forked_from_id ?? null,
              parsed.meta.timestamp ? new Date(parsed.meta.timestamp).toISOString() : null,
              updatedAt,
              indexEntry?.thread_name ?? null,
              parsed.meta.cwd ?? null,
              JSON.stringify(parsed.meta)
            );

          const rawIdByLine = new Map<number, number>();
          const rawInsert = this.db.prepare(
            `INSERT INTO raw_events (session_id, file_path, line_no, sequence, timestamp, event_type, payload_type, role, raw_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
          );
          const locatorInsert = this.db.prepare(
            `INSERT OR IGNORE INTO session_locator_tokens
             (token, session_id, archive_scope, sequence, timestamp, call_id, tool_name, source, raw_event_id, indexed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          );
          const taskInputInsert = this.db.prepare(
            `INSERT INTO session_task_inputs
             (session_id, sequence, timestamp, call_id, tool_name, token, task_text, raw_event_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
          );
          for (const raw of parsed.rawEvents) {
            const info = rawInsert.run(
              sessionId,
              file.filePath,
              raw.lineNo,
              raw.sequence,
              raw.timestamp,
              raw.eventType,
              raw.payloadType,
              raw.role,
              raw.rawJson
            );
            const rawEventId = Number(info.lastInsertRowid);
            rawIdByLine.set(raw.lineNo, rawEventId);
            for (const locator of locatorTokensFromMcpToolCallEnd(raw)) {
              locatorInsert.run(
                locator.token,
                sessionId,
                file.archiveScope,
                locator.sequence,
                locator.timestamp,
                locator.callId,
                locator.toolName,
                "tool_output",
                rawEventId,
                nowIso()
              );
            }
            const taskInput = taskInputFromMcpToolCallEnd(raw);
            if (taskInput) {
              taskInputInsert.run(
                sessionId,
                taskInput.sequence,
                taskInput.timestamp,
                taskInput.callId,
                taskInput.toolName,
                taskInput.token,
                taskInput.task,
                rawEventId
              );
            }
          }

          const messageInsert = this.db.prepare(
            `INSERT INTO messages (session_id, sequence, timestamp, role, content_text, content_json, raw_event_id)
             VALUES (?, ?, ?, ?, ?, ?, ?)`
          );
          for (const message of parsed.messages) {
            messageInsert.run(
              sessionId,
              message.sequence,
              message.timestamp,
              message.role,
              message.contentText,
              message.contentJson,
              rawIdByLine.get(message.rawLineNo)
            );
          }

          const outputByCallId = new Map(parsed.toolOutputs.map((output) => [output.callId, output]));
          const toolInsert = this.db.prepare(
            `INSERT INTO tool_calls
             (session_id, sequence, timestamp, call_id, tool_name, arguments_json, output_sequence, output_timestamp, output_text, output_json, call_raw_event_id, output_raw_event_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          );
          for (const call of parsed.toolCalls) {
            const output = outputByCallId.get(call.callId);
            toolInsert.run(
              sessionId,
              call.sequence,
              call.timestamp,
              call.callId,
              call.toolName,
              call.argumentsJson,
              output?.sequence ?? null,
              output?.timestamp ?? null,
              output?.outputText ?? null,
              output?.outputJson ?? null,
              rawIdByLine.get(call.rawLineNo),
              output ? rawIdByLine.get(output.rawLineNo) : null
            );
            for (const locator of locatorTokensFromToolCall(call, output)) {
              const rawEventId =
                locator.source === "tool_output" && output ? rawIdByLine.get(output.rawLineNo) : rawIdByLine.get(call.rawLineNo);
              locatorInsert.run(
                locator.token,
                sessionId,
                file.archiveScope,
                locator.sequence,
                locator.timestamp,
                call.callId,
                call.toolName,
                locator.source,
                rawEventId ?? null,
                nowIso()
              );
            }
          }

          this.db
            .prepare(
              `INSERT INTO session_files (file_path, session_id, archive_scope, size, mtime_ms, line_count, indexed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)`
            )
            .run(file.filePath, sessionId, file.archiveScope, file.size, file.mtimeMs, parsed.lineCount, nowIso());
        });
        insertTransaction();
        result.files_indexed += 1;
        result.events_indexed += parsed.rawEvents.length;
        result.messages_indexed += parsed.messages.length;
        result.tool_calls_indexed += parsed.toolCalls.length;
      }

      result.completed_at = nowIso();
      this.markSyncComplete(result);
      this.clearBusy();
      return result;
    } catch (error) {
      this.recordSyncError(error);
      throw error;
    }
  }

  private discoverFiles(): SessionFile[] {
    const active = listJsonlFiles(this.paths.sessionsDir, true).map((filePath) => this.fileInfo(filePath, "active"));
    const archived = listJsonlFiles(this.paths.archivedSessionsDir, true).map((filePath) => this.fileInfo(filePath, "archived"));
    return [...active, ...archived];
  }

  private fileInfo(filePath: string, archiveScope: "active" | "archived"): SessionFile {
    const stat = fs.statSync(filePath);
    return {
      filePath: toPortablePath(filePath),
      archiveScope,
      mtimeMs: stat.mtimeMs,
      size: stat.size
    };
  }

  private markSyncStart(startedAt: string): void {
    this.db
      .prepare(
        `INSERT INTO sync_status (singleton_key, indexing, started_at, completed_at, files_seen, files_indexed, events_indexed, error)
         VALUES ('main', 1, ?, NULL, 0, 0, 0, NULL)
         ON CONFLICT(singleton_key) DO UPDATE SET
           indexing = 1,
           started_at = excluded.started_at,
           completed_at = NULL,
           files_seen = 0,
           files_indexed = 0,
           events_indexed = 0,
           error = NULL`
      )
      .run(startedAt);
  }

  private markSyncComplete(result: SyncResult): void {
    this.db
      .prepare(
        `UPDATE sync_status
         SET indexing = 0, completed_at = ?, files_seen = ?, files_indexed = ?, events_indexed = ?, error = NULL
         WHERE singleton_key = 'main'`
      )
      .run(result.completed_at, result.files_seen, result.files_indexed, result.events_indexed);
  }

  private markSyncError(error: unknown): void {
    this.db
      .prepare(
        `INSERT INTO sync_status (singleton_key, indexing, started_at, completed_at, files_seen, files_indexed, events_indexed, error)
         VALUES ('main', 0, NULL, ?, 0, 0, 0, ?)
         ON CONFLICT(singleton_key) DO UPDATE SET indexing = 0, completed_at = excluded.completed_at, error = excluded.error`
      )
      .run(nowIso(), error instanceof Error ? error.message : String(error));
  }

  private recordSyncError(error: unknown): void {
    if (isSqliteBusy(error)) this.recordBusy(error);
    try {
      this.markSyncError(error);
    } catch (inner) {
      if (!isSqliteBusy(inner)) throw inner;
      this.recordBusy(inner);
    }
  }

  private recordBusy(error: unknown): void {
    this.databaseBusyAt = nowIso();
    this.databaseBusyError = error instanceof Error ? error.message : String(error);
  }

  private clearBusy(): void {
    this.databaseBusyAt = null;
    this.databaseBusyError = null;
  }

  private busyStatus(error: unknown): IndexingStatus & {
    leader: undefined;
    holder_id: string;
    is_leader: false;
    database_busy: true;
    database_busy_at: string;
  } {
    this.recordBusy(error);
    return {
      indexing: false,
      started_at: null,
      completed_at: null,
      files_seen: 0,
      files_indexed: 0,
      events_indexed: 0,
      error: this.databaseBusyError,
      leader: undefined,
      holder_id: this.holderId,
      is_leader: false,
      database_busy: true,
      database_busy_at: this.databaseBusyAt ?? nowIso()
    };
  }
}

interface LocatorTokenOccurrence {
  token: string;
  sequence: number;
  timestamp: string | null;
  source: "tool_arguments" | "tool_output";
}

interface RawLocatorTokenOccurrence {
  token: string;
  sequence: number;
  timestamp: string | null;
  callId: string | null;
  toolName: string;
}

interface RawTaskInputOccurrence {
  sequence: number;
  timestamp: string | null;
  callId: string | null;
  toolName: string;
  token: string;
  task: string;
}

interface McpToolCallEnd {
  sequence: number;
  timestamp: string | null;
  callId: string | null;
  serverName: string;
  toolName: string;
  arguments: Record<string, unknown>;
  okResult: unknown;
}

const LOCATOR_TOKEN_MARKER = "codex-session-locator:";
const UUID_PATTERN = /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/gi;

function locatorTokensFromMcpToolCallEnd(raw: {
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  payload: Record<string, unknown>;
}): RawLocatorTokenOccurrence[] {
  const call = parseMcpToolCallEnd(raw);
  if (!call || !isCodexSessionServer(call.serverName) || !isGetSessionTokenTool(call.toolName)) return [];

  const resultJson = JSON.stringify(call.okResult);
  return extractMarkedLocatorTokens(resultJson).map((token) => ({
    token,
    sequence: call.sequence,
    timestamp: call.timestamp,
    callId: call.callId,
    toolName: call.toolName
  }));
}

function taskInputFromMcpToolCallEnd(raw: {
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  payload: Record<string, unknown>;
}): RawTaskInputOccurrence | undefined {
  const call = parseMcpToolCallEnd(raw);
  if (!call || !isCodexSessionServer(call.serverName) || !isGetTaskTool(call.toolName)) return undefined;

  const invocationToken = stringValue(call.arguments.token)?.trim().toLowerCase();
  if (!invocationToken || !isUuid(invocationToken)) return undefined;
  const taskResult = taskResultFromMcpOutput(call.okResult);
  if (!taskResult || taskResult.token !== invocationToken) return undefined;

  return {
    sequence: call.sequence,
    timestamp: call.timestamp,
    callId: call.callId,
    toolName: call.toolName,
    token: taskResult.token,
    task: taskResult.task
  };
}

function parseMcpToolCallEnd(raw: {
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  payload: Record<string, unknown>;
}): McpToolCallEnd | undefined {
  if (raw.eventType !== "event_msg" || raw.payloadType !== "mcp_tool_call_end") return undefined;
  const invocation = recordValue(raw.payload.invocation);
  const serverName = stringValue(invocation.server);
  const toolName = stringValue(invocation.tool);
  if (!serverName || !toolName) return undefined;
  const result = recordValue(raw.payload.result);
  if (!("Ok" in result)) return undefined;
  return {
    sequence: raw.sequence,
    timestamp: raw.timestamp,
    callId: stringValue(raw.payload.call_id) ?? null,
    serverName,
    toolName,
    arguments: recordValue(invocation.arguments),
    okResult: result.Ok
  };
}

function taskResultFromMcpOutput(okResult: unknown): { token: string; task: string } | undefined {
  const content = recordValue(okResult).content;
  if (!Array.isArray(content)) return undefined;
  for (const item of content) {
    const block = recordValue(item);
    if (block.type !== "text") continue;
    const text = stringValue(block.text);
    if (!text) continue;
    const envelope = recordValue(tryParseJson(text));
    if (envelope.status !== "ok") continue;
    const data = recordValue(envelope.data);
    const token = stringValue(data.token)?.trim().toLowerCase();
    const task = stringValue(data.task);
    if (token && isUuid(token) && task !== undefined) return { token, task };
  }
  return undefined;
}

function locatorTokensFromToolCall(
  call: { sequence: number; timestamp: string | null; toolName: string; argumentsJson: string | null },
  output: { sequence: number; timestamp: string | null; outputText: string; outputJson: string | null } | undefined
): LocatorTokenOccurrence[] {
  const tokens: LocatorTokenOccurrence[] = [];
  if (isGetSessionTokenTool(call.toolName) && output) {
    for (const token of extractMarkedLocatorTokens(output.outputText, output.outputJson)) {
      tokens.push({ token, sequence: output.sequence, timestamp: output.timestamp, source: "tool_output" });
    }
    for (const token of extractTokenFields(output.outputText, output.outputJson)) {
      tokens.push({ token, sequence: output.sequence, timestamp: output.timestamp, source: "tool_output" });
    }
  }
  if (isGetSessionByTokenTool(call.toolName)) {
    for (const token of extractTokenFields(call.argumentsJson)) {
      tokens.push({ token, sequence: call.sequence, timestamp: call.timestamp, source: "tool_arguments" });
    }
    for (const token of extractMarkedLocatorTokens(call.argumentsJson)) {
      tokens.push({ token, sequence: call.sequence, timestamp: call.timestamp, source: "tool_arguments" });
    }
  }

  const seen = new Set<string>();
  return tokens.filter((token) => {
    const key = `${token.token}:${token.sequence}:${token.source}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function isGetSessionTokenTool(toolName: string): boolean {
  return toolName === "codex_session_get_session_token" || toolName.endsWith(".codex_session_get_session_token");
}

function isCodexSessionServer(serverName: string): boolean {
  return serverName === "codex_session" || serverName === "codex_session_context";
}

function isGetSessionByTokenTool(toolName: string): boolean {
  return toolName === "codex_session_get_session_by_token" || toolName.endsWith(".codex_session_get_session_by_token");
}

function isGetTaskTool(toolName: string): boolean {
  return toolName === "codex_session_get_task" || toolName.endsWith(".codex_session_get_task");
}

function extractMarkedLocatorTokens(...texts: Array<string | null | undefined>): string[] {
  const tokens = new Set<string>();
  for (const text of texts) {
    if (!text) continue;
    const markerPattern = new RegExp(`${escapeRegExp(LOCATOR_TOKEN_MARKER)}(${UUID_PATTERN.source})`, "gi");
    for (const match of text.matchAll(markerPattern)) {
      tokens.add(match[1].toLowerCase());
    }
  }
  return [...tokens];
}

function extractTokenFields(...texts: Array<string | null | undefined>): string[] {
  const tokens = new Set<string>();
  for (const text of texts) {
    if (!text) continue;
    const parsed = tryParseJson(text);
    collectTokenFields(parsed, tokens);
    if (tokens.size === 0 && UUID_PATTERN.test(text)) {
      UUID_PATTERN.lastIndex = 0;
      for (const match of text.matchAll(UUID_PATTERN)) tokens.add(match[0].toLowerCase());
    }
    UUID_PATTERN.lastIndex = 0;
  }
  return [...tokens];
}

function collectTokenFields(value: unknown, tokens: Set<string>): void {
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    for (const item of value) collectTokenFields(item, tokens);
    return;
  }
  for (const [key, inner] of Object.entries(value)) {
    if (key === "token" && typeof inner === "string" && isUuid(inner)) {
      tokens.add(inner.toLowerCase());
    }
    if (key === "marker" && typeof inner === "string") {
      for (const token of extractMarkedLocatorTokens(inner)) tokens.add(token);
    }
    collectTokenFields(inner, tokens);
  }
}

function tryParseJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

function isUuid(value: string): boolean {
  UUID_PATTERN.lastIndex = 0;
  const result = UUID_PATTERN.test(value);
  UUID_PATTERN.lastIndex = 0;
  return result;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
