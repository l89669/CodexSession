import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { watch, FSWatcher } from "chokidar";
import { Db, INDEX_SCHEMA_VERSION, deleteFileRows } from "./db.js";
import { analyzeForkLineage, ForkLineageAnalysis } from "./lineage.js";
import { RuntimePaths, toPortablePath } from "./paths.js";
import {
  ParsedSessionChunk,
  ParsedSessionFile,
  parseSessionChunk,
  parseSessionFile
} from "./parser.js";
import { filenameIdentity, rolloutIdentity } from "./rollout-identity.js";
import { readSessionIndex, readThreadState, ThreadStateEntry } from "./session-index.js";
import { ArchiveScope, IndexingStatus, SessionFile, SyncResult } from "./types.js";
import { ensureDir, listJsonlFiles, nowIso } from "./util.js";

interface IndexerScheduler {
  setTimeout(callback: () => void, delayMs: number): NodeJS.Timeout;
  clearTimeout(timer: NodeJS.Timeout): void;
}

interface CodexSessionIndexerOptions {
  syncCheckIntervalMs?: number;
  pollIntervalMs?: number;
  nowMs?: () => number;
  scheduler?: IndexerScheduler;
}

interface IndexedFileState {
  rollout_id: string;
  session_id: string;
  size: number;
  mtime_ms: number;
  line_count: number;
  indexed_bytes: number | null;
  boundary_hash: string | null;
  current_turn_id: string | null;
  index_version: number;
}

interface IndexWriteResult {
  events: number;
  messages: number;
  toolCalls: number;
}

interface RolloutHistoryInput {
  baseRolloutId: string;
  replayParentSequence: number | null;
  localStartSequence: number;
  sequenceOffset: number;
  parentCutoffSequence: number;
  baseEndByteOffset: number | null;
  boundaryByteStart: number | null;
  boundaryByteLength: number | null;
  boundaryHash: string | null;
  source: "history_base" | "legacy_fork";
}

interface IndexedRolloutChoice {
  rollout_id: string;
  session_id: string;
  file_path: string;
  archive_scope: "active" | "archived";
  mtime_ms: number;
  session_updated_at: string | null;
}

const FILE_INDEX_VERSION = 4;

const systemScheduler: IndexerScheduler = {
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (timer) => clearTimeout(timer)
};

export class CodexSessionIndexer {
  private readonly db: Db;
  private readonly paths: RuntimePaths;
  private readonly syncCheckIntervalMs: number;
  private readonly pollIntervalMs: number;
  private readonly nowMs: () => number;
  private readonly scheduler: IndexerScheduler;
  private watcher: FSWatcher | undefined;
  private syncPromise: Promise<SyncResult> | undefined;
  private pollTimer: NodeJS.Timeout | undefined;
  private debounceTimer: NodeJS.Timeout | undefined;
  private lastSyncCheckMs = 0;
  private started = false;

  constructor(
    db: Db,
    paths: RuntimePaths,
    options: CodexSessionIndexerOptions = {}
  ) {
    this.db = db;
    this.paths = paths;
    this.syncCheckIntervalMs = options.syncCheckIntervalMs ?? 5_000;
    this.pollIntervalMs = options.pollIntervalMs ?? 2_000;
    this.nowMs = options.nowMs ?? Date.now;
    this.scheduler = options.scheduler ?? systemScheduler;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.startWatcher();
    void this.sync({ force: false })
      .catch(() => undefined)
      .finally(() => this.schedulePoll());
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    if (this.pollTimer) this.scheduler.clearTimeout(this.pollTimer);
    this.pollTimer = undefined;
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = undefined;
    if (this.watcher) await this.watcher.close();
    this.watcher = undefined;
    const runningSync = this.syncPromise;
    if (runningSync) {
      await runningSync;
    }
  }

  status(): IndexingStatus & { backend_pid: number } {
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
    return {
      indexing: Boolean(row?.indexing),
      started_at: row?.started_at ?? null,
      completed_at: row?.completed_at ?? null,
      files_seen: row?.files_seen ?? 0,
      files_indexed: row?.files_indexed ?? 0,
      events_indexed: row?.events_indexed ?? 0,
      error: row?.error ?? null,
      backend_pid: process.pid
    };
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
    this.lastSyncCheckMs = this.nowMs();
    if (this.syncPromise) return this.syncPromise;
    this.syncPromise = Promise.resolve().then(() => this.runSync(options)).finally(() => {
      this.lastSyncCheckMs = this.nowMs();
      this.syncPromise = undefined;
    });
    return this.syncPromise;
  }

  syncIfNeeded(): void {
    const now = this.nowMs();
    if (now - this.lastSyncCheckMs < this.syncCheckIntervalMs) return;
    this.lastSyncCheckMs = now;
    if (this.syncPromise) return;
    void this.sync({ force: false }).catch(() => undefined);
  }

  private schedulePoll(): void {
    if (!this.started || this.pollTimer) return;
    this.pollTimer = this.scheduler.setTimeout(() => {
      this.pollTimer = undefined;
      void this.sync({ force: false })
        .catch(() => undefined)
        .finally(() => this.schedulePoll());
    }, this.pollIntervalMs);
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
        void this.sync({ force: false }).catch(() => undefined);
      }, 300);
    };
    this.watcher.on("add", schedule);
    this.watcher.on("change", schedule);
    this.watcher.on("unlink", schedule);
  }


  private runSync(options: { rebuild?: boolean; force?: boolean }): SyncResult {
    const started = nowIso();
    const schemaVersion = this.db.pragma("user_version", { simple: true }) as number;
    const rebuild = Boolean(options.rebuild || schemaVersion < INDEX_SCHEMA_VERSION);
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
      if (rebuild) {
        this.db.exec("DELETE FROM sessions;");
      }

      const discovered = this.discoverFiles();
      result.files_seen = discovered.length;
      const discoveredPaths = new Set(discovered.map((file) => file.filePath));
      const indexedFiles = this.db.prepare("SELECT file_path FROM rollouts").all() as { file_path: string }[];
      for (const indexed of indexedFiles) {
        if (!discoveredPaths.has(indexed.file_path)) {
          deleteFileRows(this.db, indexed.file_path);
          result.files_deleted += 1;
        }
      }

      const indexEntries = readSessionIndex(this.paths.codexHome);
      const threadState = readThreadState(this.paths.codexHome);
      const discoveredBySessionId = new Map<string, SessionFile[]>();
      const discoveredByRolloutId = new Map<string, SessionFile>();
      for (const file of discovered) {
        const identity = filenameIdentity(file.filePath);
        if (!identity) continue;
        discoveredByRolloutId.set(identity.rolloutId, file);
        const files = discoveredBySessionId.get(identity.sessionId) ?? [];
        files.push(file);
        discoveredBySessionId.set(identity.sessionId, files);
      }
      for (const file of discovered) {
        const known = this.db
          .prepare(
            `SELECT rollout_id, session_id, size, mtime_ms, line_count, indexed_bytes,
                    boundary_hash, current_turn_id, index_version
             FROM rollouts
             WHERE file_path = ?`
          )
          .get(file.filePath) as IndexedFileState | undefined;
        const changed =
          options.force ||
          rebuild ||
          !known ||
          known.size !== file.size ||
          known.mtime_ms !== file.mtimeMs ||
          (known.indexed_bytes !== null && known.indexed_bytes < file.size);
        if (!changed) continue;

        const canAppend =
          !options.force &&
          !rebuild &&
          known?.index_version === FILE_INDEX_VERSION &&
          known.indexed_bytes !== null &&
          known.boundary_hash !== null &&
          known.indexed_bytes <= file.size &&
          boundaryHash(file.filePath, known.indexed_bytes) === known.boundary_hash;
        const written = canAppend
          ? this.appendFile(file, known)
          : this.reindexFile(file, indexEntries, discoveredBySessionId, discoveredByRolloutId);
        result.files_indexed += 1;
        result.events_indexed += written.events;
        result.messages_indexed += written.messages;
        result.tool_calls_indexed += written.toolCalls;
      }
      this.refreshCurrentRollouts(threadState);

      if (rebuild) {
        this.db.exec("VACUUM;");
      }
      result.completed_at = nowIso();
      this.markSyncComplete(result);
      if (rebuild) {
        const [checkpoint] = this.db.pragma("wal_checkpoint(TRUNCATE)") as Array<{ busy: number }>;
        if (checkpoint?.busy) {
          throw new Error("rebuilt index could not be compacted because another database connection is active");
        }
      }
      if (schemaVersion < INDEX_SCHEMA_VERSION) {
        this.db.pragma(`user_version = ${INDEX_SCHEMA_VERSION}`);
      }
      return result;
    } catch (error) {
      this.recordSyncError(error);
      throw error;
    }
  }

  private reindexFile(
    file: SessionFile,
    indexEntries: ReturnType<typeof readSessionIndex>,
    discoveredBySessionId: Map<string, SessionFile[]>,
    discoveredByRolloutId: Map<string, SessionFile>
  ): IndexWriteResult {
    const parsed = parseSessionFile(file.filePath, file.size);
    const identity = rolloutIdentity(file.filePath, parsed.meta);
    const { sessionId, rolloutId } = identity;
    const indexEntry = indexEntries.get(sessionId);
    const updatedAt = indexEntry?.updated_at
      ? new Date(indexEntry.updated_at).toISOString()
      : new Date(file.mtimeMs).toISOString();
    const lineage = parsed.meta.history_base
      ? undefined
      : this.analyzeLineage(parsed, discoveredBySessionId);
    const local = localChunk(parsed, lineage?.localStartSequence);
    const history = parsed.meta.history_base
      ? this.explicitHistory(parsed, rolloutId, discoveredByRolloutId)
      : lineage
        ? legacyHistory(lineage)
        : undefined;
    const firstSequence = local.rawEvents[0]?.sequence ?? parsed.rawEvents[0]?.sequence ?? 0;

    const transaction = this.db.transaction(() => {
      this.db.prepare("DELETE FROM rollouts WHERE rollout_id = ? OR file_path = ?").run(rolloutId, file.filePath);

      this.db
        .prepare(
          `INSERT INTO sessions
           (session_id, current_rollout_id, archive_scope, forked_from_id, created_at, updated_at,
            thread_name, cwd, meta_json)
           VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(session_id) DO UPDATE SET
             archive_scope = excluded.archive_scope,
             forked_from_id = COALESCE(sessions.forked_from_id, excluded.forked_from_id),
             created_at = COALESCE(sessions.created_at, excluded.created_at),
             updated_at = CASE
               WHEN sessions.updated_at IS NULL OR excluded.updated_at > sessions.updated_at THEN excluded.updated_at
               ELSE sessions.updated_at
             END,
             thread_name = COALESCE(excluded.thread_name, sessions.thread_name),
             cwd = COALESCE(excluded.cwd, sessions.cwd),
             meta_json = excluded.meta_json`
        )
        .run(
          sessionId,
          file.archiveScope,
          parsed.meta.forked_from_id ?? null,
          parsed.meta.timestamp ? new Date(parsed.meta.timestamp).toISOString() : null,
          updatedAt,
          indexEntry?.thread_name ?? null,
          parsed.meta.cwd ?? null,
          JSON.stringify(parsed.meta)
        );

      this.db
        .prepare(
          `INSERT INTO rollouts
           (rollout_id, session_id, file_path, archive_scope, history_mode, first_sequence,
            size, mtime_ms, line_count, indexed_bytes, boundary_hash, current_turn_id,
            index_version, indexed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          rolloutId,
          sessionId,
          file.filePath,
          file.archiveScope,
          parsed.meta.history_mode,
          firstSequence,
          file.size,
          file.mtimeMs,
          parsed.lineCount,
          parsed.indexedBytes,
          boundaryHash(file.filePath, parsed.indexedBytes),
          parsed.currentTurnId,
          FILE_INDEX_VERSION,
          nowIso()
        );

      this.insertParsedRows(sessionId, rolloutId, file, local);
      if (history) {
        this.db
          .prepare(
            `INSERT INTO rollout_history
             (rollout_id, base_rollout_id, replay_parent_sequence, local_start_sequence,
              sequence_offset, parent_cutoff_sequence, base_end_byte_offset,
              boundary_byte_start, boundary_byte_length, boundary_hash, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .run(
            rolloutId,
            history.baseRolloutId,
            history.replayParentSequence,
            history.localStartSequence,
            history.sequenceOffset,
            history.parentCutoffSequence,
            history.baseEndByteOffset,
            history.boundaryByteStart,
            history.boundaryByteLength,
            history.boundaryHash,
            history.source
          );
      }
    });
    transaction();
    return {
      events: local.rawEvents.length,
      messages: local.messages.length,
      toolCalls: local.toolCalls.length
    };
  }

  private appendFile(file: SessionFile, known: IndexedFileState): IndexWriteResult {
    const parsed = parseSessionChunk(file.filePath, {
      startByte: known.indexed_bytes ?? 0,
      endByte: file.size,
      startSequence: known.line_count,
      currentTurnId: known.current_turn_id
    });
    const transaction = this.db.transaction(() => {
      this.insertParsedRows(known.session_id, known.rollout_id, file, parsed);
      this.db
        .prepare(
          `UPDATE sessions
           SET archive_scope = ?, updated_at = ?
           WHERE session_id = ?`
        )
        .run(file.archiveScope, new Date(file.mtimeMs).toISOString(), known.session_id);
      this.db
        .prepare(
          `UPDATE rollouts
           SET archive_scope = ?, size = ?, mtime_ms = ?, line_count = ?, indexed_bytes = ?,
               boundary_hash = ?, current_turn_id = ?, index_version = ?, indexed_at = ?
           WHERE file_path = ?`
        )
        .run(
          file.archiveScope,
          file.size,
          file.mtimeMs,
          parsed.lineCount,
          parsed.indexedBytes,
          boundaryHash(file.filePath, parsed.indexedBytes),
          parsed.currentTurnId,
          FILE_INDEX_VERSION,
          nowIso(),
          file.filePath
        );
    });
    transaction();
    return {
      events: parsed.rawEvents.length,
      messages: parsed.messages.length,
      toolCalls: parsed.toolCalls.length
    };
  }

  private analyzeLineage(
    parsed: ParsedSessionFile,
    discoveredBySessionId: Map<string, SessionFile[]>
  ): ForkLineageAnalysis | undefined {
    const parentId = parsed.meta.forked_from_id?.toLowerCase();
    if (!parentId) return undefined;
    const indexedParent = this.db
      .prepare(
        `SELECT r.file_path, r.archive_scope
         FROM sessions s
         JOIN rollouts r ON r.rollout_id = s.current_rollout_id
         WHERE s.session_id = ?`
      )
      .get(parentId) as { file_path: string; archive_scope: "active" | "archived" } | undefined;
    const candidates = discoveredBySessionId.get(parentId) ?? [];
    if (candidates.length === 0 && indexedParent && fs.existsSync(indexedParent.file_path)) {
      candidates.push(this.fileInfo(indexedParent.file_path, indexedParent.archive_scope));
    }
    let best: ForkLineageAnalysis | undefined;
    for (const parentFile of candidates) {
      const parent = parseSessionFile(parentFile.filePath, parentFile.size);
      const parentIdentity = rolloutIdentity(parentFile.filePath, parent.meta);
      const analyzed = analyzeForkLineage(parsed, parent, parentIdentity.rolloutId);
      if (!analyzed) continue;
      if (!best || analyzed.replayParentSequence > best.replayParentSequence) best = analyzed;
    }
    return best;
  }

  private explicitHistory(
    parsed: ParsedSessionFile,
    rolloutId: string,
    discoveredByRolloutId: Map<string, SessionFile>
  ): RolloutHistoryInput {
    const history = parsed.meta.history_base;
    if (!history) throw new Error(`rollout ${rolloutId} has no explicit history base`);
    const baseRolloutId = history.thread_id.toLowerCase();
    const discoveredBase = discoveredByRolloutId.get(baseRolloutId);
    const indexedBase = this.db
      .prepare("SELECT file_path FROM rollouts WHERE rollout_id = ?")
      .get(baseRolloutId) as { file_path: string } | undefined;
    const basePath = discoveredBase?.filePath ?? indexedBase?.file_path;
    const boundary = basePath
      ? boundaryWindow(basePath, history.end_byte_offset)
      : undefined;
    return {
      baseRolloutId,
      replayParentSequence: null,
      localStartSequence: history.end_ordinal_exclusive,
      sequenceOffset: 0,
      parentCutoffSequence: history.end_ordinal_exclusive - 1,
      baseEndByteOffset: history.end_byte_offset,
      boundaryByteStart: boundary?.start ?? null,
      boundaryByteLength: boundary?.length ?? null,
      boundaryHash: boundary?.hash ?? null,
      source: "history_base"
    };
  }

  private refreshCurrentRollouts(threadState: Map<string, ThreadStateEntry>): void {
    const rows = this.db
      .prepare(
        `SELECT r.rollout_id, r.session_id, r.file_path, r.archive_scope, r.mtime_ms,
                s.updated_at AS session_updated_at
         FROM rollouts r
         JOIN sessions s ON s.session_id = r.session_id`
      )
      .all() as IndexedRolloutChoice[];
    const grouped = new Map<string, IndexedRolloutChoice[]>();
    for (const row of rows) {
      const choices = grouped.get(row.session_id) ?? [];
      choices.push(row);
      grouped.set(row.session_id, choices);
    }

    const update = this.db.prepare(
      `UPDATE sessions
       SET current_rollout_id = ?, archive_scope = ?, updated_at = ?
       WHERE session_id = ?`
    );
    const transaction = this.db.transaction(() => {
      for (const [sessionId, choices] of grouped) {
        const state = threadState.get(sessionId);
        const selectedByState = state
          ? choices.find((choice) => pathKey(choice.file_path) === pathKey(state.rolloutPath))
          : undefined;
        const selected = selectedByState ?? [...choices].sort((left, right) =>
          right.mtime_ms - left.mtime_ms || right.file_path.localeCompare(left.file_path)
        )[0];
        const updatedAt = state
          ? new Date(state.updatedAt * 1_000).toISOString()
          : selected.session_updated_at ?? new Date(selected.mtime_ms).toISOString();
        update.run(selected.rollout_id, selected.archive_scope, updatedAt, sessionId);
      }
    });
    transaction();
  }

  private insertParsedRows(
    sessionId: string,
    rolloutId: string,
    file: SessionFile,
    parsed: ParsedSessionChunk
  ): void {
    const rawIdByLine = new Map<number, number>();
    const turnRefById = new Map<string, number>();
    const rawInsert = this.db.prepare(
      `INSERT INTO raw_events
       (session_id, rollout_id, line_no, sequence, timestamp, event_type, payload_type, role,
        byte_start, byte_length, raw_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    const locatorInsert = this.db.prepare(
      `INSERT OR IGNORE INTO session_locator_tokens
       (token, session_id, rollout_id, archive_scope, sequence, timestamp, call_id, tool_name,
        source, raw_event_id, indexed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    const taskInputInsert = this.db.prepare(
      `INSERT OR IGNORE INTO session_task_inputs
       (session_id, rollout_id, sequence, timestamp, call_id, tool_name, token, task_text, turn_ref, raw_event_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    const turnInsert = this.db.prepare(
      `INSERT OR IGNORE INTO session_turns
       (session_id, rollout_id, turn_id, start_sequence, end_sequence, rewound)
       VALUES (?, ?, ?, ?, NULL, 0)`
    );
    const turnSelect = this.db.prepare(
      "SELECT id FROM session_turns WHERE rollout_id = ? AND turn_id = ?"
    );
    const turnClose = this.db.prepare(
      `UPDATE session_turns
       SET end_sequence = ?
       WHERE rollout_id = ? AND turn_id = ? AND end_sequence IS NULL`
    );
    const rewindLastEndedTurn = this.db.prepare(
      `UPDATE session_turns
       SET rewound = 1
       WHERE id = (
         SELECT id
         FROM session_turns
         WHERE rollout_id = ? AND end_sequence IS NOT NULL
         ORDER BY end_sequence DESC
         LIMIT 1
       ) AND rewound = 0`
    );
    const turnRef = (turnId: string | null): number | null => {
      if (!turnId) return null;
      const cached = turnRefById.get(turnId);
      if (cached !== undefined) return cached;
      const row = turnSelect.get(rolloutId, turnId) as { id: number } | undefined;
      if (!row) return null;
      turnRefById.set(turnId, row.id);
      return row.id;
    };
    for (const raw of parsed.rawEvents) {
      const info = rawInsert.run(
        sessionId,
        rolloutId,
        raw.lineNo,
        raw.sequence,
        raw.timestamp,
        raw.eventType,
        raw.payloadType,
        raw.role,
        raw.byteStart,
        raw.byteLength,
        raw.rawJson
      );
      const rawEventId = Number(info.lastInsertRowid);
      rawIdByLine.set(raw.lineNo, rawEventId);
      if (raw.eventType === "event_msg" && raw.payloadType === "task_started" && raw.turnId) {
        turnInsert.run(sessionId, rolloutId, raw.turnId, raw.sequence);
        turnRef(raw.turnId);
      } else if (
        raw.eventType === "event_msg" &&
        (raw.payloadType === "task_complete" || raw.payloadType === "turn_aborted") &&
        raw.turnId
      ) {
        turnClose.run(raw.sequence, rolloutId, raw.turnId);
      } else if (raw.eventType === "event_msg" && raw.payloadType === "thread_rolled_back") {
        rewindLastEndedTurn.run(rolloutId);
      }
      for (const locator of locatorTokensFromCompletedMcpCall(raw)) {
        locatorInsert.run(
          locator.token,
          sessionId,
          rolloutId,
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
      const taskInput = taskInputFromCompletedMcpCall(raw);
      if (taskInput) {
        taskInputInsert.run(
          sessionId,
          rolloutId,
          taskInput.sequence,
          taskInput.timestamp,
          taskInput.callId,
          taskInput.toolName,
          taskInput.token,
          taskInput.task,
          turnRef(raw.turnId),
          rawEventId
        );
      }
    }

    const messageInsert = this.db.prepare(
      `INSERT INTO messages
       (session_id, rollout_id, sequence, timestamp, role, content_text, turn_ref, raw_event_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    );
    for (const message of parsed.messages) {
      const rawEventId = rawIdByLine.get(message.rawLineNo);
      if (rawEventId === undefined) continue;
      messageInsert.run(
        sessionId,
        rolloutId,
        message.sequence,
        message.timestamp,
        message.role,
        message.contentText,
        turnRef(message.turnId),
        rawEventId
      );
    }

    const outputByCallId = new Map(parsed.toolOutputs.map((output) => [output.callId, output]));
    const toolInsert = this.db.prepare(
      `INSERT INTO tool_calls
       (session_id, rollout_id, sequence, timestamp, call_id, tool_name, arguments_json, output_sequence,
        output_timestamp, output_text, output_json, turn_ref, call_raw_event_id, output_raw_event_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    for (const call of parsed.toolCalls) {
      const callRawEventId = rawIdByLine.get(call.rawLineNo);
      if (callRawEventId === undefined) continue;
      const output = outputByCallId.get(call.callId);
      toolInsert.run(
        sessionId,
        rolloutId,
        call.sequence,
        call.timestamp,
        call.callId,
        call.toolName,
        call.argumentsJson,
        output?.sequence ?? null,
        output?.timestamp ?? null,
        output?.outputText ?? null,
        output?.outputJson ?? null,
        turnRef(call.turnId),
        callRawEventId,
        output ? rawIdByLine.get(output.rawLineNo) ?? null : null
      );
      for (const locator of locatorTokensFromToolCall(call, output)) {
        const rawEventId =
          locator.source === "tool_output" && output
            ? rawIdByLine.get(output.rawLineNo)
            : callRawEventId;
        locatorInsert.run(
          locator.token,
          sessionId,
          rolloutId,
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

    const updateOutput = this.db.prepare(
      `UPDATE tool_calls
       SET output_sequence = ?, output_timestamp = ?, output_text = ?, output_json = ?, output_raw_event_id = ?
       WHERE rollout_id = ? AND call_id = ?`
    );
    for (const output of parsed.toolOutputs) {
      if (parsed.toolCalls.some((call) => call.callId === output.callId)) continue;
      const outputRawEventId = rawIdByLine.get(output.rawLineNo);
      if (outputRawEventId === undefined) continue;
      updateOutput.run(
        output.sequence,
        output.timestamp,
        output.outputText,
        output.outputJson,
        outputRawEventId,
        rolloutId,
        output.callId
      );
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
    try {
      this.markSyncError(error);
    } catch (statusError) {
      console.error("Failed to record Codex session index sync error", statusError);
    }
  }
}

function localChunk(parsed: ParsedSessionFile, localStartSequence: number | undefined): ParsedSessionChunk {
  if (localStartSequence === undefined) return parsed;
  const keep = (sequence: number) => sequence === 1 || sequence >= localStartSequence;
  return {
    meta: parsed.meta,
    rawEvents: parsed.rawEvents.filter((event) => keep(event.sequence)),
    messages: parsed.messages.filter((message) => message.sequence >= localStartSequence),
    toolCalls: parsed.toolCalls.filter((call) => call.sequence >= localStartSequence),
    toolOutputs: parsed.toolOutputs.filter((output) => output.sequence >= localStartSequence),
    lineCount: parsed.lineCount,
    indexedBytes: parsed.indexedBytes,
    currentTurnId: parsed.currentTurnId
  };
}

function boundaryHash(filePath: string, indexedBytes: number): string {
  const length = Math.min(4_096, indexedBytes);
  const buffer = Buffer.allocUnsafe(length);
  const handle = fs.openSync(filePath, "r");
  try {
    let read = 0;
    while (read < length) {
      const count = fs.readSync(handle, buffer, read, length - read, indexedBytes - length + read);
      if (count === 0) break;
      read += count;
    }
    return crypto.createHash("sha256").update(buffer.subarray(0, read)).digest("hex");
  } finally {
    fs.closeSync(handle);
  }
}

function boundaryWindow(filePath: string, endByteOffset: number): { start: number; length: number; hash: string } | undefined {
  if (endByteOffset === 0) return undefined;
  const size = fs.statSync(filePath).size;
  if (endByteOffset > size) {
    throw new Error(`history base byte offset ${endByteOffset} exceeds rollout size ${size}: ${filePath}`);
  }
  const length = Math.min(4_096, endByteOffset);
  const start = endByteOffset - length;
  const buffer = Buffer.allocUnsafe(length);
  const handle = fs.openSync(filePath, "r");
  let read = 0;
  try {
    while (read < length) {
      const count = fs.readSync(handle, buffer, read, length - read, start + read);
      if (count === 0) break;
      read += count;
    }
  } finally {
    fs.closeSync(handle);
  }
  if (read !== length) throw new Error(`could not read history base boundary: ${filePath}`);
  return {
    start,
    length,
    hash: crypto.createHash("sha256").update(buffer).digest("hex")
  };
}

function legacyHistory(lineage: ForkLineageAnalysis): RolloutHistoryInput {
  return {
    baseRolloutId: lineage.parentRolloutId,
    replayParentSequence: lineage.replayParentSequence,
    localStartSequence: lineage.localStartSequence,
    sequenceOffset: lineage.sequenceOffset,
    parentCutoffSequence: lineage.parentCutoffSequence,
    baseEndByteOffset: null,
    boundaryByteStart: lineage.boundaryByteStart,
    boundaryByteLength: lineage.boundaryByteLength,
    boundaryHash: lineage.boundaryHash,
    source: "legacy_fork"
  };
}

function pathKey(filePath: string): string {
  const withoutExtendedPrefix = filePath.startsWith("\\\\?\\") ? filePath.slice(4) : filePath;
  const resolved = path.resolve(withoutExtendedPrefix);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
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

interface CompletedMcpToolCall {
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

function locatorTokensFromCompletedMcpCall(raw: {
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  payload: Record<string, unknown>;
}): RawLocatorTokenOccurrence[] {
  const call = parseCompletedMcpToolCall(raw);
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

function taskInputFromCompletedMcpCall(raw: {
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  payload: Record<string, unknown>;
}): RawTaskInputOccurrence | undefined {
  const call = parseCompletedMcpToolCall(raw);
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

function parseCompletedMcpToolCall(raw: {
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  payload: Record<string, unknown>;
}): CompletedMcpToolCall | undefined {
  if (raw.eventType !== "event_msg") return undefined;
  if (raw.payloadType === "mcp_tool_call_end") {
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
  if (raw.payloadType !== "item_completed") return undefined;
  const item = recordValue(raw.payload.item);
  if (item.type !== "McpToolCall" || item.status !== "completed") return undefined;
  const serverName = stringValue(item.server);
  const toolName = stringValue(item.tool);
  if (!serverName || !toolName) return undefined;
  return {
    sequence: raw.sequence,
    timestamp: raw.timestamp,
    callId: stringValue(item.id) ?? null,
    serverName,
    toolName,
    arguments: recordValue(item.arguments),
    okResult: recordValue(item.result)
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
