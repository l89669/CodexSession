import Database from "better-sqlite3";
import path from "node:path";
import { ensureDir } from "./util.js";

export type Db = Database.Database;

export const INDEX_SCHEMA_VERSION = 10;
const MIN_INCREMENTAL_MIGRATION_VERSION = 9;

export function openDatabase(dbPath: string, options: { busyTimeoutMs?: number } = {}): Db {
  ensureDir(path.dirname(dbPath));
  const busyTimeoutMs = options.busyTimeoutMs ?? 5_000;
  const db = new Database(dbPath, { timeout: busyTimeoutMs });
  db.pragma(`busy_timeout = ${busyTimeoutMs}`);
  db.pragma("foreign_keys = ON");
  runIfNotBusy(() => db.pragma("journal_mode = WAL"));
  runIfNotBusy(() => migrate(db));
  return db;
}

function runIfNotBusy(operation: () => void): void {
  try {
    operation();
  } catch (error) {
    if (!isSqliteBusy(error)) throw error;
  }
}

export function isSqliteBusy(error: unknown): boolean {
  return Boolean(
    error &&
      typeof error === "object" &&
      "code" in error &&
      (error as { code?: unknown }).code === "SQLITE_BUSY"
  );
}

export function migrate(db: Db): void {
  const version = db.pragma("user_version", { simple: true }) as number;
  if (version > 0 && version < MIN_INCREMENTAL_MIGRATION_VERSION) {
    resetDerivedIndex(db);
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id TEXT PRIMARY KEY,
      current_rollout_id TEXT,
      archive_scope TEXT NOT NULL CHECK (archive_scope IN ('active', 'archived')),
      forked_from_id TEXT,
      created_at TEXT,
      updated_at TEXT,
      thread_name TEXT,
      cwd TEXT,
      meta_json TEXT
    );

    CREATE TABLE IF NOT EXISTS rollouts (
      rollout_id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      file_path TEXT NOT NULL UNIQUE,
      archive_scope TEXT NOT NULL CHECK (archive_scope IN ('active', 'archived')),
      history_mode TEXT NOT NULL CHECK (history_mode IN ('legacy', 'paginated')),
      first_sequence INTEGER NOT NULL,
      size INTEGER NOT NULL,
      mtime_ms REAL NOT NULL,
      line_count INTEGER NOT NULL,
      indexed_bytes INTEGER NOT NULL,
      boundary_hash TEXT NOT NULL,
      current_turn_id TEXT,
      index_version INTEGER NOT NULL,
      indexed_at TEXT NOT NULL,
      UNIQUE (rollout_id, session_id),
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS rollout_history (
      rollout_id TEXT PRIMARY KEY,
      base_rollout_id TEXT NOT NULL,
      replay_parent_sequence INTEGER,
      local_start_sequence INTEGER NOT NULL,
      sequence_offset INTEGER NOT NULL,
      parent_cutoff_sequence INTEGER NOT NULL,
      base_end_byte_offset INTEGER,
      boundary_byte_start INTEGER,
      boundary_byte_length INTEGER,
      boundary_hash TEXT,
      source TEXT NOT NULL CHECK (source IN ('history_base', 'legacy_fork')),
      FOREIGN KEY (rollout_id) REFERENCES rollouts(rollout_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS raw_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      rollout_id TEXT NOT NULL,
      line_no INTEGER NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      event_type TEXT NOT NULL,
      payload_type TEXT,
      role TEXT,
      byte_start INTEGER NOT NULL,
      byte_length INTEGER NOT NULL,
      raw_json TEXT NOT NULL,
      UNIQUE (rollout_id, line_no),
      FOREIGN KEY (rollout_id, session_id)
        REFERENCES rollouts(rollout_id, session_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS session_turns (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      rollout_id TEXT NOT NULL,
      turn_id TEXT NOT NULL,
      start_sequence INTEGER NOT NULL,
      end_sequence INTEGER,
      rewound INTEGER NOT NULL DEFAULT 0 CHECK (rewound IN (0, 1)),
      UNIQUE (rollout_id, turn_id),
      FOREIGN KEY (rollout_id, session_id)
        REFERENCES rollouts(rollout_id, session_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      rollout_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      role TEXT NOT NULL,
      content_text TEXT NOT NULL,
      turn_ref INTEGER,
      raw_event_id INTEGER NOT NULL,
      FOREIGN KEY (rollout_id, session_id)
        REFERENCES rollouts(rollout_id, session_id) ON DELETE CASCADE,
      FOREIGN KEY (turn_ref) REFERENCES session_turns(id) ON DELETE CASCADE,
      FOREIGN KEY (raw_event_id) REFERENCES raw_events(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS tool_calls (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      rollout_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      call_id TEXT NOT NULL,
      tool_name TEXT NOT NULL,
      arguments_json TEXT,
      output_sequence INTEGER,
      output_timestamp TEXT,
      output_text TEXT,
      output_json TEXT,
      turn_ref INTEGER,
      call_raw_event_id INTEGER NOT NULL,
      output_raw_event_id INTEGER,
      FOREIGN KEY (rollout_id, session_id)
        REFERENCES rollouts(rollout_id, session_id) ON DELETE CASCADE,
      FOREIGN KEY (turn_ref) REFERENCES session_turns(id) ON DELETE CASCADE,
      FOREIGN KEY (call_raw_event_id) REFERENCES raw_events(id) ON DELETE CASCADE,
      FOREIGN KEY (output_raw_event_id) REFERENCES raw_events(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS session_locator_tokens (
      token TEXT NOT NULL,
      session_id TEXT NOT NULL,
      rollout_id TEXT NOT NULL,
      archive_scope TEXT NOT NULL CHECK (archive_scope IN ('active', 'archived')),
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      call_id TEXT,
      tool_name TEXT NOT NULL,
      source TEXT NOT NULL CHECK (source IN ('tool_arguments', 'tool_output')),
      raw_event_id INTEGER,
      indexed_at TEXT NOT NULL,
      PRIMARY KEY (token, rollout_id, sequence, source),
      FOREIGN KEY (rollout_id, session_id)
        REFERENCES rollouts(rollout_id, session_id) ON DELETE CASCADE,
      FOREIGN KEY (raw_event_id) REFERENCES raw_events(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS agent_tasks (
      token TEXT PRIMARY KEY,
      task TEXT NOT NULL,
      created_at TEXT NOT NULL,
      retrieval_count INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS session_task_inputs (
      session_id TEXT NOT NULL,
      rollout_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      call_id TEXT,
      tool_name TEXT NOT NULL,
      token TEXT NOT NULL,
      task_text TEXT NOT NULL,
      turn_ref INTEGER,
      raw_event_id INTEGER NOT NULL,
      PRIMARY KEY (rollout_id, sequence),
      FOREIGN KEY (rollout_id, session_id)
        REFERENCES rollouts(rollout_id, session_id) ON DELETE CASCADE,
      FOREIGN KEY (turn_ref) REFERENCES session_turns(id) ON DELETE CASCADE,
      FOREIGN KEY (raw_event_id) REFERENCES raw_events(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS sync_status (
      singleton_key TEXT PRIMARY KEY,
      indexing INTEGER NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      files_seen INTEGER NOT NULL DEFAULT 0,
      files_indexed INTEGER NOT NULL DEFAULT 0,
      events_indexed INTEGER NOT NULL DEFAULT 0,
      error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at);
    CREATE INDEX IF NOT EXISTS idx_sessions_archive ON sessions(archive_scope);
    CREATE INDEX IF NOT EXISTS idx_rollouts_session ON rollouts(session_id, mtime_ms);
    CREATE INDEX IF NOT EXISTS idx_rollouts_file ON rollouts(file_path);
    CREATE INDEX IF NOT EXISTS idx_rollout_history_base ON rollout_history(base_rollout_id);
    CREATE INDEX IF NOT EXISTS idx_raw_events_rollout_seq ON raw_events(rollout_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_raw_events_session_seq ON raw_events(session_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_raw_events_type ON raw_events(event_type, payload_type);
    CREATE INDEX IF NOT EXISTS idx_session_turns_rollout_end ON session_turns(rollout_id, end_sequence DESC);
    CREATE INDEX IF NOT EXISTS idx_messages_rollout_seq ON messages(rollout_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_messages_session_role ON messages(session_id, role);
    CREATE INDEX IF NOT EXISTS idx_messages_raw_event ON messages(raw_event_id);
    CREATE INDEX IF NOT EXISTS idx_messages_turn_ref ON messages(turn_ref);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_rollout_seq ON tool_calls(rollout_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_session_name ON tool_calls(session_id, tool_name);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_call_raw_event ON tool_calls(call_raw_event_id);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_output_raw_event ON tool_calls(output_raw_event_id);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_turn_ref ON tool_calls(turn_ref);
    CREATE INDEX IF NOT EXISTS idx_session_locator_tokens_token ON session_locator_tokens(token);
    CREATE INDEX IF NOT EXISTS idx_session_locator_tokens_session ON session_locator_tokens(session_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_session_task_inputs_session ON session_task_inputs(session_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_session_task_inputs_turn_ref ON session_task_inputs(turn_ref);
  `);

  if (version === MIN_INCREMENTAL_MIGRATION_VERSION) {
    db.exec(`
      INSERT INTO messages
        (session_id, rollout_id, sequence, timestamp, role, content_text, turn_ref, raw_event_id)
      SELECT
        raw.session_id,
        raw.rollout_id,
        raw.sequence,
        raw.timestamp,
        'user',
        json_extract(raw.raw_json, '$.payload.output'),
        turn.id,
        raw.id
      FROM raw_events raw
      LEFT JOIN session_turns turn
        ON turn.rollout_id = raw.rollout_id
       AND turn.turn_id = json_extract(
         raw.raw_json,
         '$.payload.internal_chat_message_metadata_passthrough.turn_id'
       )
      WHERE raw.event_type = 'response_item'
        AND raw.payload_type = 'function_call_output'
        AND json_extract(raw.raw_json, '$.payload.namespace') = 'codex_app'
        AND json_extract(raw.raw_json, '$.payload.name') = 'send_message_to_thread'
        AND json_extract(raw.raw_json, '$.payload.call_id') IS NULL
        AND ltrim(json_extract(raw.raw_json, '$.payload.output')) LIKE '<codex_delegation>%'
        AND instr(json_extract(raw.raw_json, '$.payload.output'), '<source_thread_id>') > 0
        AND instr(json_extract(raw.raw_json, '$.payload.output'), '<input>') > 0
        AND NOT EXISTS (
          SELECT 1 FROM messages message WHERE message.raw_event_id = raw.id
        );
    `);
    db.pragma(`user_version = ${INDEX_SCHEMA_VERSION}`);
  } else if (version === 0) {
    db.pragma(`user_version = ${INDEX_SCHEMA_VERSION}`);
  }
}

function resetDerivedIndex(db: Db): void {
  db.exec(`
    DROP TABLE IF EXISTS session_lineage;
    DROP TABLE IF EXISTS rollout_history;
    DROP TABLE IF EXISTS session_task_inputs;
    DROP TABLE IF EXISTS session_locator_tokens;
    DROP TABLE IF EXISTS tool_calls;
    DROP TABLE IF EXISTS messages;
    DROP TABLE IF EXISTS session_turns;
    DROP TABLE IF EXISTS raw_events;
    DROP TABLE IF EXISTS session_files;
    DROP TABLE IF EXISTS rollouts;
    DROP TABLE IF EXISTS sync_status;
    DROP TABLE IF EXISTS sessions;
  `);
}

export function deleteFileRows(db: Db, filePath: string): void {
  const rollout = db
    .prepare("SELECT rollout_id, session_id FROM rollouts WHERE file_path = ?")
    .get(filePath) as { rollout_id: string; session_id: string } | undefined;
  if (!rollout) return;
  db.prepare("DELETE FROM rollouts WHERE rollout_id = ?").run(rollout.rollout_id);
  deleteSessionIfEmpty(db, rollout.session_id);
}

export function deleteRolloutRows(db: Db, rolloutId: string): void {
  const rollout = db
    .prepare("SELECT session_id FROM rollouts WHERE rollout_id = ?")
    .get(rolloutId) as { session_id: string } | undefined;
  if (!rollout) return;
  db.prepare("DELETE FROM rollouts WHERE rollout_id = ?").run(rolloutId);
  deleteSessionIfEmpty(db, rollout.session_id);
}

export function deleteSessionRows(db: Db, sessionId: string): void {
  db.prepare("DELETE FROM sessions WHERE session_id = ?").run(sessionId);
}

function deleteSessionIfEmpty(db: Db, sessionId: string): void {
  const remaining = db
    .prepare("SELECT 1 FROM rollouts WHERE session_id = ? LIMIT 1")
    .get(sessionId);
  if (!remaining) db.prepare("DELETE FROM sessions WHERE session_id = ?").run(sessionId);
}
