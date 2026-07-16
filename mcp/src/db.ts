import Database from "better-sqlite3";
import { ensureDir } from "./util.js";
import path from "node:path";

export type Db = Database.Database;

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
  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      session_id TEXT PRIMARY KEY,
      file_path TEXT NOT NULL UNIQUE,
      archive_scope TEXT NOT NULL CHECK (archive_scope IN ('active', 'archived')),
      forked_from_id TEXT,
      created_at TEXT,
      updated_at TEXT,
      thread_name TEXT,
      cwd TEXT,
      meta_json TEXT
    );

    CREATE TABLE IF NOT EXISTS session_files (
      file_path TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      archive_scope TEXT NOT NULL CHECK (archive_scope IN ('active', 'archived')),
      size INTEGER NOT NULL,
      mtime_ms REAL NOT NULL,
      line_count INTEGER NOT NULL,
      indexed_at TEXT NOT NULL,
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS raw_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      file_path TEXT NOT NULL,
      line_no INTEGER NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      event_type TEXT NOT NULL,
      payload_type TEXT,
      role TEXT,
      raw_json TEXT NOT NULL,
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      role TEXT NOT NULL,
      content_text TEXT NOT NULL,
      content_json TEXT,
      raw_event_id INTEGER NOT NULL,
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
      FOREIGN KEY (raw_event_id) REFERENCES raw_events(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS tool_calls (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      call_id TEXT NOT NULL,
      tool_name TEXT NOT NULL,
      arguments_json TEXT,
      output_sequence INTEGER,
      output_timestamp TEXT,
      output_text TEXT,
      output_json TEXT,
      call_raw_event_id INTEGER NOT NULL,
      output_raw_event_id INTEGER,
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
      FOREIGN KEY (call_raw_event_id) REFERENCES raw_events(id) ON DELETE CASCADE,
      FOREIGN KEY (output_raw_event_id) REFERENCES raw_events(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS session_locator_tokens (
      token TEXT NOT NULL,
      session_id TEXT NOT NULL,
      archive_scope TEXT NOT NULL CHECK (archive_scope IN ('active', 'archived')),
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      call_id TEXT,
      tool_name TEXT NOT NULL,
      source TEXT NOT NULL CHECK (source IN ('tool_arguments', 'tool_output')),
      raw_event_id INTEGER,
      indexed_at TEXT NOT NULL,
      PRIMARY KEY (token, session_id, sequence, source),
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
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
      sequence INTEGER NOT NULL,
      timestamp TEXT,
      call_id TEXT,
      tool_name TEXT NOT NULL,
      token TEXT NOT NULL,
      task_text TEXT NOT NULL,
      raw_event_id INTEGER NOT NULL,
      PRIMARY KEY (session_id, sequence),
      FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
      FOREIGN KEY (raw_event_id) REFERENCES raw_events(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS leader_lease (
      singleton_key TEXT PRIMARY KEY,
      holder_id TEXT NOT NULL,
      renewed_at_ms INTEGER NOT NULL,
      lease_ms INTEGER NOT NULL,
      generation INTEGER NOT NULL
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
    CREATE INDEX IF NOT EXISTS idx_raw_events_session_seq ON raw_events(session_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_raw_events_type ON raw_events(event_type, payload_type);
    CREATE INDEX IF NOT EXISTS idx_messages_session_seq ON messages(session_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(session_id, role);
    CREATE INDEX IF NOT EXISTS idx_messages_raw_event ON messages(raw_event_id);
    CREATE INDEX IF NOT EXISTS idx_session_files_session ON session_files(session_id);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_session_seq ON tool_calls(session_id, sequence);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_name ON tool_calls(session_id, tool_name);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_call_raw_event ON tool_calls(call_raw_event_id);
    CREATE INDEX IF NOT EXISTS idx_tool_calls_output_raw_event ON tool_calls(output_raw_event_id);
    CREATE INDEX IF NOT EXISTS idx_session_locator_tokens_token ON session_locator_tokens(token);
    CREATE INDEX IF NOT EXISTS idx_session_locator_tokens_session ON session_locator_tokens(session_id, sequence);
  `);

  if (!columnExists(db, "sessions", "forked_from_id")) {
    db.exec("ALTER TABLE sessions ADD COLUMN forked_from_id TEXT;");
  }
  if (!columnExists(db, "agent_tasks", "retrieval_count")) {
    db.exec("ALTER TABLE agent_tasks ADD COLUMN retrieval_count INTEGER NOT NULL DEFAULT 0;");
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_sessions_forked_from ON sessions(forked_from_id);");

  const version = db.pragma("user_version", { simple: true }) as number;
  if (version < 1) {
    db.exec("DELETE FROM sessions;");
    db.pragma("user_version = 1");
  }
  if (version < 2) {
    db.pragma("user_version = 2");
  }
  if (version < 3) {
    db.pragma("user_version = 3");
  }
  if (version < 4) {
    db.pragma("user_version = 4");
  }
  if (version < 5) {
    db.pragma("user_version = 5");
  }
}

function columnExists(db: Db, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  return rows.some((row) => row.name === column);
}

export function deleteFileRows(db: Db, filePath: string): void {
  const session = db.prepare("SELECT session_id FROM session_files WHERE file_path = ?").get(filePath) as { session_id: string } | undefined;
  if (!session) return;
  db.prepare("DELETE FROM sessions WHERE session_id = ?").run(session.session_id);
}

export function deleteSessionRows(db: Db, sessionId: string): void {
  db.prepare("DELETE FROM sessions WHERE session_id = ?").run(sessionId);
}
