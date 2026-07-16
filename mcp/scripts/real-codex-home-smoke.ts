import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { openDatabase } from "../src/db.js";
import { CodexSessionIndexer } from "../src/indexer.js";
import { parseSessionFile } from "../src/parser.js";
import { resolveCodexHome, resolveRuntimePaths } from "../src/paths.js";
import { CodexSessionQueries } from "../src/query.js";
import { ensureDir, listJsonlFiles } from "../src/util.js";

interface TimerResult<T> {
  ms: number;
  result: T;
}

const keepDb = process.argv.includes("--keep-db");
const rebuild = process.argv.includes("--rebuild");
const codexHome = resolveCodexHome(process.env.CODEX_HOME);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-session-real-smoke-"));
const dbPath = process.env.CODEX_SESSION_MCP_DB ?? path.join(root, "index.sqlite");
ensureDir(path.dirname(dbPath));

const paths = resolveRuntimePaths({ codexHome, indexDbPath: dbPath });
const db = openDatabase(paths.indexDbPath);
const indexer = new CodexSessionIndexer(db, paths, {
  holderId: `real-smoke-${process.pid}`,
  runSyncInProcess: true
});
const queries = new CodexSessionQueries({ db, indexer, waitForIdleMs: 1_000 });

try {
  const sourceStats = countSourceFiles(paths.sessionsDir, paths.archivedSessionsDir);
  const parsedStats = time(() => inspectSourceFiles(paths.sessionsDir, paths.archivedSessionsDir));
  const sync = await timeAsync(() => indexer.sync({ rebuild, force: rebuild }));
  const dbCounts = tableCounts();
  const sessionList = await timeAsync(() => queries.listSessions({ archive_scope: "both", limit: 10 }));
  const latest = sessionList.result.status === "ok" ? (sessionList.result.data as any).sessions?.[0] : undefined;

  let recovery: Record<string, unknown> = { skipped: true };
  if (latest?.id) {
    const snippet = findDistinctiveSnippetForSession(latest.id);
    if (snippet) {
      const located = await timeAsync(() =>
        queries.findByText({
          text: snippet,
          archive_scope: "both",
          include_candidates: false,
          max_chars: 80
        })
      );
      const recent = await timeAsync(() => queries.recentUserInputs({ session_id: latest.id, limit: 3, max_chars: 80 }));
      const keyword = await timeAsync(() =>
        queries.keywordSearch({
          session_id: latest.id,
          keywords: ["记住", "工具", "database is locked"],
          limit: 10,
          max_chars: 80
        })
      );
      const toolCalls = await timeAsync(() => queries.toolCalls({ session_id: latest.id, limit: 5, max_output_chars: 80 }));
      recovery = {
        skipped: false,
        snippet_hash: stableHash(snippet),
        find_by_text_status: located.result.status,
        find_by_text_ms: located.ms,
        recent_status: recent.result.status,
        recent_count: (recent.result.data as any)?.inputs?.length ?? 0,
        recent_ms: recent.ms,
        keyword_status: keyword.result.status,
        keyword_count: (keyword.result.data as any)?.results?.length ?? 0,
        keyword_ms: keyword.ms,
        tool_calls_status: toolCalls.result.status,
        tool_calls_count: (toolCalls.result.data as any)?.tool_calls?.length ?? 0,
        tool_calls_ms: toolCalls.ms
      };
    }
  }

  console.log(
    JSON.stringify(
      {
        codex_home_hash: stableHash(codexHome),
        db_path_hash: stableHash(paths.indexDbPath),
        db_is_temporary: !process.env.CODEX_SESSION_MCP_DB,
        keep_db: keepDb,
        source: sourceStats,
        parse_inspection: {
          ms: parsedStats.ms,
          ...parsedStats.result
        },
        sync: {
          ms: sync.ms,
          result: sync.result
        },
        db_counts: dbCounts,
        list_sessions: {
          ms: sessionList.ms,
          status: sessionList.result.status,
          count: (sessionList.result.data as any)?.sessions?.length ?? 0
        },
        latest_session_hash: latest?.id ? stableHash(latest.id) : null,
        recovery
      },
      null,
      2
    )
  );
} finally {
  await indexer.stop().catch(() => undefined);
  db.close();
  if (!keepDb && !process.env.CODEX_SESSION_MCP_DB) {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function countSourceFiles(activeDir: string, archivedDir: string): Record<string, unknown> {
  const active = listJsonlFiles(activeDir, true);
  const archived = listJsonlFiles(archivedDir, true);
  return {
    active_files: active.length,
    archived_files: archived.length,
    total_files: active.length + archived.length,
    active_bytes: sumBytes(active),
    archived_bytes: sumBytes(archived)
  };
}

function inspectSourceFiles(activeDir: string, archivedDir: string): Record<string, unknown> {
  const files = [...listJsonlFiles(activeDir, true), ...listJsonlFiles(archivedDir, true)];
  let rawEvents = 0;
  let messages = 0;
  let toolCalls = 0;
  let toolOutputs = 0;
  const ids = new Map<string, number>();
  for (const file of files) {
    const parsed = parseSessionFile(file);
    rawEvents += parsed.rawEvents.length;
    messages += parsed.messages.length;
    toolCalls += parsed.toolCalls.length;
    toolOutputs += parsed.toolOutputs.length;
    ids.set(parsed.meta.id, (ids.get(parsed.meta.id) ?? 0) + 1);
  }
  return {
    raw_events: rawEvents,
    messages,
    tool_calls: toolCalls,
    tool_outputs: toolOutputs,
    distinct_session_ids: ids.size,
    duplicate_session_ids: [...ids.values()].filter((count) => count > 1).length
  };
}

function findDistinctiveSnippetForSession(sessionId: string): string | undefined {
  const row = db
    .prepare(
      `SELECT content_text
       FROM messages
       WHERE session_id = ? AND role = 'user' AND length(content_text) >= 24
       ORDER BY sequence DESC
       LIMIT 20`
    )
    .all(sessionId) as Array<{ content_text: string }>;

  for (const candidate of row) {
    const snippet = candidate.content_text.trim().replace(/\s+/g, " ").slice(0, 160);
    if (snippet.length >= 24) return snippet;
  }
  return undefined;
}

function tableCounts(): Record<string, number> {
  return Object.fromEntries(
    ["sessions", "session_files", "raw_events", "messages", "tool_calls", "session_locator_tokens"].map((table) => [
      table,
      (db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get() as { n: number }).n
    ])
  );
}

function sumBytes(files: string[]): number {
  return files.reduce((total, file) => total + fs.statSync(file).size, 0);
}

function time<T>(callback: () => T): TimerResult<T> {
  const started = Date.now();
  const result = callback();
  return { ms: Date.now() - started, result };
}

async function timeAsync<T>(callback: () => Promise<T>): Promise<TimerResult<T>> {
  const started = Date.now();
  const result = await callback();
  return { ms: Date.now() - started, result };
}

function stableHash(value: string): string {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}
