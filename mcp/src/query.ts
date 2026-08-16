import { randomUUID } from "node:crypto";
import { Db } from "./db.js";
import { HistorySegment, resolveSessionHistory } from "./history-view.js";
import { CodexSessionIndexer } from "./indexer.js";
import { ArchiveScope, KeywordMatch, MessageRole, SearchScope, SortOrder, SyncResult } from "./types.js";
import {
  DEFAULT_LIST_LIMIT,
  DEFAULT_SEARCH_LIMIT,
  DEFAULT_TEXT_CHARS,
  DEFAULT_TOOL_OUTPUT_CHARS,
  matchedKeywords,
  nonEmptyKeywords,
  parseLimit,
  parseOrder,
  parseTimeToUtcIso,
  truncateText
} from "./util.js";

type SessionQueryIndexer = Pick<CodexSessionIndexer, "status" | "sync" | "syncIfNeeded" | "waitForIdle">;

interface QueryContext {
  db: Db;
  indexer: SessionQueryIndexer;
  waitForIdleMs?: number;
}

const PERSISTENT_SYSTEM_INPUT_PREFIXES = [
  "<recommended_plugins>\nHere is a list of plugins that are available but not installed.",
  "# AGENTS.md instructions"
] as const;
const PERSISTENT_SYSTEM_INPUT_TRUNCATION_NOTICE =
  "\n... [truncated: this system prompt is guaranteed to remain in context across compression]";

interface FindTextRow {
  session_id: string;
  thread_name: string | null;
  cwd: string | null;
  updated_at: string | null;
  archive_scope: ArchiveScope;
  sequence: number;
  timestamp: string | null;
  source_type: "message" | "published_task_retrieval";
  role: string | null;
  searchable_text: string;
  token: string | null;
  call_id: string | null;
  tool_name: string | null;
  occurrences: number;
}

type MessageQueryRow = Record<string, unknown> & {
  sequence: number;
  timestamp: string | null;
  role: string;
  content_text: string;
  content_json: string | null;
  raw_json?: string;
};

type ToolQueryRow = Record<string, unknown> & {
  sequence: number;
  timestamp: string | null;
  call_id: string;
  tool_name: string;
  arguments_json: string | null;
  output_sequence: number | null;
  output_timestamp: string | null;
  output_text: string | null;
  output_json: string | null;
  call_raw_json?: string;
  output_raw_json?: string;
};

type LocatorLookupRow = Record<string, unknown> & {
  session_id: string;
  sequence: number;
  timestamp: string | null;
};

export class CodexSessionQueries {
  private readonly db: Db;
  private readonly indexer: SessionQueryIndexer;
  private readonly waitForIdleMs: number;

  constructor(context: QueryContext) {
    this.db = context.db;
    this.indexer = context.indexer;
    this.waitForIdleMs = context.waitForIdleMs ?? 5_000;
  }

  async status(): Promise<Record<string, unknown>> {
    return { ...this.indexer.status() };
  }

  async sync(args: { rebuild?: boolean } = {}): Promise<Record<string, unknown>> {
    const syncPromise = this.indexer.sync({ rebuild: Boolean(args.rebuild), force: Boolean(args.rebuild) });
    const completed = await this.waitForSyncResult(syncPromise);
    if (!completed.done) {
      void syncPromise.catch(() => undefined);
      return this.indexingEnvelope();
    }
    return { ...completed.result };
  }

  async getSessionToken(): Promise<Record<string, unknown>> {
    const token = randomUUID();
    return {
      status: "ok",
      data: {
        token,
        marker: markerForToken(token)
      }
    };
  }

  async getSessionByToken(args: { token: string }): Promise<Record<string, unknown>> {
    const token = args.token.trim().toLowerCase();
    if (!isUuid(token)) {
      return { status: "invalid", error: "token must be a UUID returned by codex_session_get_session_token" };
    }

    const synced = await this.syncNowForTokenLookup();
    let rows = this.locatorRows(token);
    if (!synced) {
      const deadline = Date.now() + this.waitForIdleMs;
      while (rows.length === 0 && Date.now() < deadline) {
        await delay(100);
        rows = this.locatorRows(token);
      }
    }

    if (rows.length === 0) {
      return {
        status: "pending",
        error: "token has not appeared in the indexed Codex session JSONL yet; retry after the tool call is written",
        data: { token }
      };
    }

    const sessionIds = [...new Set(rows.map((row) => row.session_id))];
    if (sessionIds.length !== 1) {
      return {
        status: "ambiguous",
        error: "token appeared in multiple sessions; this should not happen for a freshly generated UUID",
        match_count: sessionIds.length
      };
    }

    const row = rows[0];
    return {
      status: "ok",
      data: {
        token,
        session_id: row.session_id,
        thread_name: row.thread_name,
        cwd: row.cwd,
        updated_at: row.updated_at,
        archive_scope: row.archive_scope,
        sequence: row.sequence,
        timestamp: row.timestamp,
        occurrences: rows.map((occurrence) => ({
          sequence: occurrence.sequence,
          timestamp: occurrence.timestamp,
          call_id: occurrence.call_id,
          tool_name: occurrence.tool_name,
          source: occurrence.source
        }))
      }
    };
  }

  async listSessions(args: {
    archive_scope?: ArchiveScope;
    updated_from?: string;
    updated_to?: string;
    limit?: number;
    order?: SortOrder;
  }): Promise<Record<string, unknown>> {
    const ready = await this.ensureReady();
    if (!ready) return this.indexingEnvelope();

    const archiveScope = args.archive_scope ?? "active";
    const order = parseOrder(args.order);
    const limit = parseLimit(args.limit, DEFAULT_LIST_LIMIT);
    const params: unknown[] = [];
    const where: string[] = [];
    addArchiveScope(where, params, archiveScope, "archive_scope");
    const from = parseTimeToUtcIso(args.updated_from);
    const to = parseTimeToUtcIso(args.updated_to);
    if (from) {
      where.push("updated_at >= ?");
      params.push(from);
    }
    if (to) {
      where.push("updated_at <= ?");
      params.push(to);
    }
    params.push(limit);
    const rows = this.db
      .prepare(
        `SELECT session_id AS id, thread_name, cwd, updated_at, archive_scope
         FROM sessions
         ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
         ORDER BY updated_at ${order.toUpperCase()}, session_id ${order.toUpperCase()}
         LIMIT ?`
      )
      .all(...params);
    return { status: "ok", data: { sessions: rows } };
  }

  async findByText(args: {
    text: string;
    archive_scope?: ArchiveScope;
    include_candidates?: boolean;
    max_chars?: number;
  }): Promise<Record<string, unknown>> {
    const ready = await this.ensureReady();
    if (!ready) return this.indexingEnvelope();
    const text = args.text.trim();
    if (effectiveLength(text) < 8) {
      return { status: "invalid", error: "text is too short; provide one distinctive sentence, usually 20-300 characters" };
    }

    const archiveScope = args.archive_scope ?? "active";
    const pattern = `%${text}%`;
    const params: unknown[] = [pattern, pattern];
    const where = ["(searchable_text LIKE ? OR token LIKE ?)"];
    addArchiveScope(where, params, archiveScope, "archive_scope");

    const rows = this.db
      .prepare(
        `SELECT
           session_id, thread_name, cwd, updated_at, archive_scope,
           sequence, timestamp, source_type, role, searchable_text,
           token, call_id, tool_name, occurrences
         FROM (
           SELECT
             s.session_id, s.thread_name, s.cwd, s.updated_at, s.archive_scope,
             m.sequence, m.timestamp, 'message' AS source_type,
             m.role, m.content_text AS searchable_text,
             NULL AS token, NULL AS call_id, NULL AS tool_name,
             1 AS occurrences
           FROM messages m
           JOIN sessions s ON s.session_id = m.session_id

           UNION ALL

           SELECT
             s.session_id, s.thread_name, s.cwd, s.updated_at, s.archive_scope,
             i.sequence, i.timestamp, 'published_task_retrieval' AS source_type,
             NULL AS role, i.task_text AS searchable_text,
             i.token, i.call_id, i.tool_name, grouped.occurrences
           FROM session_task_inputs i
           JOIN (
             SELECT session_id, token, MAX(sequence) AS sequence, COUNT(*) AS occurrences
             FROM session_task_inputs
             GROUP BY session_id, token
           ) grouped
             ON grouped.session_id = i.session_id
            AND grouped.token = i.token
            AND grouped.sequence = i.sequence
           JOIN sessions s ON s.session_id = i.session_id
         )
         WHERE ${where.join(" AND ")}
         ORDER BY archive_scope ASC, updated_at DESC, sequence DESC
         LIMIT 25`
      )
      .all(...params) as FindTextRow[];

    if (rows.length === 0) {
      return { status: "not_found", error: "no message or published task retrieval matched the provided text" };
    }
    if (rows.length !== 1) {
      const response: Record<string, unknown> = {
        status: "ambiguous",
        error: "text matched multiple messages or published task retrievals; provide a longer, more distinctive original snippet",
        match_count: rows.length
      };
      if (args.include_candidates) {
        response.candidates = rows.map((row) => ({
          session_id: row.session_id,
          thread_name: row.thread_name,
          updated_at: row.updated_at,
          archive_scope: row.archive_scope,
          ...findTextMatch(row, text, args.max_chars ?? 500)
        }));
      }
      return response;
    }

    const row = rows[0];
    const match = findTextMatch(row, text, args.max_chars ?? 500);
    const legacyMessage = row.source_type === "message"
      ? {
          sequence: row.sequence,
          timestamp: row.timestamp,
          role: row.role,
          snippet: match.snippet
        }
      : undefined;
    return {
      status: "ok",
      data: {
        session_id: row.session_id,
        thread_name: row.thread_name,
        cwd: row.cwd,
        updated_at: row.updated_at,
        archive_scope: row.archive_scope,
        match,
        ...(legacyMessage ? { message: legacyMessage } : {})
      }
    };
  }

  async messages(args: {
    session_id: string;
    roles?: MessageRole[];
    time_from?: string;
    time_to?: string;
    index_from?: number;
    index_to?: number;
    around_sequence?: number;
    before_count?: number;
    after_count?: number;
    limit?: number;
    order?: SortOrder;
    include_raw?: boolean;
    max_chars?: number;
  }): Promise<Record<string, unknown>> {
    const ready = await this.ensureReady();
    if (!ready) return this.indexingEnvelope();
    const order = parseOrder(args.order);
    const maxChars = parseLimit(args.max_chars, DEFAULT_TEXT_CHARS, 100_000);
    const from = parseTimeToUtcIso(args.time_from);
    const to = parseTimeToUtcIso(args.time_to);
    const view = resolveSessionHistory(this.db, args.session_id);
    let rows: MessageQueryRow[] = view.segments.flatMap((segment) => {
      const params: unknown[] = [segment.sessionId, segment.minSequence];
      const where = ["m.session_id = ?", "m.sequence >= ?"];
      if (segment.maxSequence !== null) {
        where.push("m.sequence <= ?");
        params.push(segment.maxSequence);
      }
      if (args.roles?.length) {
        where.push(`m.role IN (${args.roles.map(() => "?").join(", ")})`);
        params.push(...args.roles);
      }
      if (from) {
        where.push("m.timestamp >= ?");
        params.push(from);
      }
      if (to) {
        where.push("m.timestamp <= ?");
        params.push(to);
      }
      const rawSelect = args.include_raw ? ", r.raw_json" : "";
      const rawJoin = args.include_raw ? "JOIN raw_events r ON r.id = m.raw_event_id" : "";
      return (this.db
        .prepare(
          `SELECT m.sequence, m.timestamp, m.role, m.content_text, m.content_json${rawSelect}
           FROM messages m
           ${rawJoin}
           WHERE ${where.join(" AND ")}`
        )
        .all(...params) as MessageQueryRow[])
        .map((row) => ({ ...row, sequence: row.sequence + segment.sequenceOffset }));
    });
    if (args.around_sequence !== undefined) {
      const before = parseLimit(args.before_count, 5, 100);
      const after = parseLimit(args.after_count, 5, 100);
      rows = rows.filter((row) =>
        row.sequence >= args.around_sequence! - before &&
        row.sequence <= args.around_sequence! + after
      );
    } else {
      if (args.index_from !== undefined) {
        rows = rows.filter((row) => row.sequence >= args.index_from!);
      }
      if (args.index_to !== undefined) {
        rows = rows.filter((row) => row.sequence <= args.index_to!);
      }
    }
    rows.sort((left, right) => order === "asc" ? left.sequence - right.sequence : right.sequence - left.sequence);
    rows = rows.slice(0, parseLimit(args.limit, DEFAULT_LIST_LIMIT));

    return {
      status: "ok",
      data: {
        session_id: args.session_id,
        messages: rows.map((row) => ({
          sequence: row.sequence,
          timestamp: row.timestamp,
          role: row.role,
          content_text: truncateText(row.content_text, maxChars),
          content_json: row.content_json,
          raw_json: args.include_raw ? row.raw_json : undefined
        })),
        ...(view.parentHistoryStatus ? { parent_history_status: view.parentHistoryStatus } : {})
      }
    };
  }

  async recentUserInputs(args: { session_id: string; limit?: number; include_raw?: boolean; max_chars?: number }): Promise<Record<string, unknown>> {
    const ready = await this.ensureReady();
    if (!ready) return this.indexingEnvelope();
    const limit = parseLimit(args.limit, 3, 100);
    const maxChars = parseLimit(args.max_chars, DEFAULT_TEXT_CHARS, 100_000);
    const includeRaw = Boolean(args.include_raw);
    const view = resolveSessionHistory(this.db, args.session_id);
    const rows = view.segments.flatMap((segment) => {
      const range = segment.maxSequence === null ? "" : "AND m.sequence <= ?";
      const taskRange = segment.maxSequence === null ? "" : "AND i.sequence <= ?";
      const userParams: unknown[] = [segment.sessionId, segment.minSequence];
      const taskParams: unknown[] = [segment.sessionId, segment.minSequence];
      if (segment.maxSequence !== null) {
        userParams.push(segment.maxSequence);
        taskParams.push(segment.maxSequence);
      }
      const userRawJoin = includeRaw ? "JOIN raw_events user_raw ON user_raw.id = m.raw_event_id" : "";
      const taskRawJoin = includeRaw ? "JOIN raw_events task_raw ON task_raw.id = i.raw_event_id" : "";
      const userRawSelect = includeRaw ? "user_raw.raw_json" : "NULL";
      const taskRawSelect = includeRaw ? "task_raw.raw_json" : "NULL";
      const userRows = this.db
        .prepare(
          `SELECT m.sequence, m.timestamp, 'user_message' AS input_type,
                  m.role, m.content_text,
                  NULL AS token, NULL AS task_text, NULL AS call_id, NULL AS tool_name,
                  ${userRawSelect} AS raw_json
           FROM messages m
           ${userRawJoin}
           WHERE m.session_id = ? AND m.sequence >= ? ${range} AND m.role = 'user'`
        )
        .all(...userParams) as Array<Record<string, unknown> & { sequence: number }>;
      const taskRows = this.db
        .prepare(
          `SELECT i.sequence, i.timestamp, 'published_task_retrieval' AS input_type,
                  NULL AS role, NULL AS content_text,
                  i.token, i.task_text, i.call_id, i.tool_name,
                  ${taskRawSelect} AS raw_json
           FROM session_task_inputs i
           ${taskRawJoin}
           WHERE i.session_id = ? AND i.sequence >= ? ${taskRange}`
        )
        .all(...taskParams) as Array<Record<string, unknown> & { sequence: number }>;
      return [...userRows, ...taskRows].map((row) => ({
        ...row,
        sequence: row.sequence + segment.sequenceOffset
      }));
    }).sort((left, right) => right.sequence - left.sequence).slice(0, limit) as Array<{
         sequence: number;
         timestamp: string | null;
         input_type: "user_message" | "published_task_retrieval";
        role: string | null;
        content_text: string | null;
        token: string | null;
        task_text: string | null;
        call_id: string | null;
         tool_name: string | null;
         raw_json: string | null;
       }>;

    const inputs = rows.map((row) => {
      const common = {
        sequence: row.sequence,
        timestamp: row.timestamp,
        input_type: row.input_type
      };
      if (row.input_type === "published_task_retrieval") {
        return {
          ...common,
          token: row.token,
          task: truncateText(row.task_text, maxChars),
          call_id: row.call_id,
          tool_name: row.tool_name,
          ...(includeRaw ? { raw_json: row.raw_json } : {})
        };
      }
      return {
        ...common,
        role: row.role,
        content_text: formatRecentUserInput(row.content_text, maxChars),
        ...(includeRaw ? { raw_json: row.raw_json } : {})
      };
    });

    return {
      status: "ok",
      data: {
        session_id: args.session_id,
        inputs,
        ...(view.parentHistoryStatus ? { parent_history_status: view.parentHistoryStatus } : {})
      }
    };
  }

  async toolCalls(args: {
    session_id: string;
    tool_name_contains?: string;
    time_from?: string;
    time_to?: string;
    has_output?: boolean;
    keyword?: string;
    limit?: number;
    order?: SortOrder;
    include_raw?: boolean;
    max_output_chars?: number;
  }): Promise<Record<string, unknown>> {
    const ready = await this.ensureReady();
    if (!ready) return this.indexingEnvelope();
    const order = parseOrder(args.order);
    const from = parseTimeToUtcIso(args.time_from);
    const to = parseTimeToUtcIso(args.time_to);
    const view = resolveSessionHistory(this.db, args.session_id);
    let rows: ToolQueryRow[] = view.segments.flatMap((segment) => {
      const params: unknown[] = [segment.sessionId, segment.minSequence];
      const where = ["t.session_id = ?", "t.sequence >= ?"];
      if (segment.maxSequence !== null) {
        where.push("t.sequence <= ?");
        params.push(segment.maxSequence);
      }
      if (args.tool_name_contains) {
        where.push("t.tool_name LIKE ?");
        params.push(`%${args.tool_name_contains}%`);
      }
      if (from) {
        where.push("t.timestamp >= ?");
        params.push(from);
      }
      if (to) {
        where.push("t.timestamp <= ?");
        params.push(to);
      }
      if (args.has_output !== undefined) {
        where.push(args.has_output ? "t.output_raw_event_id IS NOT NULL" : "t.output_raw_event_id IS NULL");
      }
      if (args.keyword) {
        where.push("(COALESCE(t.arguments_json, '') LIKE ? OR COALESCE(t.output_text, '') LIKE ? OR COALESCE(t.tool_name, '') LIKE ?)");
        params.push(`%${args.keyword}%`, `%${args.keyword}%`, `%${args.keyword}%`);
      }
      const rawSelect = args.include_raw ? ", call_raw.raw_json AS call_raw_json, out_raw.raw_json AS output_raw_json" : "";
      const rawJoin = args.include_raw
        ? "JOIN raw_events call_raw ON call_raw.id = t.call_raw_event_id LEFT JOIN raw_events out_raw ON out_raw.id = t.output_raw_event_id"
        : "";
      return (this.db
        .prepare(
          `SELECT
             t.sequence, t.timestamp, t.call_id, t.tool_name, t.arguments_json,
             t.output_sequence, t.output_timestamp, t.output_text, t.output_json${rawSelect}
           FROM tool_calls t
           ${rawJoin}
           WHERE ${where.join(" AND ")}`
        )
        .all(...params) as ToolQueryRow[]).map((row) => ({
          ...row,
          sequence: row.sequence + segment.sequenceOffset,
          output_sequence: row.output_sequence === null ? null : row.output_sequence + segment.sequenceOffset
        }));
    });
    rows.sort((left, right) => order === "asc" ? left.sequence - right.sequence : right.sequence - left.sequence);
    rows = rows.slice(0, parseLimit(args.limit, DEFAULT_LIST_LIMIT));
    const maxOutput = parseLimit(args.max_output_chars, DEFAULT_TOOL_OUTPUT_CHARS, 100_000);
    return {
      status: "ok",
      data: {
        session_id: args.session_id,
        tool_calls: rows.map((row) => ({
          sequence: row.sequence,
          timestamp: row.timestamp,
          call_id: row.call_id,
          tool_name: row.tool_name,
          arguments_json: row.arguments_json,
          output_sequence: row.output_sequence,
          output_timestamp: row.output_timestamp,
          output_text: truncateText(row.output_text, maxOutput),
          output_json: row.output_json,
          call_raw_json: args.include_raw ? row.call_raw_json : undefined,
          output_raw_json: args.include_raw ? row.output_raw_json : undefined
        })),
        ...(view.parentHistoryStatus ? { parent_history_status: view.parentHistoryStatus } : {})
      }
    };
  }

  async keywordSearch(args: {
    session_id: string;
    keywords?: string[];
    query?: string;
    match?: KeywordMatch;
    scope?: SearchScope;
    roles?: MessageRole[];
    time_from?: string;
    time_to?: string;
    limit?: number;
    order?: SortOrder;
    include_raw?: boolean;
    max_chars?: number;
  }): Promise<Record<string, unknown>> {
    const ready = await this.ensureReady();
    if (!ready) return this.indexingEnvelope();
    const keywords = nonEmptyKeywords(args.keywords, args.query);
    if (keywords.length === 0) {
      return { status: "invalid", error: "provide query or keywords" };
    }
    const scope = args.scope ?? "messages";
    const match = args.match ?? "any";
    const limit = parseLimit(args.limit, DEFAULT_SEARCH_LIMIT);
    const order = parseOrder(args.order);
    const maxChars = parseLimit(args.max_chars, DEFAULT_TEXT_CHARS, 100_000);
    const from = parseTimeToUtcIso(args.time_from);
    const to = parseTimeToUtcIso(args.time_to);

    const results: Array<Record<string, unknown>> = [];
    const view = resolveSessionHistory(this.db, args.session_id);
    for (const segment of view.segments) {
      if (scope === "messages" || scope === "all") {
        results.push(...this.searchMessages({ segment, keywords, match, roles: args.roles, from, to, order, includeRaw: Boolean(args.include_raw), maxChars }));
      }
      if (scope === "tool_calls" || scope === "tool_outputs" || scope === "all") {
        results.push(...this.searchToolCalls({ segment, keywords, match, scope, from, to, order, includeRaw: Boolean(args.include_raw), maxChars }));
      }
      if (scope === "raw_events" || scope === "all") {
        results.push(...this.searchRawEvents({ segment, keywords, match, from, to, order, includeRaw: Boolean(args.include_raw), maxChars }));
      }
    }

    results.sort((a, b) => {
      const left = Number(a.sequence);
      const right = Number(b.sequence);
      return order === "asc" ? left - right : right - left;
    });

    return {
      status: "ok",
      data: {
        session_id: args.session_id,
        keywords,
        match,
        scope,
        results: results.slice(0, limit),
        ...(view.parentHistoryStatus ? { parent_history_status: view.parentHistoryStatus } : {})
      }
    };
  }

  private searchMessages(options: {
    segment: HistorySegment;
    keywords: string[];
    match: KeywordMatch;
    roles?: MessageRole[];
    from?: string;
    to?: string;
    order: SortOrder;
    includeRaw: boolean;
    maxChars: number;
  }): Array<Record<string, unknown>> {
    const params: unknown[] = [options.segment.sessionId, options.segment.minSequence];
    const where = ["m.session_id = ?", "m.sequence >= ?"];
    if (options.segment.maxSequence !== null) {
      where.push("m.sequence <= ?");
      params.push(options.segment.maxSequence);
    }
    if (options.roles?.length) {
      where.push(`m.role IN (${options.roles.map(() => "?").join(", ")})`);
      params.push(...options.roles);
    }
    if (options.from) {
      where.push("m.timestamp >= ?");
      params.push(options.from);
    }
    if (options.to) {
      where.push("m.timestamp <= ?");
      params.push(options.to);
    }
    params.push(1000);
    const rawSelect = options.includeRaw ? ", r.raw_json" : "";
    const rawJoin = options.includeRaw ? "JOIN raw_events r ON r.id = m.raw_event_id" : "";
    const rows = this.db
      .prepare(
        `SELECT m.sequence, m.timestamp, m.role, m.content_text${rawSelect}
         FROM messages m
         ${rawJoin}
         WHERE ${where.join(" AND ")}
         ORDER BY m.sequence ${options.order.toUpperCase()}
         LIMIT ?`
      )
      .all(...params) as Array<Record<string, unknown> & { content_text: string }>;
    return rows.flatMap((row) => {
      const matched = matchedKeywords(row.content_text, options.keywords);
      if (!matchesMode(matched, options.keywords, options.match)) return [];
      return [
         {
           scope: "messages",
           sequence: Number(row.sequence) + options.segment.sequenceOffset,
          timestamp: row.timestamp,
          role: row.role,
          matched_keywords: matched,
          content_text: truncateText(row.content_text, options.maxChars),
          raw_json: options.includeRaw ? row.raw_json : undefined
        }
      ];
    });
  }

  private searchToolCalls(options: {
    segment: HistorySegment;
    keywords: string[];
    match: KeywordMatch;
    scope: SearchScope;
    from?: string;
    to?: string;
    order: SortOrder;
    includeRaw: boolean;
    maxChars: number;
  }): Array<Record<string, unknown>> {
    const params: unknown[] = [options.segment.sessionId, options.segment.minSequence];
    const where = ["t.session_id = ?", "t.sequence >= ?"];
    if (options.segment.maxSequence !== null) {
      where.push("t.sequence <= ?");
      params.push(options.segment.maxSequence);
    }
    if (options.from) {
      where.push("t.timestamp >= ?");
      params.push(options.from);
    }
    if (options.to) {
      where.push("t.timestamp <= ?");
      params.push(options.to);
    }
    params.push(1000);
    const rawSelect = options.includeRaw ? ", call_raw.raw_json AS call_raw_json, out_raw.raw_json AS output_raw_json" : "";
    const rawJoin = options.includeRaw
      ? "JOIN raw_events call_raw ON call_raw.id = t.call_raw_event_id LEFT JOIN raw_events out_raw ON out_raw.id = t.output_raw_event_id"
      : "";
    const rows = this.db
      .prepare(
        `SELECT t.sequence, t.timestamp, t.call_id, t.tool_name, t.arguments_json, t.output_text${rawSelect}
         FROM tool_calls t
         ${rawJoin}
         WHERE ${where.join(" AND ")}
         ORDER BY t.sequence ${options.order.toUpperCase()}
         LIMIT ?`
      )
      .all(...params) as Array<Record<string, unknown> & { arguments_json: string | null; output_text: string | null; tool_name: string }>;
    return rows.flatMap((row) => {
      const searchable =
        options.scope === "tool_outputs"
          ? row.output_text ?? ""
          : `${row.tool_name}\n${row.arguments_json ?? ""}\n${row.output_text ?? ""}`;
      const matched = matchedKeywords(searchable, options.keywords);
      if (!matchesMode(matched, options.keywords, options.match)) return [];
      return [
         {
           scope: "tool_calls",
           sequence: Number(row.sequence) + options.segment.sequenceOffset,
          timestamp: row.timestamp,
          call_id: row.call_id,
          tool_name: row.tool_name,
          matched_keywords: matched,
          arguments_json: row.arguments_json,
          output_text: truncateText(row.output_text, options.maxChars),
          call_raw_json: options.includeRaw ? row.call_raw_json : undefined,
          output_raw_json: options.includeRaw ? row.output_raw_json : undefined
        }
      ];
    });
  }

  private searchRawEvents(options: {
    segment: HistorySegment;
    keywords: string[];
    match: KeywordMatch;
    from?: string;
    to?: string;
    order: SortOrder;
    includeRaw: boolean;
    maxChars: number;
  }): Array<Record<string, unknown>> {
    const params: unknown[] = [options.segment.sessionId, options.segment.minSequence];
    const where = ["session_id = ?", "sequence >= ?"];
    if (options.segment.maxSequence !== null) {
      where.push("sequence <= ?");
      params.push(options.segment.maxSequence);
    }
    if (options.from) {
      where.push("timestamp >= ?");
      params.push(options.from);
    }
    if (options.to) {
      where.push("timestamp <= ?");
      params.push(options.to);
    }
    params.push(1000);
    const rows = this.db
      .prepare(
        `SELECT sequence, timestamp, event_type, payload_type, role, raw_json
         FROM raw_events
         WHERE ${where.join(" AND ")}
         ORDER BY sequence ${options.order.toUpperCase()}
         LIMIT ?`
      )
      .all(...params) as Array<Record<string, unknown> & { raw_json: string }>;
    return rows.flatMap((row) => {
      const matched = matchedKeywords(row.raw_json, options.keywords);
      if (!matchesMode(matched, options.keywords, options.match)) return [];
      return [
         {
           scope: "raw_events",
           sequence: Number(row.sequence) + options.segment.sequenceOffset,
          timestamp: row.timestamp,
          event_type: row.event_type,
          payload_type: row.payload_type,
          role: row.role,
          matched_keywords: matched,
          content_text: truncateText(row.raw_json, options.maxChars),
          raw_json: options.includeRaw ? row.raw_json : undefined
        }
      ];
    });
  }

  private async ensureReady(): Promise<boolean> {
    this.indexer.syncIfNeeded();
    if (!await this.indexer.waitForIdle(this.waitForIdleMs)) return false;
    return !this.indexer.status().indexing;
  }

  private async syncNowForTokenLookup(): Promise<boolean> {
    try {
      const syncPromise = this.indexer.sync({ force: false });
      const completed = await this.waitForSyncResult(syncPromise);
      if (!completed.done) {
        void syncPromise.catch(() => undefined);
        return false;
      }
      return true;
    } catch {
      this.indexer.syncIfNeeded();
      await this.indexer.waitForIdle(this.waitForIdleMs);
      return false;
    }
  }

  private locatorRows(token: string): LocatorLookupRow[] {
    return this.db
      .prepare(
        `SELECT
           l.token,
           l.session_id,
           l.sequence,
           l.timestamp,
           l.call_id,
           l.tool_name,
           l.source,
           s.thread_name,
           s.cwd,
           s.updated_at,
           s.archive_scope
         FROM session_locator_tokens l
         JOIN sessions s ON s.session_id = l.session_id
         WHERE l.token = ?
         ORDER BY l.timestamp DESC, l.sequence DESC`
      )
      .all(token) as LocatorLookupRow[];
  }

  private async waitForSyncResult(promise: Promise<SyncResult>): Promise<{ done: true; result: SyncResult } | { done: false }> {
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        promise.then((result) => ({ done: true as const, result })),
        new Promise<{ done: false }>((resolve) => {
          timer = setTimeout(() => resolve({ done: false }), this.waitForIdleMs);
        })
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private indexingEnvelope(): Record<string, unknown> {
    return { status: "indexing", data: this.indexer.status() };
  }
}

function addArchiveScope(where: string[], params: unknown[], archiveScope: ArchiveScope, column: string): void {
  if (archiveScope === "both") return;
  where.push(`${column} = ?`);
  params.push(archiveScope);
}

function matchesMode(matched: string[], keywords: string[], match: KeywordMatch): boolean {
  if (match === "all") return matched.length === keywords.length;
  return matched.length > 0;
}

function findTextMatch(row: FindTextRow, text: string, maxChars: number): Record<string, unknown> & { snippet: string } {
  const common = {
    input_type: row.source_type === "published_task_retrieval"
      ? "published_task_retrieval"
      : row.role === "user"
        ? "user_message"
        : "message",
    sequence: row.sequence,
    timestamp: row.timestamp,
    snippet: makeSnippet(row.searchable_text, text, maxChars)
  };
  if (row.source_type === "published_task_retrieval") {
    return {
      ...common,
      token: row.token,
      call_id: row.call_id,
      tool_name: row.tool_name,
      occurrences: row.occurrences
    };
  }
  return { ...common, role: row.role };
}

function effectiveLength(text: string): number {
  return text.replace(/\s+/g, "").length;
}

function formatRecentUserInput(value: string | null, maxChars: number): string {
  const persistentPrefix = PERSISTENT_SYSTEM_INPUT_PREFIXES.find((prefix) => value?.startsWith(prefix));
  if (persistentPrefix) {
    return `${persistentPrefix}${PERSISTENT_SYSTEM_INPUT_TRUNCATION_NOTICE}`;
  }
  return truncateText(value, maxChars);
}

function makeSnippet(text: string, query: string, maxChars: number): string {
  const index = text.indexOf(query);
  if (index < 0) return truncateText(text, maxChars);
  const side = Math.max(0, Math.floor((maxChars - query.length) / 2));
  const start = Math.max(0, index - side);
  const end = Math.min(text.length, index + query.length + side);
  const prefix = start > 0 ? "..." : "";
  const suffix = end < text.length ? "..." : "";
  return `${prefix}${text.slice(start, end)}${suffix}`;
}

function markerForToken(token: string): string {
  return `codex-session-locator:${token}`;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
