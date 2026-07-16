export type ArchiveScope = "active" | "archived" | "both";
export type SortOrder = "asc" | "desc";
export type MessageRole = "user" | "assistant" | "developer" | "system" | "tool" | "unknown";
export type SearchScope = "messages" | "tool_calls" | "tool_outputs" | "raw_events" | "all";
export type KeywordMatch = "any" | "all";

export interface CodexSessionMeta {
  id: string;
  forked_from_id?: string;
  timestamp?: string;
  cwd?: string;
  originator?: string;
  cli_version?: string;
  source?: string;
  thread_source?: string;
}

export interface SessionFile {
  filePath: string;
  archiveScope: Exclude<ArchiveScope, "both">;
  mtimeMs: number;
  size: number;
}

export interface MessageRow {
  session_id: string;
  sequence: number;
  timestamp: string | null;
  role: MessageRole;
  content_text: string;
  content_json: string | null;
  raw_event_id: number;
}

export interface ToolCallRow {
  session_id: string;
  sequence: number;
  timestamp: string | null;
  call_id: string;
  tool_name: string;
  arguments_json: string | null;
  output_text: string | null;
  output_json: string | null;
  call_raw_event_id: number;
  output_raw_event_id: number | null;
}

export interface SyncResult {
  started_at: string;
  completed_at: string;
  files_seen: number;
  files_indexed: number;
  files_deleted: number;
  events_indexed: number;
  messages_indexed: number;
  tool_calls_indexed: number;
}

export interface IndexingStatus {
  indexing: boolean;
  started_at: string | null;
  completed_at: string | null;
  files_seen: number;
  files_indexed: number;
  events_indexed: number;
  error: string | null;
}

export interface ToolResultEnvelope<T> {
  status: "ok" | "indexing" | "not_found" | "pending" | "ambiguous" | "invalid";
  data?: T;
  error?: string;
}
