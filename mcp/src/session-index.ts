import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { safeJsonParse } from "./util.js";

export interface SessionIndexEntry {
  id: string;
  thread_name?: string;
  updated_at?: string;
}

export interface ThreadStateEntry {
  id: string;
  rolloutPath: string;
  archived: boolean;
  updatedAt: number;
}

export function readSessionIndex(codexHome: string): Map<string, SessionIndexEntry> {
  const indexPath = path.join(codexHome, "session_index.jsonl");
  const map = new Map<string, SessionIndexEntry>();
  if (!fs.existsSync(indexPath)) return map;
  const lines = fs.readFileSync(indexPath, "utf8").split(/\r?\n/);
  for (const line of lines) {
    if (line.trim() === "") continue;
    const parsed = safeJsonParse(line);
    if (!parsed || typeof parsed !== "object") continue;
    const entry = parsed as Record<string, unknown>;
    if (typeof entry.id !== "string") continue;
    map.set(entry.id, {
      id: entry.id,
      thread_name: typeof entry.thread_name === "string" ? entry.thread_name : undefined,
      updated_at: typeof entry.updated_at === "string" ? entry.updated_at : undefined
    });
  }
  return map;
}

export function readThreadState(codexHome: string): Map<string, ThreadStateEntry> {
  const statePath = path.join(codexHome, "state_5.sqlite");
  const result = new Map<string, ThreadStateEntry>();
  if (!fs.existsSync(statePath)) return result;

  let db: Database.Database | undefined;
  try {
    db = new Database(statePath, { readonly: true, fileMustExist: true, timeout: 1_000 });
    const rows = db
      .prepare("SELECT id, rollout_path, archived, updated_at FROM threads WHERE rollout_path IS NOT NULL")
      .all() as Array<{ id: string; rollout_path: string; archived: number; updated_at: number }>;
    for (const row of rows) {
      result.set(row.id.toLowerCase(), {
        id: row.id.toLowerCase(),
        rolloutPath: normalizeStatePath(row.rollout_path),
        archived: Boolean(row.archived),
        updatedAt: row.updated_at
      });
    }
  } catch {
    return new Map();
  } finally {
    db?.close();
  }
  return result;
}

function normalizeStatePath(value: string): string {
  const withoutExtendedPrefix = value.startsWith("\\\\?\\") ? value.slice(4) : value;
  return path.resolve(withoutExtendedPrefix);
}
