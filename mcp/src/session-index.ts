import fs from "node:fs";
import path from "node:path";
import { safeJsonParse } from "./util.js";

export interface SessionIndexEntry {
  id: string;
  thread_name?: string;
  updated_at?: string;
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

