import fs from "node:fs";
import path from "node:path";
import { MessageRole, SortOrder } from "./types.js";

export const DEFAULT_LIST_LIMIT = 50;
export const DEFAULT_SEARCH_LIMIT = 20;
export const DEFAULT_TEXT_CHARS = 4000;
export const DEFAULT_TOOL_OUTPUT_CHARS = 2000;

export function nowIso(): string {
  return new Date().toISOString();
}

export function ensureDir(dirPath: string): void {
  fs.mkdirSync(dirPath, { recursive: true });
}

export function truncateText(value: string | null | undefined, maxChars: number): string {
  if (!value) return "";
  if (value.length <= maxChars) return value;
  return `${value.slice(0, Math.max(0, maxChars))}... [truncated ${value.length - maxChars} chars]`;
}

export function parseLimit(value: number | undefined, fallback: number, max = 500): number {
  if (!Number.isFinite(value) || value === undefined) return fallback;
  return Math.max(1, Math.min(max, Math.floor(value)));
}

export function parseOrder(value: SortOrder | undefined): SortOrder {
  return value === "asc" ? "asc" : "desc";
}

export function normalizeRole(value: unknown): MessageRole {
  if (value === "user" || value === "assistant" || value === "developer" || value === "system" || value === "tool") {
    return value;
  }
  return "unknown";
}

export function parseTimeToUtcIso(value: string | undefined): string | undefined {
  if (!value || value.trim() === "") return undefined;
  const trimmed = value.trim();
  const hasTimezone = /(?:Z|[+-]\d{2}:\d{2})$/i.test(trimmed);
  if (hasTimezone) {
    const parsed = new Date(trimmed);
    if (Number.isNaN(parsed.getTime())) throw new Error(`Invalid ISO timestamp: ${value}`);
    return parsed.toISOString();
  }

  const match = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?$/.exec(trimmed);
  if (!match) throw new Error(`Invalid local ISO timestamp: ${value}`);
  const [, y, mo, d, h = "0", mi = "0", s = "0", ms = "0"] = match;
  const parsed = new Date(
    Number(y),
    Number(mo) - 1,
    Number(d),
    Number(h),
    Number(mi),
    Number(s),
    Number(ms.padEnd(3, "0"))
  );
  if (Number.isNaN(parsed.getTime())) throw new Error(`Invalid local ISO timestamp: ${value}`);
  return parsed.toISOString();
}

export function jsonString(value: unknown): string {
  return JSON.stringify(value);
}

export function safeJsonParse(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

export function textFromContent(content: unknown): string {
  const parts: string[] = [];
  collectText(content, parts);
  return parts.join("\n").replace(/\r\n/g, "\n").trim();
}

function collectText(value: unknown, parts: string[]): void {
  if (value === null || value === undefined) return;
  if (typeof value === "string") {
    if (value.length > 0) parts.push(value);
    return;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    parts.push(String(value));
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectText(item, parts);
    return;
  }
  if (typeof value !== "object") return;

  const obj = value as Record<string, unknown>;
  const type = typeof obj.type === "string" ? obj.type : undefined;
  if (typeof obj.text === "string") {
    parts.push(obj.text);
    return;
  }
  if (typeof obj.message === "string") {
    parts.push(obj.message);
    return;
  }
  if (typeof obj.output === "string") {
    parts.push(obj.output);
    return;
  }
  if (type && (type.includes("image") || type.includes("file"))) {
    const name = typeof obj.name === "string" ? `:${obj.name}` : "";
    parts.push(`[${type}${name}]`);
    return;
  }
  if ("content" in obj) collectText(obj.content, parts);
}

export function listJsonlFiles(root: string, recursive: boolean): string[] {
  if (!fs.existsSync(root)) return [];
  const results: string[] = [];
  const entries = fs.readdirSync(root, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory() && recursive) {
      results.push(...listJsonlFiles(fullPath, recursive));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".jsonl")) {
      results.push(path.resolve(fullPath));
    }
  }
  return results.sort((a, b) => a.localeCompare(b));
}

export function containsLiteral(haystack: string | null | undefined, needle: string): boolean {
  return (haystack ?? "").includes(needle);
}

export function matchedKeywords(text: string, keywords: string[]): string[] {
  return keywords.filter((keyword) => keyword.length > 0 && text.includes(keyword));
}

export function nonEmptyKeywords(keywords: string[] | undefined, single?: string): string[] {
  const all = [...(keywords ?? []), ...(single ? [single] : [])]
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  return [...new Set(all)];
}

