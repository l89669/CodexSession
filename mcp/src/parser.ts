import fs from "node:fs";
import path from "node:path";
import { CodexSessionMeta, MessageRole } from "./types.js";
import { jsonString, normalizeRole, safeJsonParse, textFromContent } from "./util.js";

export interface ParsedRawEvent {
  lineNo: number;
  sequence: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  role: string | null;
  rawJson: string;
  parsed: Record<string, unknown>;
  payload: Record<string, unknown>;
}

export interface ParsedMessage {
  sequence: number;
  timestamp: string | null;
  role: MessageRole;
  contentText: string;
  contentJson: string | null;
  rawLineNo: number;
}

export interface ParsedToolCall {
  sequence: number;
  timestamp: string | null;
  callId: string;
  toolName: string;
  argumentsJson: string | null;
  rawLineNo: number;
}

export interface ParsedToolOutput {
  sequence: number;
  timestamp: string | null;
  callId: string;
  outputText: string;
  outputJson: string | null;
  rawLineNo: number;
}

export interface ParsedSessionFile {
  meta: CodexSessionMeta;
  rawEvents: ParsedRawEvent[];
  messages: ParsedMessage[];
  toolCalls: ParsedToolCall[];
  toolOutputs: ParsedToolOutput[];
  lineCount: number;
}

export function parseSessionFile(filePath: string): ParsedSessionFile {
  const text = fs.readFileSync(filePath, "utf8");
  const lines = text.split(/\r?\n/);
  const rawEvents: ParsedRawEvent[] = [];
  const messages: ParsedMessage[] = [];
  const toolCalls: ParsedToolCall[] = [];
  const toolOutputs: ParsedToolOutput[] = [];
  let meta: CodexSessionMeta | undefined;
  let sequence = 0;

  for (let i = 0; i < lines.length; i += 1) {
    const rawJson = lines[i]?.trimEnd() ?? "";
    if (rawJson.trim() === "") continue;
    const parsed = safeJsonParse(rawJson);
    if (!parsed || typeof parsed !== "object") {
      continue;
    }

    sequence += 1;
    const record = parsed as Record<string, unknown>;
    const payload = asRecord(record.payload);
    const eventType = stringValue(record.type) ?? "unknown";
    const payloadType = stringValue(payload.type) ?? null;
    const role = stringValue(payload.role) ?? null;
    const timestamp = normalizeTimestamp(stringValue(record.timestamp));
    const raw: ParsedRawEvent = {
      lineNo: i + 1,
      sequence,
      timestamp,
      eventType,
      payloadType,
      role,
      rawJson,
      parsed: record,
      payload
    };
    rawEvents.push(raw);

    if (eventType === "session_meta") {
      if (!meta) meta = extractMeta(payload);
      continue;
    }

    if (eventType !== "response_item") continue;

    if (payloadType === "message") {
      const content = payload.content;
      const contentText = textFromContent(content);
      messages.push({
        sequence,
        timestamp,
        role: normalizeRole(role),
        contentText,
        contentJson: content === undefined ? null : jsonString(content),
        rawLineNo: raw.lineNo
      });
      continue;
    }

    if (payloadType === "function_call" || payloadType === "custom_tool_call") {
      const callId = stringValue(payload.call_id) ?? stringValue(payload.id);
      const toolName = stringValue(payload.name) ?? payloadType;
      if (!callId) continue;
      const argsValue = payload.arguments ?? payload.input;
      toolCalls.push({
        sequence,
        timestamp,
        callId,
        toolName,
        argumentsJson: argsValue === undefined ? null : stringifyArgument(argsValue),
        rawLineNo: raw.lineNo
      });
      continue;
    }

    if (payloadType === "function_call_output" || payloadType === "custom_tool_call_output") {
      const callId = stringValue(payload.call_id) ?? stringValue(payload.id);
      if (!callId) continue;
      const outputValue = payload.output ?? payload.result ?? payload.content;
      toolOutputs.push({
        sequence,
        timestamp,
        callId,
        outputText: textFromContent(outputValue),
        outputJson: outputValue === undefined ? null : jsonString(outputValue),
        rawLineNo: raw.lineNo
      });
    }
  }

  if (!meta?.id) {
    meta = {
      id: deriveSessionIdFromFile(filePath),
      timestamp: rawEvents[0]?.timestamp ?? undefined
    };
  }

  return {
    meta,
    rawEvents,
    messages,
    toolCalls,
    toolOutputs,
    lineCount: rawEvents.length
  };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) return value as Record<string, unknown>;
  return {};
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function normalizeTimestamp(value: string | undefined): string | null {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString();
}

function extractMeta(payload: Record<string, unknown>): CodexSessionMeta {
  return {
    id: stringValue(payload.id) ?? "",
    forked_from_id: stringValue(payload.forked_from_id),
    timestamp: stringValue(payload.timestamp),
    cwd: stringValue(payload.cwd),
    originator: stringValue(payload.originator),
    cli_version: stringValue(payload.cli_version),
    source: stringValue(payload.source),
    thread_source: stringValue(payload.thread_source)
  };
}

function deriveSessionIdFromFile(filePath: string): string {
  const base = path.basename(filePath, ".jsonl");
  const match = /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i.exec(base);
  return match?.[1] ?? base;
}

function stringifyArgument(value: unknown): string {
  if (typeof value === "string") return value;
  return jsonString(value);
}
