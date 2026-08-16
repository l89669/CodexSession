import fs from "node:fs";
import path from "node:path";
import { CodexSessionMeta, MessageRole } from "./types.js";
import { jsonString, normalizeRole, safeJsonParse, textFromContent } from "./util.js";

export interface ParsedRawEvent {
  lineNo: number;
  sequence: number;
  byteStart: number;
  byteLength: number;
  timestamp: string | null;
  eventType: string;
  payloadType: string | null;
  role: string | null;
  turnId: string | null;
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
  turnId: string | null;
}

export interface ParsedToolCall {
  sequence: number;
  timestamp: string | null;
  callId: string;
  toolName: string;
  argumentsJson: string | null;
  rawLineNo: number;
  turnId: string | null;
}

export interface ParsedToolOutput {
  sequence: number;
  timestamp: string | null;
  callId: string;
  outputText: string;
  outputJson: string | null;
  rawLineNo: number;
  turnId: string | null;
}

export interface ParsedSessionChunk {
  meta?: CodexSessionMeta;
  rawEvents: ParsedRawEvent[];
  messages: ParsedMessage[];
  toolCalls: ParsedToolCall[];
  toolOutputs: ParsedToolOutput[];
  lineCount: number;
  indexedBytes: number;
  currentTurnId: string | null;
}

export interface ParsedSessionFile extends ParsedSessionChunk {
  meta: CodexSessionMeta;
}

export function parseSessionFile(filePath: string, endByte = fs.statSync(filePath).size): ParsedSessionFile {
  const parsed = parseSessionChunk(filePath, {
    startByte: 0,
    endByte,
    startSequence: 0,
    currentTurnId: null
  });
  return {
    ...parsed,
    meta: parsed.meta ?? {
      id: deriveSessionIdFromFile(filePath),
      timestamp: parsed.rawEvents[0]?.timestamp ?? undefined
    }
  };
}

export function parseSessionChunk(
  filePath: string,
  options: {
    startByte: number;
    endByte: number;
    startSequence: number;
    currentTurnId: string | null;
  }
): ParsedSessionChunk {
  const length = Math.max(0, options.endByte - options.startByte);
  const buffer = Buffer.allocUnsafe(length);
  const handle = fs.openSync(filePath, "r");
  let bytesRead = 0;
  try {
    while (bytesRead < length) {
      const read = fs.readSync(handle, buffer, bytesRead, length - bytesRead, options.startByte + bytesRead);
      if (read === 0) break;
      bytesRead += read;
    }
  } finally {
    fs.closeSync(handle);
  }

  const rawEvents: ParsedRawEvent[] = [];
  const messages: ParsedMessage[] = [];
  const toolCalls: ParsedToolCall[] = [];
  const toolOutputs: ParsedToolOutput[] = [];
  let meta: CodexSessionMeta | undefined;
  let sequence = options.startSequence;
  let currentTurnId = options.currentTurnId;
  let lineStart = 0;
  let indexedLength = 0;

  const parseLine = (lineEnd: number, consumedEnd: number) => {
    let contentEnd = lineEnd;
    if (contentEnd > lineStart && buffer[contentEnd - 1] === 0x0d) contentEnd -= 1;
    const rawJson = buffer.subarray(lineStart, contentEnd).toString("utf8").trimEnd();
    if (rawJson.trim() === "") {
      indexedLength = consumedEnd;
      return;
    }
    const parsed = safeJsonParse(rawJson);
    if (!parsed || typeof parsed !== "object") {
      indexedLength = consumedEnd;
      return;
    }

    sequence += 1;
    const record = parsed as Record<string, unknown>;
    const payload = asRecord(record.payload);
    const eventType = stringValue(record.type) ?? "unknown";
    const payloadType = stringValue(payload.type) ?? null;
    const role = stringValue(payload.role) ?? null;
    const timestamp = normalizeTimestamp(stringValue(record.timestamp));
    if (eventType === "event_msg" && payloadType === "task_started") {
      currentTurnId = stringValue(payload.turn_id) ?? currentTurnId;
    }
    const raw: ParsedRawEvent = {
      lineNo: sequence,
      sequence,
      byteStart: options.startByte + lineStart,
      byteLength: Buffer.byteLength(rawJson, "utf8"),
      timestamp,
      eventType,
      payloadType,
      role,
      turnId: currentTurnId,
      rawJson,
      parsed: record,
      payload
    };
    rawEvents.push(raw);
    const finish = () => {
      if (eventType === "event_msg" && payloadType === "task_complete") currentTurnId = null;
      indexedLength = consumedEnd;
    };

    if (eventType === "session_meta") {
      if (!meta) meta = extractMeta(payload);
      finish();
      return;
    }

    if (eventType !== "response_item") {
      finish();
      return;
    }

    if (payloadType === "message") {
      const content = payload.content;
      const contentText = textFromContent(content);
      messages.push({
        sequence,
        timestamp,
        role: normalizeRole(role),
        contentText,
        contentJson: content === undefined ? null : jsonString(content),
        rawLineNo: raw.lineNo,
        turnId: currentTurnId
      });
      finish();
      return;
    }

    if (payloadType === "function_call" || payloadType === "custom_tool_call") {
      const callId = stringValue(payload.call_id) ?? stringValue(payload.id);
      const toolName = stringValue(payload.name) ?? payloadType;
      if (!callId) {
        finish();
        return;
      }
      const argsValue = payload.arguments ?? payload.input;
      toolCalls.push({
        sequence,
        timestamp,
        callId,
        toolName,
        argumentsJson: argsValue === undefined ? null : stringifyArgument(argsValue),
        rawLineNo: raw.lineNo,
        turnId: currentTurnId
      });
      finish();
      return;
    }

    if (payloadType === "function_call_output" || payloadType === "custom_tool_call_output") {
      const callId = stringValue(payload.call_id) ?? stringValue(payload.id);
      if (!callId) {
        finish();
        return;
      }
      const outputValue = payload.output ?? payload.result ?? payload.content;
      toolOutputs.push({
        sequence,
        timestamp,
        callId,
        outputText: textFromContent(outputValue),
        outputJson: outputValue === undefined ? null : jsonString(outputValue),
        rawLineNo: raw.lineNo,
        turnId: currentTurnId
      });
    }
    finish();
  };

  for (let i = 0; i < bytesRead; i += 1) {
    if (buffer[i] !== 0x0a) continue;
    parseLine(i, i + 1);
    indexedLength = i + 1;
    lineStart = i + 1;
  }
  if (lineStart < bytesRead) {
    const candidate = buffer.subarray(lineStart, bytesRead).toString("utf8").trimEnd();
    if (candidate.trim() !== "" && safeJsonParse(candidate) !== undefined) {
      parseLine(bytesRead, bytesRead);
      indexedLength = bytesRead;
    }
  }

  return {
    meta,
    rawEvents,
    messages,
    toolCalls,
    toolOutputs,
    lineCount: sequence,
    indexedBytes: options.startByte + indexedLength,
    currentTurnId
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
