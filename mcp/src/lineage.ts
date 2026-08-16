import crypto from "node:crypto";
import { ParsedRawEvent, ParsedSessionFile } from "./parser.js";

export interface ForkLineageAnalysis {
  parentSessionId: string;
  replayParentSequence: number;
  localStartSequence: number;
  sequenceOffset: number;
  parentCutoffSequence: number;
  boundaryMessageSequence: number | null;
  boundaryByteStart: number | null;
  boundaryByteLength: number | null;
  boundaryHash: string | null;
}

interface TurnRange {
  turnId: string;
  startSequence: number;
  endSequence: number | null;
}

export function analyzeForkLineage(
  child: ParsedSessionFile,
  parent: ParsedSessionFile
): ForkLineageAnalysis | undefined {
  if (!child.meta.forked_from_id || child.meta.forked_from_id !== parent.meta.id) return undefined;
  if (child.rawEvents.length < 2 || parent.rawEvents.length === 0) return undefined;

  let replayed = 0;
  while (
    replayed < parent.rawEvents.length &&
    replayed + 1 < child.rawEvents.length &&
    replayEquivalent(parent.rawEvents[replayed], child.rawEvents[replayed + 1])
  ) {
    replayed += 1;
  }
  if (replayed === 0) return undefined;

  const localStartSequence = child.rawEvents[replayed + 1]?.sequence ?? child.lineCount + 1;
  const activeTurns = activeTurnsAt(parent.rawEvents, replayed);
  const setupRollbacks: ParsedRawEvent[] = [];
  for (const event of child.rawEvents) {
    if (event.sequence < localStartSequence) continue;
    if (event.eventType === "event_msg" && event.payloadType === "task_started") break;
    if (event.eventType === "event_msg" && event.payloadType === "thread_rolled_back") {
      setupRollbacks.push(event);
    }
  }

  for (const rollback of setupRollbacks) {
    const count = positiveInteger(rollback.payload.num_turns);
    if (count > 0) activeTurns.splice(Math.max(0, activeTurns.length - count), count);
  }

  const rollbackApplied = setupRollbacks.length > 0;
  const lastTurn = activeTurns.at(-1);
  const parentCutoffSequence = rollbackApplied
    ? lastTurn?.endSequence ?? lastTurn?.startSequence ?? 0
    : replayed;
  const boundaryMessage = [...parent.messages]
    .reverse()
    .find((message) =>
      message.sequence <= parentCutoffSequence &&
      (!lastTurn || message.turnId === lastTurn.turnId)
    );
  const boundaryRaw = boundaryMessage
    ? parent.rawEvents.find((event) => event.sequence === boundaryMessage.sequence)
    : undefined;

  return {
    parentSessionId: parent.meta.id,
    replayParentSequence: replayed,
    localStartSequence,
    sequenceOffset: child.rawEvents[1].sequence - parent.rawEvents[0].sequence,
    parentCutoffSequence,
    boundaryMessageSequence: boundaryRaw?.sequence ?? null,
    boundaryByteStart: boundaryRaw?.byteStart ?? null,
    boundaryByteLength: boundaryRaw?.byteLength ?? null,
    boundaryHash: boundaryRaw ? sha256(boundaryRaw.rawJson) : null
  };
}

function activeTurnsAt(events: ParsedRawEvent[], maxSequence: number): TurnRange[] {
  const active: TurnRange[] = [];
  const byId = new Map<string, TurnRange>();
  for (const event of events) {
    if (event.sequence > maxSequence) break;
    if (event.eventType !== "event_msg") continue;
    if (event.payloadType === "task_started") {
      const turnId = stringValue(event.payload.turn_id);
      if (!turnId) continue;
      const turn = { turnId, startSequence: event.sequence, endSequence: null };
      active.push(turn);
      byId.set(turnId, turn);
      continue;
    }
    if (event.payloadType === "task_complete") {
      const turnId = stringValue(event.payload.turn_id);
      const turn = turnId ? byId.get(turnId) : active.at(-1);
      if (turn) turn.endSequence = event.sequence;
      continue;
    }
    if (event.payloadType === "thread_rolled_back") {
      const count = positiveInteger(event.payload.num_turns);
      if (count > 0) active.splice(Math.max(0, active.length - count), count);
    }
  }
  return active;
}

function replayEquivalent(parent: ParsedRawEvent, child: ParsedRawEvent): boolean {
  return stableJson(replayValue(parent.parsed)) === stableJson(replayValue(child.parsed));
}

function replayValue(value: Record<string, unknown>): unknown {
  const copy = structuredClone(value);
  delete copy.timestamp;
  const payload = recordValue(copy.payload);
  if (copy.type === "response_item" && payload.type === "reasoning") {
    delete payload.content;
    delete payload.encrypted_content;
  }
  return copy;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([left], [right]) => left.localeCompare(right));
    return `{${entries.map(([key, inner]) => `${JSON.stringify(key)}:${stableJson(inner)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function positiveInteger(value: unknown): number {
  const number = typeof value === "number" ? value : Number(value);
  return Number.isInteger(number) && number > 0 ? number : 0;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function sha256(value: string): string {
  return crypto.createHash("sha256").update(value, "utf8").digest("hex");
}
