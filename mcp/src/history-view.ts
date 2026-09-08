import crypto from "node:crypto";
import fs from "node:fs";
import { Db } from "./db.js";

export interface HistorySegment {
  rolloutId: string;
  minSequence: number;
  maxSequence: number | null;
  sequenceOffset: number;
}

export interface SessionHistoryView {
  segments: HistorySegment[];
  parentHistoryStatus?: "included" | "boundary_mismatch";
}

interface HistoryRow {
  base_rollout_id: string;
  local_start_sequence: number;
  sequence_offset: number;
  parent_cutoff_sequence: number;
  boundary_byte_start: number | null;
  boundary_byte_length: number | null;
  boundary_hash: string | null;
}

export function resolveSessionHistory(db: Db, sessionId: string): SessionHistoryView {
  const session = db
    .prepare("SELECT current_rollout_id FROM sessions WHERE session_id = ?")
    .get(sessionId) as { current_rollout_id: string | null } | undefined;
  if (!session?.current_rollout_id) return { segments: [] };

  const resolved = resolve(db, session.current_rollout_id, null, 0, new Set());
  return {
    segments: resolved.segments,
    ...(resolved.hasHistory
      ? { parentHistoryStatus: resolved.boundaryValid ? "included" : "boundary_mismatch" }
      : {})
  };
}

function resolve(
  db: Db,
  rolloutId: string,
  maxSequence: number | null,
  targetOffset: number,
  visited: Set<string>
): { segments: HistorySegment[]; hasHistory: boolean; boundaryValid: boolean } {
  const rollout = db
    .prepare("SELECT first_sequence FROM rollouts WHERE rollout_id = ?")
    .get(rolloutId) as { first_sequence: number } | undefined;
  if (!rollout) return { segments: [], hasHistory: false, boundaryValid: false };
  if (visited.has(rolloutId)) {
    return {
      segments: [segment(rolloutId, rollout.first_sequence, maxSequence, targetOffset)],
      hasHistory: false,
      boundaryValid: false
    };
  }

  visited.add(rolloutId);
  try {
    const history = db
      .prepare(
        `SELECT base_rollout_id, local_start_sequence, sequence_offset, parent_cutoff_sequence,
                boundary_byte_start, boundary_byte_length, boundary_hash
         FROM rollout_history
         WHERE rollout_id = ?`
      )
      .get(rolloutId) as HistoryRow | undefined;
    if (!history) {
      return {
        segments: maxSequence !== null && maxSequence < rollout.first_sequence
          ? []
          : [segment(rolloutId, rollout.first_sequence, maxSequence, targetOffset)],
        hasHistory: false,
        boundaryValid: true
      };
    }

    const translatedMax = maxSequence === null
      ? history.parent_cutoff_sequence
      : Math.min(history.parent_cutoff_sequence, maxSequence - history.sequence_offset);
    const boundaryValid = validateBoundary(db, history);
    const parent = boundaryValid
      ? resolve(
          db,
          history.base_rollout_id,
          translatedMax,
          targetOffset + history.sequence_offset,
          visited
        )
      : { segments: [], hasHistory: false, boundaryValid: false };
    const includeLocal = maxSequence === null || maxSequence >= history.local_start_sequence;
    const local = includeLocal
      ? [segment(rolloutId, history.local_start_sequence, maxSequence, targetOffset)]
      : [];
    return {
      segments: [...parent.segments, ...local],
      hasHistory: true,
      boundaryValid: boundaryValid && parent.boundaryValid
    };
  } finally {
    visited.delete(rolloutId);
  }
}

function validateBoundary(db: Db, history: HistoryRow): boolean {
  if (history.parent_cutoff_sequence <= 0 && history.boundary_hash === null) return true;
  if (
    history.boundary_byte_start === null ||
    history.boundary_byte_length === null ||
    history.boundary_hash === null
  ) {
    return false;
  }
  const base = db
    .prepare("SELECT file_path FROM rollouts WHERE rollout_id = ?")
    .get(history.base_rollout_id) as { file_path: string } | undefined;
  if (!base || !fs.existsSync(base.file_path)) return false;

  const buffer = Buffer.allocUnsafe(history.boundary_byte_length);
  const handle = fs.openSync(base.file_path, "r");
  let read = 0;
  try {
    while (read < buffer.length) {
      const count = fs.readSync(
        handle,
        buffer,
        read,
        buffer.length - read,
        history.boundary_byte_start + read
      );
      if (count === 0) break;
      read += count;
    }
  } finally {
    fs.closeSync(handle);
  }
  if (read !== buffer.length) return false;
  return crypto.createHash("sha256").update(buffer).digest("hex") === history.boundary_hash;
}

function segment(
  rolloutId: string,
  minSequence: number,
  maxSequence: number | null,
  sequenceOffset: number
): HistorySegment {
  return { rolloutId, minSequence, maxSequence, sequenceOffset };
}
