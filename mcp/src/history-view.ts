import crypto from "node:crypto";
import fs from "node:fs";
import { Db } from "./db.js";

export interface HistorySegment {
  sessionId: string;
  minSequence: number;
  maxSequence: number | null;
  sequenceOffset: number;
}

export interface SessionHistoryView {
  segments: HistorySegment[];
  parentHistoryStatus?: "included" | "boundary_mismatch";
}

interface LineageRow {
  parent_session_id: string;
  local_start_sequence: number;
  sequence_offset: number;
  parent_cutoff_sequence: number;
  boundary_byte_start: number | null;
  boundary_byte_length: number | null;
  boundary_hash: string | null;
}

export function resolveSessionHistory(db: Db, sessionId: string): SessionHistoryView {
  const visited = new Set<string>();
  const resolved = resolve(db, sessionId, null, 0, visited);
  return {
    segments: resolved.segments,
    ...(resolved.hasLineage ? { parentHistoryStatus: resolved.boundaryValid ? "included" : "boundary_mismatch" } : {})
  };
}

function resolve(
  db: Db,
  sessionId: string,
  maxSequence: number | null,
  targetOffset: number,
  visited: Set<string>
): { segments: HistorySegment[]; hasLineage: boolean; boundaryValid: boolean } {
  if (visited.has(sessionId)) {
    return {
      segments: [segment(sessionId, 1, maxSequence, targetOffset)],
      hasLineage: false,
      boundaryValid: true
    };
  }
  visited.add(sessionId);
  try {
    const lineage = db
      .prepare(
        `SELECT parent_session_id, local_start_sequence, sequence_offset, parent_cutoff_sequence,
                boundary_byte_start, boundary_byte_length, boundary_hash
         FROM session_lineage
         WHERE session_id = ?`
      )
      .get(sessionId) as LineageRow | undefined;
    if (!lineage) {
      return {
        segments: maxSequence !== null && maxSequence < 1
          ? []
          : [segment(sessionId, 1, maxSequence, targetOffset)],
        hasLineage: false,
        boundaryValid: true
      };
    }

    const translatedMax = maxSequence === null
      ? lineage.parent_cutoff_sequence
      : Math.min(lineage.parent_cutoff_sequence, maxSequence - lineage.sequence_offset);
    const boundaryValid = validateBoundary(db, lineage);
    const parent = boundaryValid && translatedMax >= 1
      ? resolve(
          db,
          lineage.parent_session_id,
          translatedMax,
          targetOffset + lineage.sequence_offset,
          visited
        )
      : { segments: [], hasLineage: false, boundaryValid };
    const includeLocal = maxSequence === null || maxSequence >= lineage.local_start_sequence;
    const local = includeLocal
      ? [segment(sessionId, lineage.local_start_sequence, maxSequence, targetOffset)]
      : [];
    return {
      segments: [...parent.segments, ...local],
      hasLineage: true,
      boundaryValid: boundaryValid && parent.boundaryValid
    };
  } finally {
    visited.delete(sessionId);
  }
}

function validateBoundary(db: Db, lineage: LineageRow): boolean {
  if (lineage.parent_cutoff_sequence === 0 && lineage.boundary_hash === null) return true;
  if (
    lineage.boundary_byte_start === null ||
    lineage.boundary_byte_length === null ||
    lineage.boundary_hash === null
  ) {
    return false;
  }
  const parent = db
    .prepare("SELECT file_path FROM sessions WHERE session_id = ?")
    .get(lineage.parent_session_id) as { file_path: string } | undefined;
  if (!parent || !fs.existsSync(parent.file_path)) return false;

  const buffer = Buffer.allocUnsafe(lineage.boundary_byte_length);
  const handle = fs.openSync(parent.file_path, "r");
  let read = 0;
  try {
    while (read < buffer.length) {
      const count = fs.readSync(
        handle,
        buffer,
        read,
        buffer.length - read,
        lineage.boundary_byte_start + read
      );
      if (count === 0) break;
      read += count;
    }
  } finally {
    fs.closeSync(handle);
  }
  if (read !== buffer.length) return false;
  return crypto.createHash("sha256").update(buffer).digest("hex") === lineage.boundary_hash;
}

function segment(
  sessionId: string,
  minSequence: number,
  maxSequence: number | null,
  sequenceOffset: number
): HistorySegment {
  return { sessionId, minSequence, maxSequence, sequenceOffset };
}

