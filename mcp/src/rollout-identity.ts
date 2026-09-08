import path from "node:path";
import { CodexSessionMeta } from "./types.js";

export interface RolloutIdentity {
  sessionId: string;
  rolloutId: string;
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

export function rolloutIdentity(filePath: string, meta: CodexSessionMeta): RolloutIdentity {
  const ids = [...path.basename(filePath).matchAll(UUID)].map((match) => match[0].toLowerCase());
  const metadataId = meta.id.trim().toLowerCase();
  const sessionId = ids[0] ?? metadataId;
  const rolloutId = ids.at(-1) ?? metadataId;

  if (!sessionId || !rolloutId) {
    throw new Error(`rollout file has no thread identity: ${filePath}`);
  }
  if (metadataId && metadataId !== sessionId) {
    throw new Error(`rollout filename thread id ${sessionId} disagrees with session_meta.id ${metadataId}: ${filePath}`);
  }
  if (ids.length > 2) {
    throw new Error(`rollout filename contains more than thread and rollout ids: ${filePath}`);
  }
  return { sessionId, rolloutId };
}

export function filenameIdentity(filePath: string): RolloutIdentity | undefined {
  const ids = [...path.basename(filePath).matchAll(UUID)].map((match) => match[0].toLowerCase());
  if (ids.length === 0 || ids.length > 2) return undefined;
  return { sessionId: ids[0], rolloutId: ids.at(-1) as string };
}
