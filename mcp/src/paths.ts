import os from "node:os";
import path from "node:path";

export interface RuntimePaths {
  codexHome: string;
  sessionsDir: string;
  archivedSessionsDir: string;
  indexDir: string;
  indexDbPath: string;
}

export function resolveCodexHome(explicit?: string): string {
  if (explicit && explicit.trim().length > 0) {
    return path.resolve(explicit);
  }
  const envHome = process.env.CODEX_HOME;
  if (envHome && envHome.trim().length > 0) {
    return path.resolve(envHome);
  }
  return path.join(os.homedir(), ".codex");
}

export function resolveRuntimePaths(options: { codexHome?: string; indexDbPath?: string } = {}): RuntimePaths {
  const codexHome = resolveCodexHome(options.codexHome);
  const indexDir = path.join(codexHome, "session-mcp");
  return {
    codexHome,
    sessionsDir: path.join(codexHome, "sessions"),
    archivedSessionsDir: path.join(codexHome, "archived_sessions"),
    indexDir,
    indexDbPath: options.indexDbPath ? path.resolve(options.indexDbPath) : path.join(indexDir, "index.sqlite")
  };
}

export function toPortablePath(filePath: string): string {
  return path.resolve(filePath);
}
