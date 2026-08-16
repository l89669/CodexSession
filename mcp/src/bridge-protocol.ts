import fs from "node:fs";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolveRuntimePaths } from "./paths.js";

export const BRIDGE_HOST = "127.0.0.1";
export const DEFAULT_BRIDGE_PORT = 49321;
export const DEFAULT_IDLE_TIMEOUT_MS = 60_000;
export const DEFAULT_STARTUP_TIMEOUT_MS = 20_000;
export const BRIDGE_SERVICE = "codex-session-context";
export const BRIDGE_PROTOCOL_VERSION = "1";
export const BRIDGE_SERVICE_HEADER = "x-codex-session-service";
export const BRIDGE_PROTOCOL_HEADER = "x-codex-session-bridge-version";
export const BRIDGE_INSTANCE_HEADER = "x-codex-session-instance";
export const PLUGIN_VERSION = readPluginVersion();

export interface BridgeConfiguration {
  port: number;
  idleTimeoutMs: number;
  startupTimeoutMs: number;
}

export function bridgeConfigurationFromEnv(env: NodeJS.ProcessEnv = process.env): BridgeConfiguration {
  return {
    port: positiveInteger(env.CODEX_SESSION_MCP_PORT, DEFAULT_BRIDGE_PORT, 65_535),
    idleTimeoutMs: positiveInteger(env.CODEX_SESSION_MCP_IDLE_TIMEOUT_MS, DEFAULT_IDLE_TIMEOUT_MS),
    startupTimeoutMs: positiveInteger(env.CODEX_SESSION_MCP_STARTUP_TIMEOUT_MS, DEFAULT_STARTUP_TIMEOUT_MS)
  };
}

export function bridgeInstanceId(options: { codexHome?: string; indexDbPath?: string } = {}): string {
  const paths = resolveRuntimePaths(options);
  return crypto
    .createHash("sha256")
    .update(`${paths.codexHome}\n${paths.indexDbPath}`)
    .digest("hex")
    .slice(0, 24);
}

function positiveInteger(value: string | undefined, fallback: number, maximum = Number.MAX_SAFE_INTEGER): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 && parsed <= maximum ? parsed : fallback;
}

function readPluginVersion(): string {
  try {
    const manifestPath = fileURLToPath(new URL("../../../.codex-plugin/plugin.json", import.meta.url));
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8")) as { version?: unknown };
    return typeof manifest.version === "string" ? manifest.version : "0.1.0";
  } catch {
    return "0.1.0";
  }
}
