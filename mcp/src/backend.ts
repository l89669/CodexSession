#!/usr/bin/env node
import http, { IncomingMessage, ServerResponse } from "node:http";
import { fileURLToPath } from "node:url";
import {
  BRIDGE_HOST,
  BRIDGE_INSTANCE_HEADER,
  BRIDGE_PROTOCOL_HEADER,
  BRIDGE_PROTOCOL_VERSION,
  BRIDGE_SERVICE,
  BRIDGE_SERVICE_HEADER,
  PLUGIN_VERSION,
  bridgeConfigurationFromEnv,
  bridgeInstanceId
} from "./bridge-protocol.js";
import { HttpBridgeServerTransport } from "./http-bridge-transport.js";
import { createSessionRuntime, SessionRuntime } from "./runtime.js";
import { createMcpServer } from "./server.js";

interface BackendOptions {
  port?: number;
  idleTimeoutMs?: number;
  codexHome?: string;
  indexDbPath?: string;
  busyTimeoutMs?: number;
}

export async function runBackend(options: BackendOptions = {}): Promise<"stopped" | "port_in_use"> {
  const envConfiguration = bridgeConfigurationFromEnv();
  const daemon = new BackendDaemon({
    port: options.port ?? envConfiguration.port,
    idleTimeoutMs: options.idleTimeoutMs ?? envConfiguration.idleTimeoutMs,
    codexHome: options.codexHome ?? process.env.CODEX_HOME,
    indexDbPath: options.indexDbPath ?? process.env.CODEX_SESSION_MCP_DB,
    busyTimeoutMs: options.busyTimeoutMs ?? parseBusyTimeout(process.env.CODEX_SESSION_MCP_BUSY_TIMEOUT_MS)
  });
  return daemon.run();
}

class BackendDaemon {
  private readonly server = http.createServer((request, response) => {
    void this.handleRequest(request, response);
  });
  private readonly transports = new Set<HttpBridgeServerTransport>();
  private resolveStopped!: () => void;
  private readonly stopped = new Promise<void>((resolve) => {
    this.resolveStopped = resolve;
  });
  private runtime: SessionRuntime | undefined;
  private idleTimer: NodeJS.Timeout | undefined;
  private shuttingDown = false;
  private readonly instanceId: string;

  constructor(
    private readonly options: Required<Pick<BackendOptions, "port" | "idleTimeoutMs">> &
      Pick<BackendOptions, "codexHome" | "indexDbPath" | "busyTimeoutMs">
  ) {
    this.instanceId = bridgeInstanceId({ codexHome: options.codexHome, indexDbPath: options.indexDbPath });
  }

  async run(): Promise<"stopped" | "port_in_use"> {
    if (!await listen(this.server, this.options.port)) return "port_in_use";

    try {
      this.runtime = createSessionRuntime({
        codexHome: this.options.codexHome,
        indexDbPath: this.options.indexDbPath,
        busyTimeoutMs: this.options.busyTimeoutMs
      });
    } catch (error) {
      await closeServer(this.server);
      throw error;
    }

    process.once("SIGINT", this.handleSignal);
    process.once("SIGTERM", this.handleSignal);
    this.armIdleTimer();
    await this.stopped;
    return "stopped";
  }

  private async handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const pathname = new URL(request.url ?? "/", `http://${BRIDGE_HOST}`).pathname;
    if (request.method === "GET" && pathname === "/health") {
      this.writeHealth(response);
      return;
    }
    if (request.method !== "POST" || pathname !== "/bridge") {
      writeJson(response, 404, { error: "not_found" });
      return;
    }
    if (!this.runtime || this.shuttingDown) {
      writeJson(response, 503, { service: BRIDGE_SERVICE, status: "starting" });
      return;
    }
    if (
      request.headers[BRIDGE_SERVICE_HEADER] !== BRIDGE_SERVICE ||
      request.headers[BRIDGE_PROTOCOL_HEADER] !== BRIDGE_PROTOCOL_VERSION ||
      request.headers[BRIDGE_INSTANCE_HEADER] !== this.instanceId
    ) {
      writeJson(response, 409, {
        service: BRIDGE_SERVICE,
        bridge_protocol: BRIDGE_PROTOCOL_VERSION,
        instance_id: this.instanceId,
        error: "incompatible_bridge_client"
      });
      return;
    }

    this.cancelIdleTimer();
    response.writeHead(200, {
      "Content-Type": "application/x-ndjson",
      "Cache-Control": "no-store",
      Connection: "keep-alive"
    });
    response.flushHeaders();

    let transport!: HttpBridgeServerTransport;
    transport = new HttpBridgeServerTransport(request, response, () => {
      this.transports.delete(transport);
      if (this.transports.size === 0) this.armIdleTimer();
    });
    this.transports.add(transport);
    const mcpServer = createMcpServer(this.runtime);
    try {
      await mcpServer.connect(transport);
    } catch (error) {
      transport.onerror?.(asError(error));
      await transport.close();
    }
  }

  private writeHealth(response: ServerResponse): void {
    if (!this.runtime || this.shuttingDown) {
      writeJson(response, 503, { service: BRIDGE_SERVICE, status: "starting" });
      return;
    }
    writeJson(response, 200, {
      service: BRIDGE_SERVICE,
      bridge_protocol: BRIDGE_PROTOCOL_VERSION,
      plugin_version: PLUGIN_VERSION,
      instance_id: this.instanceId,
      pid: process.pid,
      active_connections: this.transports.size,
      idle_timeout_ms: this.options.idleTimeoutMs
    });
  }

  private armIdleTimer(): void {
    if (this.shuttingDown || this.transports.size !== 0) return;
    this.cancelIdleTimer();
    this.idleTimer = setTimeout(() => {
      void this.shutdown();
    }, this.options.idleTimeoutMs);
  }

  private cancelIdleTimer(): void {
    if (!this.idleTimer) return;
    clearTimeout(this.idleTimer);
    this.idleTimer = undefined;
  }

  private readonly handleSignal = () => {
    void this.shutdown();
  };

  private async shutdown(): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    this.cancelIdleTimer();
    process.off("SIGINT", this.handleSignal);
    process.off("SIGTERM", this.handleSignal);

    const closingServer = closeServer(this.server);
    await Promise.all([...this.transports].map((transport) => transport.close()));
    await closingServer;
    await this.runtime?.close();
    this.resolveStopped();
  }
}

function listen(server: http.Server, port: number): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const onError = (error: NodeJS.ErrnoException) => {
      server.off("listening", onListening);
      if (error.code === "EADDRINUSE") {
        resolve(false);
        return;
      }
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve(true);
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, BRIDGE_HOST);
  });
}

function closeServer(server: http.Server): Promise<void> {
  if (!server.listening) return Promise.resolve();
  return new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
}

function writeJson(response: ServerResponse, status: number, value: Record<string, unknown>): void {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify(value));
}

function parseBusyTimeout(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  void runBackend().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
