import { spawn } from "node:child_process";
import crypto from "node:crypto";
import http, { ClientRequest, IncomingMessage } from "node:http";
import { fileURLToPath } from "node:url";
import type { JSONRPCMessage, RequestId } from "@modelcontextprotocol/sdk/types.js";
import { ReadBuffer, serializeMessage } from "@modelcontextprotocol/sdk/shared/stdio.js";
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

interface BridgeConnection {
  request: ClientRequest;
  response: IncomingMessage;
}

type HealthProbe =
  | { kind: "ready"; pid: number }
  | { kind: "unavailable" | "starting" }
  | { kind: "incompatible"; description: string };

const HEALTH_REQUEST_TIMEOUT_MS = 500;
const RECONNECT_DELAY_MS = 250;
const INTERNAL_INITIALIZE_ID_PREFIX = "codex-session-bridge-initialize:";

export async function runStdioBridge(): Promise<void> {
  const bridge = new StdioBridge();
  await bridge.run();
}

class StdioBridge {
  private readonly configuration = bridgeConfigurationFromEnv();
  private readonly instanceId = bridgeInstanceId({
    codexHome: process.env.CODEX_HOME,
    indexDbPath: process.env.CODEX_SESSION_MCP_DB
  });
  private readonly stdinBuffer = new ReadBuffer();
  private readonly responseBuffer = new ReadBuffer();
  private readonly queue: JSONRPCMessage[] = [];
  private readonly outstanding = new Map<string, JSONRPCMessage>();
  private resolveDone!: () => void;
  private rejectDone!: (error: Error) => void;
  private readonly done = new Promise<void>((resolve, reject) => {
    this.resolveDone = resolve;
    this.rejectDone = reject;
  });
  private request: ClientRequest | undefined;
  private response: IncomingMessage | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private connecting = false;
  private flushing = false;
  private stopped = false;
  private protocolReady = false;
  private initializeRequest: JSONRPCMessage | undefined;
  private initializeResponseSeen = false;
  private initializedNotification: JSONRPCMessage | undefined;
  private internalInitializeId: string | undefined;

  async run(): Promise<void> {
    process.stdin.on("data", this.handleStdinData);
    process.stdin.once("error", this.handleStdinError);
    process.stdin.once("end", this.handleStdinClose);
    process.stdin.once("close", this.handleStdinClose);
    process.once("SIGINT", this.handleSignal);
    process.once("SIGTERM", this.handleSignal);
    void this.connect();
    await this.done;
  }

  private readonly handleStdinData = (chunk: Buffer) => {
    this.stdinBuffer.append(chunk);
    while (!this.stopped) {
      try {
        const message = this.stdinBuffer.readMessage();
        if (message === null) return;
        if (isRequest(message) && message.method === "initialize") this.initializeRequest = message;
        if (isNotification(message) && message.method === "notifications/initialized") {
          this.initializedNotification = message;
        }
        this.queue.push(message);
        void this.flushQueue();
      } catch (error) {
        this.fail(asError(error));
        return;
      }
    }
  };

  private readonly handleStdinError = (error: Error) => {
    this.fail(error);
  };

  private readonly handleStdinClose = () => {
    this.stop();
  };

  private readonly handleSignal = () => {
    this.stop();
  };

  private async connect(): Promise<void> {
    if (this.stopped || this.connecting || this.request) return;
    this.connecting = true;
    try {
      await ensureBackend(this.configuration.port, this.configuration.startupTimeoutMs, this.instanceId);
      if (this.stopped) return;
      const connection = await openBridge(this.configuration.port, this.instanceId);
      if (this.stopped) {
        connection.request.destroy();
        return;
      }
      this.attachConnection(connection);
      if (this.initializeResponseSeen && this.initializeRequest) {
        await this.restoreProtocolSession();
      } else {
        this.protocolReady = true;
        await this.flushQueue();
      }
    } catch (error) {
      if (isIncompatible(error)) {
        this.fail(asError(error));
      } else if (this.request) {
        this.connectionLost(this.request, asError(error));
      } else {
        this.scheduleReconnect();
      }
    } finally {
      this.connecting = false;
    }
  }

  private attachConnection(connection: BridgeConnection): void {
    this.request = connection.request;
    this.response = connection.response;
    this.responseBuffer.clear();
    connection.request.once("error", (error) => this.connectionLost(connection.request, error));
    connection.response.on("data", (chunk: Buffer) => this.handleBackendData(connection.request, chunk));
    connection.response.once("end", () => this.connectionLost(connection.request));
    connection.response.once("close", () => this.connectionLost(connection.request));
    connection.response.once("error", (error) => this.connectionLost(connection.request, error));
  }

  private async restoreProtocolSession(): Promise<void> {
    if (!this.request || !this.initializeRequest || !isRequest(this.initializeRequest)) return;
    this.protocolReady = false;
    this.internalInitializeId = `${INTERNAL_INITIALIZE_ID_PREFIX}${crypto.randomUUID()}`;
    await this.writeMessage({ ...this.initializeRequest, id: this.internalInitializeId }, false);
  }

  private handleBackendData(connection: ClientRequest, chunk: Buffer): void {
    if (connection !== this.request || this.stopped) return;
    this.responseBuffer.append(chunk);
    while (connection === this.request && !this.stopped) {
      try {
        const message = this.responseBuffer.readMessage();
        if (message === null) return;
        this.handleBackendMessage(message);
      } catch (error) {
        this.connectionLost(connection, asError(error));
        return;
      }
    }
  }

  private handleBackendMessage(message: JSONRPCMessage): void {
    if (isResponse(message) && this.internalInitializeId !== undefined && message.id === this.internalInitializeId) {
      if ("error" in message) {
        this.connectionLost(this.request, new Error(`HTTP backend rejected restored MCP initialization: ${message.error.message}`));
        return;
      }
      this.internalInitializeId = undefined;
      void this.finishProtocolRestore().catch((error) => this.connectionLost(this.request, asError(error)));
      return;
    }

    if (isResponse(message)) {
      this.outstanding.delete(requestKey(message.id));
      if (
        this.initializeRequest &&
        isRequest(this.initializeRequest) &&
        message.id === this.initializeRequest.id &&
        "result" in message
      ) {
        this.initializeResponseSeen = true;
      }
    }
    process.stdout.write(serializeMessage(message));
  }

  private async finishProtocolRestore(): Promise<void> {
    if (!this.request) return;
    const queuedInitialization = this.queue.some(
      (message) => isNotification(message) && message.method === "notifications/initialized"
    );
    if (this.initializedNotification && !queuedInitialization) {
      await this.writeMessage(this.initializedNotification, false);
    }
    this.protocolReady = true;
    await this.flushQueue();
  }

  private async flushQueue(): Promise<void> {
    if (this.flushing || !this.protocolReady || !this.request || this.stopped) return;
    this.flushing = true;
    try {
      while (this.protocolReady && this.request && this.queue.length > 0 && !this.stopped) {
        const message = this.queue.shift() as JSONRPCMessage;
        await this.writeMessage(message, true);
      }
    } catch (error) {
      this.connectionLost(this.request, asError(error));
    } finally {
      this.flushing = false;
    }
  }

  private async writeMessage(message: JSONRPCMessage, trackRequest: boolean): Promise<void> {
    const request = this.request;
    if (!request) throw new Error("HTTP backend is not connected");
    if (trackRequest && isRequest(message)) this.outstanding.set(requestKey(message.id), message);
    const serialized = serializeMessage(message);
    if (request.write(serialized)) return;
    await new Promise<void>((resolve, reject) => {
      const onDrain = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error("HTTP backend disconnected while sending an MCP message"));
      };
      const cleanup = () => {
        request.off("drain", onDrain);
        request.off("close", onClose);
      };
      request.once("drain", onDrain);
      request.once("close", onClose);
    });
  }

  private connectionLost(connection: ClientRequest | undefined, error?: Error): void {
    if (!connection || connection !== this.request || this.stopped) return;
    this.request = undefined;
    this.response = undefined;
    this.protocolReady = false;
    this.internalInitializeId = undefined;
    this.responseBuffer.clear();
    connection.destroy();

    let initializeToRetry: JSONRPCMessage | undefined;
    for (const message of this.outstanding.values()) {
      if (isRequest(message) && message.method === "initialize" && !this.initializeResponseSeen) {
        initializeToRetry = message;
        continue;
      }
      if (isRequest(message)) this.writeConnectionLostError(message.id);
    }
    this.outstanding.clear();
    if (initializeToRetry && !this.queue.some((message) => isSameRequest(message, initializeToRetry))) {
      this.queue.unshift(initializeToRetry);
    }
    if (error) console.error(`Codex Session backend connection lost: ${error.message}`);
    this.scheduleReconnect();
  }

  private writeConnectionLostError(id: RequestId): void {
    process.stdout.write(
      serializeMessage({
        jsonrpc: "2.0",
        id,
        error: {
          code: -32000,
          message: "Codex Session backend connection was lost; retry the request."
        }
      })
    );
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, RECONNECT_DELAY_MS);
  }

  private fail(error: Error): void {
    if (this.stopped) return;
    this.stopped = true;
    this.cleanup();
    this.rejectDone(error);
  }

  private stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.cleanup();
    this.resolveDone();
  }

  private cleanup(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    const request = this.request;
    const response = this.response;
    this.request = undefined;
    this.response = undefined;
    if (request && !request.destroyed) request.end();
    if (response && !response.destroyed) response.destroy();
    this.stdinBuffer.clear();
    this.responseBuffer.clear();
    process.stdin.off("data", this.handleStdinData);
    process.stdin.off("error", this.handleStdinError);
    process.stdin.off("end", this.handleStdinClose);
    process.stdin.off("close", this.handleStdinClose);
    process.off("SIGINT", this.handleSignal);
    process.off("SIGTERM", this.handleSignal);
  }
}

async function ensureBackend(port: number, startupTimeoutMs: number, instanceId: string): Promise<void> {
  const initial = await probeHealth(port, instanceId);
  if (initial.kind === "ready") return;
  if (initial.kind === "incompatible") throw incompatibleError(initial.description);

  spawnBackend();
  const deadline = Date.now() + startupTimeoutMs;
  while (Date.now() < deadline) {
    await delay(100);
    const probe = await probeHealth(port, instanceId);
    if (probe.kind === "ready") return;
    if (probe.kind === "incompatible") throw incompatibleError(probe.description);
  }
  throw new Error(`Codex Session HTTP backend did not become ready on ${BRIDGE_HOST}:${port}`);
}

function spawnBackend(): void {
  const backendPath = fileURLToPath(new URL("./backend.js", import.meta.url));
  const child = spawn(process.execPath, [backendPath], {
    cwd: process.cwd(),
    env: process.env,
    detached: true,
    stdio: "ignore",
    windowsHide: true
  });
  child.unref();
}

function probeHealth(port: number, instanceId: string): Promise<HealthProbe> {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (probe: HealthProbe) => {
      if (settled) return;
      settled = true;
      resolve(probe);
    };
    const request = http.request(
      {
        host: BRIDGE_HOST,
        port,
        path: "/health",
        method: "GET",
        agent: false
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("end", () => {
          const value = parseObject(Buffer.concat(chunks).toString("utf8"));
          if (
            response.statusCode === 200 &&
            value.service === BRIDGE_SERVICE &&
            value.bridge_protocol === BRIDGE_PROTOCOL_VERSION &&
            value.plugin_version === PLUGIN_VERSION &&
            value.instance_id === instanceId &&
            typeof value.pid === "number"
          ) {
            finish({ kind: "ready", pid: value.pid });
            return;
          }
          if (response.statusCode === 503 && value.service === BRIDGE_SERVICE) {
            finish({ kind: "starting" });
            return;
          }
          finish({
            kind: "incompatible",
            description: `port ${port} is owned by ${String(value.service ?? "an unknown service")} with bridge ${String(value.bridge_protocol ?? "unknown")}, plugin ${String(value.plugin_version ?? "unknown")}, and instance ${String(value.instance_id ?? "unknown")}`
          });
        });
      }
    );
    request.setTimeout(HEALTH_REQUEST_TIMEOUT_MS, () => request.destroy());
    request.once("error", () => finish({ kind: "unavailable" }));
    request.end();
  });
}

function openBridge(port: number, instanceId: string): Promise<BridgeConnection> {
  return new Promise((resolve, reject) => {
    const request = http.request(
      {
        host: BRIDGE_HOST,
        port,
        path: "/bridge",
        method: "POST",
        headers: {
          "Content-Type": "application/x-ndjson",
          [BRIDGE_SERVICE_HEADER]: BRIDGE_SERVICE,
          [BRIDGE_PROTOCOL_HEADER]: BRIDGE_PROTOCOL_VERSION,
          [BRIDGE_INSTANCE_HEADER]: instanceId
        }
      },
      (response) => {
        if (response.statusCode === 200) {
          request.off("error", onError);
          resolve({ request, response });
          return;
        }
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("end", () => {
          request.destroy();
          reject(new Error(`Codex Session HTTP bridge rejected the connection: ${Buffer.concat(chunks).toString("utf8")}`));
        });
      }
    );
    const onError = (error: Error) => reject(error);
    request.once("error", onError);
    request.once("socket", (socket) => socket.setKeepAlive(true, 10_000));
    request.flushHeaders();
  });
}

function isRequest(message: JSONRPCMessage): message is JSONRPCMessage & { id: RequestId; method: string } {
  return "id" in message && "method" in message;
}

function isNotification(message: JSONRPCMessage): message is JSONRPCMessage & { method: string } {
  return !("id" in message) && "method" in message;
}

function isResponse(message: JSONRPCMessage): message is JSONRPCMessage & { id: RequestId } {
  return "id" in message && !("method" in message);
}

function isSameRequest(left: JSONRPCMessage, right: JSONRPCMessage): boolean {
  return isRequest(left) && isRequest(right) && left.id === right.id;
}

function requestKey(id: RequestId): string {
  return `${typeof id}:${String(id)}`;
}

function parseObject(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function incompatibleError(description: string): Error {
  const error = new Error(`Codex Session HTTP backend is incompatible: ${description}`);
  error.name = "IncompatibleBridgeError";
  return error;
}

function isIncompatible(error: unknown): boolean {
  return error instanceof Error && error.name === "IncompatibleBridgeError";
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
