import type { IncomingMessage, ServerResponse } from "node:http";
import type { Transport, TransportSendOptions } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage, MessageExtraInfo } from "@modelcontextprotocol/sdk/types.js";
import { ReadBuffer, serializeMessage } from "@modelcontextprotocol/sdk/shared/stdio.js";

export class HttpBridgeServerTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: <T extends JSONRPCMessage>(message: T, extra?: MessageExtraInfo) => void;

  private readonly readBuffer = new ReadBuffer();
  private started = false;
  private closed = false;

  constructor(
    private readonly request: IncomingMessage,
    private readonly response: ServerResponse,
    private readonly closedCallback: () => void
  ) {}

  async start(): Promise<void> {
    if (this.started) throw new Error("HTTP bridge transport has already started");
    this.started = true;
    this.request.socket.setKeepAlive(true, 10_000);
    this.request.on("data", this.handleData);
    this.request.once("end", this.handleClose);
    this.request.once("aborted", this.handleClose);
    this.request.once("error", this.handleError);
    this.response.once("close", this.handleClose);
    this.response.once("error", this.handleError);
  }

  async send(message: JSONRPCMessage, _options?: TransportSendOptions): Promise<void> {
    if (this.closed) throw new Error("HTTP bridge transport is closed");
    const serialized = serializeMessage(message);
    if (this.response.write(serialized)) return;
    await new Promise<void>((resolve, reject) => {
      const onDrain = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error("HTTP bridge closed while sending an MCP message"));
      };
      const cleanup = () => {
        this.response.off("drain", onDrain);
        this.response.off("close", onClose);
      };
      this.response.once("drain", onDrain);
      this.response.once("close", onClose);
    });
  }

  async close(): Promise<void> {
    this.finish();
  }

  private readonly handleData = (chunk: Buffer) => {
    this.readBuffer.append(chunk);
    while (!this.closed) {
      try {
        const message = this.readBuffer.readMessage();
        if (message === null) return;
        this.onmessage?.(message);
      } catch (error) {
        this.onerror?.(asError(error));
      }
    }
  };

  private readonly handleError = (error: Error) => {
    this.onerror?.(error);
    this.finish();
  };

  private readonly handleClose = () => {
    this.finish();
  };

  private finish(): void {
    if (this.closed) return;
    this.closed = true;
    this.request.off("data", this.handleData);
    this.readBuffer.clear();
    if (!this.response.writableEnded) this.response.end();
    this.closedCallback();
    this.onclose?.();
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
