import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { parseSessionFile } from "../src/parser.js";
import { ensureDir } from "../src/util.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(here, "../..");
const samplePath = path.join(projectRoot, "samples", "current-session.redacted.jsonl");
if (!fs.existsSync(samplePath)) {
  throw new Error("Missing samples/current-session.redacted.jsonl. The committed synthetic fixture is required for simulate:mcp.");
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "codex-session-mcp-sim-"));
const codexHome = path.join(tmpRoot, ".codex");
const activeDir = path.join(codexHome, "sessions", "2026", "06", "07");
ensureDir(activeDir);
ensureDir(path.join(codexHome, "archived_sessions"));
const copiedSample = path.join(activeDir, "rollout-simulated-current.jsonl");
fs.copyFileSync(samplePath, copiedSample);
const bridgePort = await availablePort();

const parsed = parseSessionFile(samplePath);
fs.writeFileSync(
  path.join(codexHome, "session_index.jsonl"),
  `${JSON.stringify({ id: parsed.meta.id, thread_name: "redacted current session", updated_at: new Date().toISOString() })}\n`,
  "utf8"
);

const client = new Client({ name: "codex-session-mcp-simulator", version: "0.1.0" });
const transport = new StdioClientTransport({
  command: process.execPath,
  args: [path.join(projectRoot, "dist", "src", "server.js")],
  cwd: projectRoot,
  env: {
    ...process.env,
    CODEX_HOME: codexHome,
    CODEX_SESSION_MCP_DB: path.join(tmpRoot, "index.sqlite"),
    CODEX_SESSION_MCP_PORT: String(bridgePort),
    CODEX_SESSION_MCP_IDLE_TIMEOUT_MS: "250"
  },
  stderr: "pipe"
});

await client.connect(transport);
const tools = await client.listTools();
const toolNames = tools.tools.map((tool) => tool.name).sort();

await call("codex_session_sync", { rebuild: true });
const list = await call("codex_session_list", { archive_scope: "active", limit: 5 });
const sessionId = list.data.sessions[0].id;
const snippet = findDistinctiveSnippet(parsed);
const located = await call("codex_session_find_by_text", { text: snippet, archive_scope: "active" });
const recent = await call("codex_session_recent_user_inputs", { session_id: sessionId, limit: 3 });
const calls = await call("codex_session_tool_calls", { session_id: sessionId, limit: 3 });
const search = await call("codex_session_keyword_search", {
  session_id: sessionId,
  keywords: ["记住", "工具", "session"],
  limit: 10
});
const around = await call("codex_session_messages", {
  session_id: sessionId,
  around_sequence: located.data.message.sequence,
  before_count: 2,
  after_count: 2,
  order: "asc"
});
const backend = await fetch(`http://127.0.0.1:${bridgePort}/health`).then((response) => response.json()) as { pid: number };

await client.close();
await waitForProcessExit(backend.pid, 3_000);
fs.rmSync(tmpRoot, { recursive: true, force: true });

console.log(
  JSON.stringify(
    {
      tools: toolNames,
      session_id: sessionId,
      find_by_text_status: located.status,
      recent_user_inputs: recent.data.inputs.length,
      tool_calls: calls.data.tool_calls.length,
      keyword_results: search.data.results.length,
      around_messages: around.data.messages.length
    },
    null,
    2
  )
);

async function call(name: string, args: Record<string, unknown>): Promise<any> {
  const result = await client.callTool({ name, arguments: args });
  const content = result.content as Array<{ type: string; text?: string }>;
  const first = content[0];
  if (first.type !== "text") throw new Error(`Unexpected non-text tool result from ${name}`);
  return JSON.parse(first.text ?? "{}");
}

function findDistinctiveSnippet(parsedSession: ReturnType<typeof parseSessionFile>): string {
  const candidate = parsedSession.messages
    .map((message) => message.contentText)
    .find((text) => text.includes("PLEASE IMPLEMENT THIS PLAN"));
  if (candidate) return "PLEASE IMPLEMENT THIS PLAN";
  const fallback = parsedSession.messages.find((message) => message.contentText.trim().length >= 40)?.contentText.trim();
  if (!fallback) throw new Error("No distinctive message text found in sample.");
  return fallback.slice(0, 160);
}

async function availablePort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Could not allocate a local bridge port.");
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return address.port;
}

async function waitForProcessExit(pid: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      process.kill(pid, 0);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("Simulated backend process did not exit after its idle timeout.");
}
