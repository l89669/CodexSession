#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { AgentTaskStore } from "./agent-task-store.js";
import { openDatabase } from "./db.js";
import { CodexSessionIndexer } from "./indexer.js";
import { resolveRuntimePaths } from "./paths.js";
import { CodexSessionQueries } from "./query.js";

const archiveScopeSchema = z.enum(["active", "archived", "both"]).default("active");
const orderSchema = z.enum(["asc", "desc"]).default("desc");
const roleSchema = z.enum(["user", "assistant", "developer", "system", "tool", "unknown"]);
const searchScopeSchema = z.enum(["messages", "tool_calls", "tool_outputs", "raw_events", "all"]).default("messages");
const repeatedTaskRetrievalComment =
  "如果你是在恢复上下文，就继续调用 codex_session_recent_user_inputs，并按需调用 codex_session_messages。";
const publishedTaskComment =
  "For every later instruction update to the agent, use the same handoff: call codex_session_publish_task, then send the returned prompt verbatim.";

function taskHandoffPrompt(token: string): string {
  return `token: ${token}, use codex_session_get_task tool to retrieve exact instruction`;
}

export async function createServer(options: { codexHome?: string; indexDbPath?: string; busyTimeoutMs?: number } = {}): Promise<{
  server: McpServer;
  indexer: CodexSessionIndexer;
}> {
  const paths = resolveRuntimePaths(options);
  const db = openDatabase(paths.indexDbPath, { busyTimeoutMs: options.busyTimeoutMs });
  const indexer = new CodexSessionIndexer(db, paths);
  const queries = new CodexSessionQueries({ db, indexer });
  const agentTasks = new AgentTaskStore(db);

  const server = new McpServer({
    name: "codex-session-mcp",
    version: "0.1.0"
  });

  registerJsonTool(server, "codex_session_status", {
    title: "Codex Session Index Status",
    description: "Return indexing, writer lease, and database status for the local Codex session index.",
    inputSchema: {}
  }, async () => queries.status());

  registerJsonTool(server, "codex_session_sync", {
    title: "Sync Codex Session Index",
    description: "Run an index sync. By default this is incremental; set rebuild=true to clear and rebuild the mirror from disk.",
    inputSchema: {
      rebuild: z.boolean().optional().describe("Clear indexed session data and rebuild from source JSONL files.")
    }
  }, async (args) => queries.sync(args));

  registerJsonTool(server, "codex_session_list", {
    title: "List Codex Sessions",
    description: "List indexed Codex sessions by time range. Use this when no distinctive original text is available to locate a session id.",
    inputSchema: {
      archive_scope: archiveScopeSchema.describe("active, archived, or both. Defaults to active."),
      updated_from: z.string().optional().describe("ISO time. If no timezone is present, it is interpreted in the MCP process local timezone."),
      updated_to: z.string().optional().describe("ISO time. If no timezone is present, it is interpreted in the MCP process local timezone."),
      limit: z.number().int().positive().max(500).optional().describe("Maximum sessions to return. Defaults to 50."),
      order: orderSchema.describe("Sort order by updated_at. Defaults to desc.")
    }
  }, async (args) => queries.listSessions(args));

  registerJsonTool(server, "codex_session_get_session_token", {
    title: "Get Codex Session Locator Token",
    description:
      "Return a fresh UUID marker that will be written into the current Codex session JSONL through this tool output. Call codex_session_get_session_by_token with the returned token to locate the current session.",
    inputSchema: {}
  }, async () => queries.getSessionToken());

  registerJsonTool(server, "codex_session_publish_task", {
    title: "Publish Codex Agent Task",
    description:
      "Persist an exact instruction locally and return a ready-to-send handoff prompt. Send data.prompt verbatim for both initial tasks and later instruction updates.",
    inputSchema: {
      task: z.string().min(1).refine((value) => value.trim().length > 0, "Task must contain a task description.")
        .describe("Complete task description to preserve exactly for the receiving agent.")
    }
  }, async (args) => {
    const { token } = agentTasks.publish(args.task);
    return {
      status: "ok",
      data: {
        token,
        prompt: taskHandoffPrompt(token),
        comment: publishedTaskComment
      }
    };
  });

  registerJsonTool(server, "codex_session_get_task", {
    title: "Get Published Codex Agent Task",
    description: "Retrieve the exact task description stored by codex_session_publish_task using its UUID token.",
    inputSchema: {
      token: z.string().uuid().describe("UUID returned by codex_session_publish_task.")
    }
  }, async (args) => {
    const task = agentTasks.get(args.token);
    if (!task) {
      return { status: "not_found", error: "no published task exists for this token", data: { token: args.token } };
    }
    const data: { token: string; task: string; comment?: string } = { token: task.token, task: task.task };
    if (task.retrievalCount >= 2) data.comment = repeatedTaskRetrievalComment;
    return { status: "ok", data };
  });

  registerJsonTool(server, "codex_session_get_session_by_token", {
    title: "Locate Codex Session By Token",
    description:
      "Locate the current Codex session by a UUID returned from codex_session_get_session_token. This triggers an incremental sync and searches the dedicated token index built from locator tool calls.",
    inputSchema: {
      token: z.string().uuid().describe("UUID returned by codex_session_get_session_token.")
    }
  }, async (args) => queries.getSessionByToken(args));

  registerJsonTool(server, "codex_session_find_by_text", {
    title: "Find Codex Session By Original Text",
    description:
      "Deprecated fallback: locate a unique Codex session/message by a distinctive original text snippet. Prefer codex_session_get_session_token followed by codex_session_get_session_by_token for current-session recovery.",
    inputSchema: {
      text: z.string().min(1).describe("Original transcript text to locate. Fewer than 8 effective characters is rejected."),
      archive_scope: archiveScopeSchema.describe("active, archived, or both. Defaults to active."),
      include_candidates: z.boolean().optional().describe("When ambiguous, include diagnostic candidates. Defaults to false."),
      max_chars: z.number().int().positive().max(100000).optional().describe("Maximum snippet characters.")
    }
  }, async (args) => queries.findByText(args));

  registerJsonTool(server, "codex_session_messages", {
    title: "Query Codex Session Messages",
    description:
      "Query messages inside one session by role, time range, sequence range, or around_sequence. session_id is required.",
    inputSchema: {
      session_id: z.string().min(1),
      roles: z.array(roleSchema).optional(),
      time_from: z.string().optional(),
      time_to: z.string().optional(),
      index_from: z.number().int().optional(),
      index_to: z.number().int().optional(),
      around_sequence: z.number().int().optional(),
      before_count: z.number().int().positive().max(100).optional(),
      after_count: z.number().int().positive().max(100).optional(),
      limit: z.number().int().positive().max(500).optional(),
      order: orderSchema,
      include_raw: z.boolean().optional(),
      max_chars: z.number().int().positive().max(100000).optional()
    }
  }, async (args) => queries.messages(args));

  registerJsonTool(server, "codex_session_recent_user_inputs", {
    title: "Recent Codex Session Inputs",
    description:
      "Return the most recent effective inputs in a session: ordinary user messages and successful codex_session_get_task retrievals, each with an explicit input_type. Defaults to the last 3 inputs.",
    inputSchema: {
      session_id: z.string().min(1),
      limit: z.number().int().positive().max(100).optional(),
      include_raw: z.boolean().optional(),
      max_chars: z.number().int().positive().max(100000).optional()
        .describe("Maximum characters returned for each user message or published task text.")
    }
  }, async (args) => queries.recentUserInputs(args));

  registerJsonTool(server, "codex_session_tool_calls", {
    title: "Query Codex Session Tool Calls",
    description:
      "Query tool calls in a session by tool name, time, output presence, or literal keyword. Output is summarized by default.",
    inputSchema: {
      session_id: z.string().min(1),
      tool_name_contains: z.string().optional(),
      time_from: z.string().optional(),
      time_to: z.string().optional(),
      has_output: z.boolean().optional(),
      keyword: z.string().optional(),
      limit: z.number().int().positive().max(500).optional(),
      order: orderSchema,
      include_raw: z.boolean().optional(),
      max_output_chars: z.number().int().positive().max(100000).optional()
    }
  }, async (args) => queries.toolCalls(args));

  registerJsonTool(server, "codex_session_keyword_search", {
    title: "Literal Keyword Search In Codex Session",
    description:
      "Search one session using exact literal substring matching. Supports multiple keywords; default match is any and each result includes matched_keywords.",
    inputSchema: {
      session_id: z.string().min(1),
      query: z.string().optional().describe("Single literal keyword. Combined with keywords if both are provided."),
      keywords: z.array(z.string()).optional().describe("Literal keywords. Results are merged, not grouped."),
      match: z.enum(["any", "all"]).default("any"),
      scope: searchScopeSchema,
      roles: z.array(roleSchema).optional(),
      time_from: z.string().optional(),
      time_to: z.string().optional(),
      limit: z.number().int().positive().max(500).optional(),
      order: orderSchema,
      include_raw: z.boolean().optional(),
      max_chars: z.number().int().positive().max(100000).optional()
    }
  }, async (args) => queries.keywordSearch(args));

  indexer.start();
  return { server, indexer };
}

function registerJsonTool(
  server: McpServer,
  name: string,
  config: {
    title: string;
    description: string;
    inputSchema: z.ZodRawShape;
  },
  handler: (args: any) => Promise<Record<string, unknown>>
): void {
  server.registerTool(name, config, async (args) => {
    try {
      const result = await handler(args);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } catch (error) {
      return {
        isError: true,
        content: [
          {
            type: "text",
            text: JSON.stringify(
              {
                status: "error",
                error: error instanceof Error ? error.message : String(error)
              },
              null,
              2
            )
          }
        ]
      };
    }
  });
}

async function main(): Promise<void> {
  const { server, indexer } = await createServer({
    codexHome: process.env.CODEX_HOME,
    indexDbPath: process.env.CODEX_SESSION_MCP_DB,
    busyTimeoutMs: parseBusyTimeout(process.env.CODEX_SESSION_MCP_BUSY_TIMEOUT_MS)
  });
  const transport = new StdioServerTransport();
  let shuttingDown = false;
  const shutdown = (code: number) => {
    if (shuttingDown) return;
    shuttingDown = true;
    void indexer.stop().finally(() => process.exit(code));
  };
  transport.onclose = () => shutdown(0);
  process.stdin.on("end", () => shutdown(0));
  process.stdin.on("close", () => shutdown(0));
  process.on("SIGINT", () => shutdown(0));
  process.on("SIGTERM", () => shutdown(0));
  await server.connect(transport);
}

function parseBusyTimeout(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  void main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
