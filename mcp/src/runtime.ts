import { AgentTaskStore } from "./agent-task-store.js";
import { Db, openDatabase } from "./db.js";
import { CodexSessionIndexer } from "./indexer.js";
import { resolveRuntimePaths, RuntimePaths } from "./paths.js";
import { CodexSessionQueries } from "./query.js";

export interface SessionRuntime {
  readonly db: Db;
  readonly paths: RuntimePaths;
  readonly indexer: CodexSessionIndexer;
  readonly queries: CodexSessionQueries;
  readonly agentTasks: AgentTaskStore;
  close(): Promise<void>;
}

export function createSessionRuntime(
  options: { codexHome?: string; indexDbPath?: string; busyTimeoutMs?: number } = {}
): SessionRuntime {
  const paths = resolveRuntimePaths(options);
  const db = openDatabase(paths.indexDbPath, { busyTimeoutMs: options.busyTimeoutMs });
  const indexer = new CodexSessionIndexer(db, paths);
  const queries = new CodexSessionQueries({ db, indexer });
  const agentTasks = new AgentTaskStore(db);
  let closed = false;

  indexer.start();
  return {
    db,
    paths,
    indexer,
    queries,
    agentTasks,
    async close() {
      if (closed) return;
      closed = true;
      await indexer.stop();
      db.close();
    }
  };
}
