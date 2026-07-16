import { parentPort, workerData } from "node:worker_threads";
import { openDatabase } from "./db.js";
import { CodexSessionIndexer } from "./indexer.js";
import { resolveRuntimePaths } from "./paths.js";

interface SyncWorkerData {
  codexHome: string;
  indexDbPath: string;
  holderId: string;
  options?: {
    rebuild?: boolean;
    force?: boolean;
  };
}

async function main(): Promise<void> {
  if (!parentPort) throw new Error("sync worker requires a parent port");
  const data = workerData as SyncWorkerData;
  const paths = resolveRuntimePaths({ codexHome: data.codexHome, indexDbPath: data.indexDbPath });
  const db = openDatabase(paths.indexDbPath);
  const indexer = new CodexSessionIndexer(db, paths, { holderId: data.holderId, runSyncInProcess: true });

  try {
    const result = await indexer.sync(data.options ?? {});
    parentPort.postMessage({ ok: true, result });
  } catch (error) {
    parentPort.postMessage({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  } finally {
    db.close();
  }
}

void main();
