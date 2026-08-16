import { INDEX_SCHEMA_VERSION, openDatabase } from "../src/db.js";
import { CodexSessionIndexer } from "../src/indexer.js";
import { resolveRuntimePaths } from "../src/paths.js";

const paths = resolveRuntimePaths();
const db = openDatabase(paths.indexDbPath);
const indexer = new CodexSessionIndexer(db, paths);

try {
  const schemaBefore = db.pragma("user_version", { simple: true }) as number;
  const tasksBefore = countAgentTasks();

  if (schemaBefore === INDEX_SCHEMA_VERSION) {
    console.log(JSON.stringify({ status: "already_migrated", schema_version: schemaBefore }, null, 2));
  } else {
    if (schemaBefore !== 6) {
      throw new Error(`expected index schema 6, found ${schemaBefore}`);
    }

    console.log(
      JSON.stringify(
        {
          status: "migration_started",
          database: paths.indexDbPath,
          schema_from: schemaBefore,
          schema_to: INDEX_SCHEMA_VERSION,
          agent_tasks: tasksBefore
        },
        null,
        2
      )
    );

    const result = await indexer.sync({ rebuild: true, force: true });
    const schemaAfter = db.pragma("user_version", { simple: true }) as number;
    const tasksAfter = countAgentTasks();

    if (schemaAfter !== INDEX_SCHEMA_VERSION) {
      throw new Error(`migration completed with index schema ${schemaAfter}`);
    }
    if (tasksAfter !== tasksBefore) {
      throw new Error(`agent task count changed from ${tasksBefore} to ${tasksAfter}`);
    }

    console.log(
      JSON.stringify(
        {
          status: "migration_completed",
          schema_version: schemaAfter,
          agent_tasks: tasksAfter,
          result
        },
        null,
        2
      )
    );
  }
} finally {
  db.close();
}

function countAgentTasks(): number {
  return (db.prepare("SELECT COUNT(*) AS count FROM agent_tasks").get() as { count: number }).count;
}
