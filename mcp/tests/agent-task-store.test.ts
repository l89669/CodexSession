import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AgentTaskStore } from "../src/agent-task-store.js";
import { openDatabase } from "../src/db.js";

test("agent tasks are exact, repeatable, and immediately readable from another connection", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-session-agent-task-test-"));
  const dbPath = path.join(root, "index.sqlite");
  const publisherDb = openDatabase(dbPath);
  const receiverDb = openDatabase(dbPath);

  try {
    const publisher = new AgentTaskStore(publisherDb);
    const receiver = new AgentTaskStore(receiverDb);
    const task = "  检查父任务。\nPreserve <xml> and **Markdown** exactly.  ";
    const { token } = publisher.publish(task);

    assert.deepEqual(receiver.get(token), { token, task, retrievalCount: 1 });
    assert.deepEqual(receiver.get(token), { token, task, retrievalCount: 2 });
    assert.equal(receiver.get("00000000-0000-4000-8000-000000000000"), undefined);
  } finally {
    publisherDb.close();
    receiverDb.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
