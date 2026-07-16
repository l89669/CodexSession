import { randomUUID } from "node:crypto";
import { Db } from "./db.js";
import { nowIso } from "./util.js";

export interface PublishedAgentTask {
  token: string;
  task: string;
  retrievalCount: number;
}

export class AgentTaskStore {
  constructor(private readonly db: Db) {}

  publish(task: string): { token: string } {
    if (task.trim().length === 0) {
      throw new Error("task must contain a task description");
    }

    const token = randomUUID();
    this.db.prepare("INSERT INTO agent_tasks (token, task, created_at) VALUES (?, ?, ?)").run(token, task, nowIso());
    return { token };
  }

  get(token: string): PublishedAgentTask | undefined {
    const normalizedToken = token.trim().toLowerCase();
    const row = this.db
      .prepare(
        `UPDATE agent_tasks
         SET retrieval_count = retrieval_count + 1
         WHERE token = ?
         RETURNING task, retrieval_count`
      )
      .get(normalizedToken) as { task: string; retrieval_count: number } | undefined;
    if (!row) return undefined;
    return { token: normalizedToken, task: row.task, retrievalCount: row.retrieval_count };
  }
}
