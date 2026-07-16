import crypto from "node:crypto";
import { Db } from "./db.js";

const LEASE_KEY = "codex-session-mcp";

export class LeaderLease {
  readonly holderId: string;
  private readonly leaseMs: number;
  private readonly db: Db;

  constructor(db: Db, options: { leaseMs?: number; holderId?: string } = {}) {
    this.db = db;
    this.leaseMs = options.leaseMs ?? 30_000;
    this.holderId = options.holderId ?? `${process.pid}-${crypto.randomUUID()}`;
  }

  acquireOrRenew(): boolean {
    const now = Date.now();
    const existing = this.db
      .prepare("SELECT holder_id, renewed_at_ms, lease_ms, generation FROM leader_lease WHERE singleton_key = ?")
      .get(LEASE_KEY) as { holder_id: string; renewed_at_ms: number; lease_ms: number; generation: number } | undefined;

    if (!existing) {
      this.db
        .prepare("INSERT INTO leader_lease (singleton_key, holder_id, renewed_at_ms, lease_ms, generation) VALUES (?, ?, ?, ?, 1)")
        .run(LEASE_KEY, this.holderId, now, this.leaseMs);
      return true;
    }

    const expired = now - existing.renewed_at_ms > existing.lease_ms;
    if (existing.holder_id === this.holderId || expired) {
      const result = this.db
        .prepare(
          `UPDATE leader_lease
           SET holder_id = ?, renewed_at_ms = ?, lease_ms = ?, generation = generation + 1
           WHERE singleton_key = ? AND (holder_id = ? OR renewed_at_ms + lease_ms < ?)`
        )
        .run(this.holderId, now, this.leaseMs, LEASE_KEY, existing.holder_id, now);
      return result.changes > 0;
    }

    return false;
  }

  isLeader(): boolean {
    const existing = this.db
      .prepare("SELECT holder_id, renewed_at_ms, lease_ms FROM leader_lease WHERE singleton_key = ?")
      .get(LEASE_KEY) as { holder_id: string; renewed_at_ms: number; lease_ms: number } | undefined;
    if (!existing) return false;
    return existing.holder_id === this.holderId && Date.now() - existing.renewed_at_ms <= existing.lease_ms;
  }

  current(): Record<string, unknown> | undefined {
    return this.db.prepare("SELECT * FROM leader_lease WHERE singleton_key = ?").get(LEASE_KEY) as Record<string, unknown> | undefined;
  }

  release(): void {
    this.db.prepare("DELETE FROM leader_lease WHERE singleton_key = ? AND holder_id = ?").run(LEASE_KEY, this.holderId);
  }
}
