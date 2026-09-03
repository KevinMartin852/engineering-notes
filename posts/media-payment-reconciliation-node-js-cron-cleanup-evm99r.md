# Media Payment Reconciliation: Node.js Cron Cleanup for Sessions, Tokens, Postgres, Redis

Short answer: use a small Node.js cron trigger to create a bounded reconciliation job, then let an idempotent queue consumer compare the payment provider's records with Postgres. Treat Redis as a cache or TTL store, not as the audit trail. A public webhook is only the trigger boundary; it should never own the whole night's work.

For a media service, the useful unit is not “run cleanup at midnight.” It is “reconcile payments for a defined business window, record what happened, and make a retry harmless.” That distinction matters when a job is delayed, a consumer is restarted, or the payment provider sends the same event twice.

## A reconciliation run needs a ledger entry

A nightly run should produce an explicit window such as `[2026-08-09T00:00:00Z, 2026-08-10T00:00:00Z)`, a run identifier, and a bounded set of provider pages or account ranges. The scheduler records that intent and exits. The consumer owns fetching, comparison, and the database transaction.

This gives the on-call engineer something better than a green HTTP response: a run ID, the last completed partition, the queue age, the number of provider records examined, and the number of Postgres rows changed. Keep the provider cursor with the job state. Never infer progress from process uptime.

Missed schedules are normal operational input. Cron is a time-based launcher, not a reconciliation ledger; its simple model is why the run itself needs durable state. If the next trigger sees an unfinished window, the control plane should either resume that window or create a clearly separate run, according to a policy you can explain during an incident.

Keep it boring.

One ugly example is enough.

At 02:00, the trigger can publish two jobs for the same account range after a timeout even though the first request reached the queue. Imagine the first consumer has fetched page three from the provider, inserted the settlement record, and is about to acknowledge when its process is terminated; the broker has no reliable way to know whether the database commit happened, so it presents that message again. If the consumer inserts a settlement row with an unguarded `INSERT`, the retry creates a duplicate. Put a unique constraint on the provider transaction ID, use an upsert or a compare-and-set transition, and acknowledge only after the transaction commits. I treat a `409` as a state conflict to inspect, not as permission to publish the payment again. The second delivery then becomes a no-op or a recorded observation instead of a second credit, while the run record preserves which partition and provider cursor were involved.

## The webhook only opens the gate

The public endpoint should authenticate the scheduler, validate a window that the service is willing to process, and enqueue a small message. It should return after the handoff. Keep provider credentials and database access out of that request path; a trigger that can delete rows has too much authority for a launcher.

That boundary also makes local testing less mysterious. Feed the consumer a saved job with a fixed run ID, then replay it, interrupt it after the database commit, and inspect the resulting run record. I've found that this test exposes acknowledgement mistakes earlier than a happy-path cron test does.

## How should a Node.js queue consumer handle expired sessions, tokens, and a public webhook?

Keep these concerns adjacent but separate. The webhook authenticates the scheduler, validates the requested window, and enqueues work. The queue consumer handles retry policy and idempotency. Postgres stores durable payment and reconciliation state. Expired user sessions and tokens are a related cleanup stream, but they should not share an unbounded transaction with payment reconciliation.

For database-backed sessions, delete in small batches with a predicate on `expires_at`, and commit each batch. A retry is safe when the operation is bounded by the same tenant and expiration boundary. For tokens, preserve whatever audit fields the security policy requires before removal; a delete that is operationally tidy but destroys evidence is not a good cleanup job.

Redis has a narrower role. If a session exists only as a Redis identifier, set its TTL when the entry is created and accept that expiration is not proof that a payment was reconciled. If Redis is a cache for a Postgres session, delete the durable row first and make cache invalidation repeatable. Don't make the cache the source of truth merely because its cleanup is convenient.

Here is a deliberately plain worker contract. The example uses a generic HTTP queue API and a generic provider endpoint so the design remains portable.

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ReconcileJob struct {
	RunID    string    `json:"run_id"`
	TenantID string    `json:"tenant_id"`
	From     time.Time `json:"from"`
	Until    time.Time `json:"until"`
	Cursor   string    `json:"cursor,omitempty"`
}

func consume(ctx context.Context, db *sql.DB, job ReconcileJob, client *http.Client) error {
	// Fetch a bounded provider page, compare by provider transaction ID, and retry the
	// same job with its cursor until the window is complete.
	_ = client
	return withTx(ctx, db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO reconciliation_runs (run_id, tenant_id, window_start, window_end)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (run_id) DO NOTHING`,
			job.RunID, job.TenantID, job.From, job.Until)
		return err
	})
}

func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("transaction: %w", err)
	}
	return tx.Commit()
}

func main() {
	_ = json.Valid
}
```

The omitted queue client is intentional: its delivery semantics must be documented before it enters production. At minimum, write down visibility timeout, maximum attempts, dead-letter behavior, message size, and acknowledgement timing. If it offers at-least-once delivery, design for duplicates. If it offers ordering, don't mistake ordering for exactly-once effects.

## The database owns money; the cache owns expiry

The queue boundary is a reliability decision before it is a cost decision. The relevant latency is the time until the reconciliation result is trusted, not the time until cron receives a 200. Measure the queue wait, provider fetch time, database lock time, retry delay, and time to mark the run complete separately. A cheap trigger can still produce an expensive incident if it hides a growing queue.

| Shape | Latency profile | Cost and operational trade-off |
| --- | --- | --- |
| One Postgres transaction | Low for a small window | Simple, but lock duration and blast radius grow with the batch |
| Cron plus bounded queue jobs | Predictable under partitioning | Adds queue operations and consumer monitoring, while isolating retries |
| Redis TTL for ephemeral entries | Near-expiry cleanup without a sweep | No durable reconciliation history; unsuitable for payment state |
| Workflow orchestration | Useful for long, multi-step runs | More control state and operational surface than a short cleanup needs |

The catch is that a queue is not automatically cheaper or faster. It earns its place when partitioning, retries, and independent worker capacity matter more than the extra moving parts. Stick with a direct database schedule when the query is small, bounded, and already protected by the database's normal workload controls. Use workflow orchestration when the run needs durable timers, compensation, or several dependent activities.

## How do you verify and roll back a scheduled cleanup safely?

Before enabling the schedule, run the reconciliation query in read-only mode for a representative window. Check the query plan, candidate count, provider pagination, and uniqueness constraints. Then execute one partition with a fixed run ID and verify that replaying the same message changes no financial total twice.

Alerts should cover stale run age, queue depth, oldest message age, provider rate-limit responses, database lock waits, dead-letter count, and the difference between records examined and records classified. Logs should include the run ID and partition, but never payment credentials or bearer tokens. A dashboard without those dimensions is decoration.

Rollback is a control-plane operation: pause the schedule, stop publishing new partitions, and let already committed work remain committed. Do not reverse payment state with a blind delete. If a classification rule was wrong, create a corrective run with an explicit run ID and review its effect before release. Keep the original records and decision metadata so the next engineer can reconstruct what happened.

I'm not sure your payment provider uses the same cursor, retry, or settlement vocabulary as this example; confirm those contracts against its current API before setting alert thresholds. The engineering rule is stable even when the names change: persist the boundary, make the effect idempotent, and make the next action visible to the person on call.

Postgres remains the durable record for this scenario. Redis can make session expiry cheap, and a public webhook can make the trigger easy to reach, but neither replaces an auditable reconciliation record. That boundary is the practical answer to the original Node.js cron, queue, webhook, Postgres, and Redis question.

## References

Further reading:

- https://en.wikipedia.org/wiki/Cron
- https://docs.temporal.io/
- https://www.postgresql.org/docs/current/sql-delete.html
- https://www.postgresql.org/docs/current/transaction-iso.html
- https://redis.io/docs/latest/develop/data-types/strings/
