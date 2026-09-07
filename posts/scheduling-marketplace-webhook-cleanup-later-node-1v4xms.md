# Scheduling Marketplace Webhook Cleanup Later: Node.js Delayed Queue Task or Cron?

Short answer: for a marketplace webhook that needs one follow-up an hour later, put one durable delayed message behind an idempotent consumer. Keep cron for recurring recovery scans, not as a separate timer for every event. The decision is about which component owns recovery after a crash.

I've been paged for missed jobs and duplicate deliveries. Those pages have the same shape: a timer fired, but nobody could say whether the business effect had happened. A scheduler can establish eligibility. It cannot prove delivery.

## Evaluate how a Node.js webhook follow-up survives a delay

The useful unit is a business intent, not a delivery attempt. For an order-cleanup webhook, create a stable identity such as `marketplace:order_8472:cleanup`, persist its due time, and return from the Node.js request after the intent has a durable home. The one-hour wait must survive a deploy, a process restart, and a worker that vanishes at the wrong moment.

The failure that matters is narrow. A worker receives the delayed message, calls the downstream cleanup endpoint, sees an accepted response, and dies before acknowledgement. The message is delivered again. If the consumer treats each delivery as a new instruction, cleanup runs twice. If it acknowledges before making the call, a crash can lose cleanup altogether.

There is no magic exactly-once boundary in that gap. I want three states visible in the data: eligible, effect recorded, and retryable failure. The idempotency key belongs to the effect, while an attempt number belongs to the delivery. Those are different facts.

A short rule helps during review: claim, effect, acknowledge.

## Implement one intent, not one delivery attempt

The request handler should validate the webhook, write the follow-up intent, and hand off a compact message containing the source event ID, due time, and idempotency key. The worker checks eligibility, claims the business intent, performs the effect, records the result, and acknowledges only after its durable state is clear. A retry must reuse the same key.

Here is the ordering I expect a consumer to make explicit. The interfaces are deliberately generic; the transaction and uniqueness rules live in the implementation behind them.

```go
package main

import (
	"context"
	"errors"
	"time"
)

type FollowUp struct {
	ID             string
	IdempotencyKey string
	DueAt          time.Time
}

type Store interface {
	Claim(ctx context.Context, key string) (alreadyDone bool, err error)
	MarkDone(ctx context.Context, key string) error
}

type Sender interface {
	Send(ctx context.Context, key string, item FollowUp) error
}

func Deliver(ctx context.Context, store Store, sender Sender, item FollowUp) error {
	if time.Now().Before(item.DueAt) {
		return errors.New("follow-up is not eligible")
	}

	done, err := store.Claim(ctx, item.IdempotencyKey)
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	if err := sender.Send(ctx, item.IdempotencyKey, item); err != nil {
		return err
	}
	return store.MarkDone(ctx, item.IdempotencyKey)
}
```

The example exposes the uncomfortable case: the send can succeed while `MarkDone` fails. The next delivery may therefore repeat the call. The destination needs to accept the same idempotency key safely, or the application needs an outbox and reconciliation protocol. I'm not sure one claim pattern fits every downstream system; your mileage may vary with its consistency and retry semantics.

When I see a `409`, I don't automatically retry it. A timeout or temporary refusal may be retryable; a malformed payload, missing order, or permanent authorization decision usually needs a recorded terminal outcome. The exact classification is part of the runbook, not a guess made by a generic retry loop.

## Govern the handoff with durable evidence

Cron is a good owner for work whose input is a query: recurring expiry scans, reconciliation, and recovery of durable rows that became due. A sweep can select a bounded batch and use PostgreSQL row-locking options such as `FOR UPDATE SKIP LOCKED` so workers do not wait on rows already being processed.

Per-event cron has a different operational burden. Every webhook creates scheduler state that must be cancelled, inspected, retained, and reconciled when an order changes. That can be a reasonable choice for a small set of named schedules that operators need to inspect directly. It is a poor default when thousands of unrelated one-hour callbacks each need the same retry and idempotency policy.

The distinction is ownership, not syntax:

| Work shape | Natural owner | Main review question |
|---|---|---|
| One follow-up for one webhook | Delayed queue message | Can the business effect safely repeat with one stable key? |
| Recurring or query-driven work | Cron plus durable rows | Can the sweep claim due rows without double processing? |
| Several timed steps with compensation | Workflow state machine | Does this callback now represent a real workflow? |
| A few named schedules for operators | Per-event scheduler | Who cancels and reconciles a changed order? |

The recommendation has a boundary. A delayed message is unsuitable when operators require a visual, long-lived schedule for each case, or when several steps must coordinate with compensation and joins. Stick with cron for recurring scans; use a workflow system when that state machine is the requirement.

## What should a Node.js webhook task measure after a delayed message?

Store `due_at`, status, idempotency key, payload reference, attempt count, and the last error in a durable system. Track the age of the oldest eligible item, retry counts by classification, claim conflicts, permanent failures, and the time from due time to completed effect. Queue depth alone can look healthy while old eligible work is quietly stuck.

The publish transaction also deserves a written failure policy. If the database write commits and message publication does not, recovery must find the row. If publication succeeds and the transaction is retried, duplicate messages must still be harmless. Document those cases in the runbook instead of hiding them behind an abstraction.

Then test the ugly transitions. Deliver the same webhook twice. Advance the clock over the one-hour boundary. Stop a worker after the downstream response but before acknowledgement. Pause the downstream service. Cancel the order before the follow-up is eligible. For each case, the expected result should be one cleanup, an explicit cancellation, or an operator-visible failure.

A green happy-path test proves very little here.

The decision rule is short: delayed messages own one-off eligibility, cron owns recurring recovery, and idempotency protects the business effect in both designs. Choose the mechanism that leaves the fewest unanswered questions after the next process crash.

## References

- [Celery introduction documentation](https://docs.celeryq.dev/en/stable/getting-started/introduction.html)
- [PostgreSQL SELECT documentation](https://www.postgresql.org/docs/current/sql-select.html)

## Further reading

- [Celery introduction documentation](https://docs.celeryq.dev/en/stable/getting-started/introduction.html)
- [PostgreSQL SELECT documentation](https://www.postgresql.org/docs/current/sql-select.html)
