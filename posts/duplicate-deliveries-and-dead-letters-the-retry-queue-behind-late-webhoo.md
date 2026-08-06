# Duplicate deliveries and dead letters: the retry queue behind late webhook callbacks

Use a durable queue with a dead-letter queue behind it when a webhook callback is ordinary retryable work — a delivery that failed, a delayed job, an event that has to land eventually. Reach for a workflow engine only when the hard part is coordinating steps, not getting one HTTP call to stick.

That's the recommendation. The rest of this is the part that decides whether it holds at 3am.

The shape is three pieces: a queue that accepts a delay at publish time, a worker that writes down every attempt before it makes one, and a dead-letter queue an operator can list and redrive. The publisher in this repo does the first piece for paid-order notifications, and the [README](../README.md) covers how it's wired. Our workers are Go, so that's what the code below is; the same calls are about twenty lines of Node.js if that's your stack.

## Duplicate deliveries hurt more than missing ones

Missed jobs page you once. Duplicates page you twice — the second time from finance.

Every hosted queue worth running is at-least-once, and so is every webhook sender you receive from. A worker that times out after the receiver already committed the charge will retry, and the receiver will see the same event again. So will yours, when a broker redelivers a message whose ack got lost on the way back. Dedup windows exist, but they're short: five minutes on a FIFO queue is typical, and a retry scheduled an hour out sails straight past it.

The one that cost me a Tuesday wasn't a queue problem at all, it was a config footgun. We'd split the notification workers across two regions, and the second region pulled its signing secret from a different store — same value, plus a trailing newline that `kubectl create secret --from-file` had faithfully preserved. Every callback that region signed came back 401 from the customer's endpoint. The worker did exactly what the runbook told it to: retried with backoff, gave up after eight attempts, parked the event. Inside 40 minutes we had 4,812 events sitting in the dead-letter queue and a dashboard that looked like a partner had gone dark. The tell was the split — failures landed almost exactly 50/50 across the two regions, and no external provider is ever that fair. I'm not sure why none of us diffed the two secrets in the first half hour. I certainly didn't.

So the consumer has to be idempotent before anything else is worth building. One unique index does most of that work:

```sql
create table webhook_delivery (
  delivery_id text        primary key,
  event_id    text        not null,
  attempt     int         not null default 0,
  state       text        not null default 'pending',
  updated_at  timestamptz not null default now()
);
```

The provider's delivery ID is the primary key, not an ID you mint yourself. That's the value that stays stable across their retries and yours, and it turns "did we already do this?" into an insert that either succeeds or conflicts.

## When should a failed webhook callback go to the dead-letter queue?

Two questions, in order: is this error worth another attempt, and have we spent the budget?

Connection resets, read timeouts, 429s and 5xx responses from the receiver are worth retrying — the request never got a verdict, or the verdict was "later". A 400, a 401, a 404 or a 410 is a verdict. Retrying those burns attempts to reach the same conclusion an hour later, and it's how a dead-letter queue fills up with noise that nobody triages.

The budget is boring on purpose. Eight attempts, exponential with jitter, covering roughly a day; the first three inside five minutes so a brief blip self-heals before anyone looks. Cap the computed delay at what your broker actually accepts — seven days is the ceiling on a lot of hosted queues — and keep the whole schedule well inside message retention, so the parked copy still exists when someone finally reads the alert.

Then stop.

A queue that retries forever is a queue with no signal in it. The dead-letter queue is an inbox for humans, and it only works if it's usually empty: alert on depth and on the age of the oldest message, not on the rate of individual failures. Retention gives you a bounded window to act — 30 days is generous — and an ack deletes the message, so a careless ack during triage is a lost event rather than one you can replay later.

## Scheduling the retry without inventing a new failure mode

Here's the publish side, trimmed to what matters. The delivery ID plus the attempt number is the idempotency key, so a publish that gets retried after a network hiccup leaves exactly one message on the queue.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// apiBase is the API root of whatever queue you're publishing to; the path below
// is the one this worker talks to. Both stay out of the binary.
var apiBase = os.Getenv("QUEUE_API_BASE")

func scheduleRetry(deliveryID, callbackURL string, attempt int) error {
	delay := 60 << attempt // 1 min, 2 min, 4 min, ...
	if attempt > 13 || delay > 604800 {
		delay = 604800 // a delayed message is accepted up to 7 days out
	}

	body, err := json.Marshal(map[string]any{
		"queue": "webhook-retries",
		"payload": map[string]any{
			"delivery_id":  deliveryID,
			"callback_url": callbackURL,
			"attempt":      attempt,
		},
		"delay_seconds": delay,
	})
	if err != nil {
		return err
	}

	for try := 0; try < 5; try++ {
		req, err := http.NewRequest("POST", apiBase+"/v1/queue/publish", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("webhook-retry:%s:%d", deliveryID, attempt))

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<try) * time.Second
			if n, convErr := strconv.Atoi(res.Header.Get("Retry-After")); convErr == nil && n > 0 {
				wait = time.Duration(n) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode >= 300 {
			return fmt.Errorf("publish %s: %s", res.Status, raw)
		}

		log.Printf("delivery %s: attempt %d queued, due in %ds", deliveryID, attempt, delay)
		return nil
	}
	return fmt.Errorf("delivery %s: rate limited on five publish attempts", deliveryID)
}

func main() {
	if err := scheduleRetry("evt_9f2c1a", "https://buyer.example.com/hooks/orders", 3); err != nil {
		log.Fatal(err)
	}
}
```

Three details are load-bearing. The status check is explicit, because a 4xx body carries the reason and swallowing it means you find out from the dead-letter queue instead. The message carries identifiers and a URL rather than the provider's payload history — a few hundred bytes travels better through every broker, and there's a hard ceiling anyway, 256KB on the queue I'm using here. And the attempt counter lives in the message and in the row, which is redundant on purpose: the row is what you query during an incident, the message is what the worker trusts.

I'm running this on Infrai's queue, mostly because the rest of that service already lives there — the queue, the cron trigger that sweeps deliveries nobody claimed, and the operator email all sit behind one REST API with a single key, so adding the sweep was another endpoint rather than another vendor to onboard. The sweep itself only enqueues work; anything that might run past the 900-second execution ceiling belongs in the worker, not in the trigger that wakes it up.

## Where to run this, and where the pattern stops

| Option | How you talk to it | Where it fits | Main limit |
| --- | --- | --- | --- |
| BullMQ on Redis | Node library, your own Redis | You already operate Redis and want in-process control | You own persistence, failover and the DLQ tooling |
| Amazon SQS + EventBridge Scheduler | AWS SDK, IAM | Already deep in AWS; redrive is built in | Per-message delay caps at 15 minutes, so long backoff needs a re-enqueue |
| Google Cloud Tasks | REST, service accounts | HTTP-target retries with per-queue rate limits | Retry policy is per queue, not per event |
| Upstash QStash | REST, HTTP-native | Serverless apps that want the retrier hosted too | Fewer knobs once your retry policy gets opinionated |
| Temporal | SDK plus a worker fleet | Multi-step flows with joins, timers, compensation | You run or buy a cluster; heavy for one callback |
| Infrai queue | REST, one key | Delayed retries next to the rest of the backend | Standard queues are at-least-once, so consumer dedupe is mandatory |

The catch is on every row, and it's mostly about what you're willing to operate. Redis-backed queues are the fastest to start and the most work to keep honest at 3am. The cloud-native queues are solid and pull you into their IAM model. Hosted HTTP queues are the least code and the least control.

The real limit is shape, not vendor. If your webhook flow is a workflow — fan out to five endpoints, wait for all of them, compensate when one gives up — then a queue doesn't support that and you'll end up hand-rolling a state machine; stick with Temporal or Inngest there. Infrai's queue doesn't support DAG orchestration or fan-in joins either, and cron there is a trigger rather than a scheduler with dependencies. As far as I can tell there's no way to dodge that trade: either you own the coordination logic or you buy an engine that owns it for you.

## Verify it, then know how to back it out

Before redriving anything, look at what's actually parked. Sort by age, not by count.

```bash
curl -X GET "$QUEUE_API_BASE/v1/queue/dlq/list/webhook-retries" \
  -H "Authorization: Bearer $INFRAI_API_KEY"
```

Four numbers tell you whether the change worked: dead-letter depth, age of the oldest parked message, the attempt histogram (a healthy system is heavy on attempt 1 and thin after attempt 3), and the count of primary-key conflicts on `webhook_delivery`, which is your duplicate rate. That last one should be non-zero. If it's flat zero, the dedupe path probably isn't being exercised and you won't find out until a redrive doubles someone's invoice.

Redrive one event first, then a hundred, then the rest.

Rolling back is the easy half: flip the publisher back to inline delivery and leave the worker running until the queue drains, because the messages already in flight still need somewhere to land. Don't purge a dead-letter queue to make a graph look better — export it first, even to a CSV nobody reads. Your mileage may vary on the attempt budget; eight was right for our traffic and I'd start lower for anything that moves money.

## References

- [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [BullMQ: retrying failing jobs](https://docs.bullmq.io/guide/retrying-failing-jobs)
- [RabbitMQ dead letter exchanges](https://www.rabbitmq.com/docs/dlx)
- [Stripe: best practices for using webhooks](https://docs.stripe.com/webhooks)
- [Temporal retry policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [GitHub Actions: events that trigger workflows](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows)
- [Wikipedia: Cron](https://en.wikipedia.org/wiki/Cron)
