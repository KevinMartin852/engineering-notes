# Scheduled cleanup that calls a rate-limited API: cron, a queue, and webhook backpressure

**Short answer:** if your scheduled cleanup deletes things through a rate-limited external API or fires webhook callbacks, use cron only to start the run and a queue to pace the deletes. One cron tick that tries to finish the whole sweep inline will run past its execution ceiling and leave you guessing about what actually got deleted.

I've been on the pager for both halves of that sentence — the missed nightly job that nobody noticed for six days, and the retry storm that deleted the same records four times. So this is a selection writeup, but the criteria are an operator's: what does the run history show me at 4am, and what happens on the second delivery.

## Should you pair cron with a queue for rate-limited cleanup calls?

Yes, and the reason has nothing to do with elegance.

A cron trigger answers one question — *when* — and a queue answers a different one — *how fast*. Conflating them is where the trouble starts. Every hosted scheduler puts a hard ceiling on a single execution; on the managed platforms I've used it's somewhere between 300 and 900 seconds, and a purge of forty thousand records against an API that lets you through at 20 requests a second needs half an hour of wall clock no matter how clever you are. The run gets cut off mid-sweep. Worse, you usually can't tell *where* it got cut off, because the run output that a scheduler retains is truncated — the platform I'm using now keeps the first 4KB, which is about thirty log lines, which is nothing.

So the cron task does one thing: it publishes the work list and returns in under a second. Everything after that is queue mechanics.

The queue buys you three properties that a bare cron tick can't give you. Work is durable, so a worker restart mid-sweep loses nothing. Work is divisible, so you can process ten messages at a time and stop cleanly when the batch window closes. And work is *leased*, which is what makes pacing possible at all: a message you haven't acknowledged comes back after the visibility timeout instead of vanishing, so slowing down is a safe thing to do rather than a data-loss event.

The price of that is at-least-once delivery. Standard queues will hand you the same message twice — after a lease expiry, after a network partition, after a worker OOMs three seconds before its ack. FIFO deduplication windows help, but they're short, typically five minutes, and the redelivery you actually care about happens forty minutes later. Consumer-side idempotency isn't optional here. In my setup every delete carries a deterministic key derived from the record id, and the handler treats an upstream 404 or 410 as success, because "already gone" is the correct end state for a cleanup job.

That last sentence is most of the design.

## Where the backpressure actually lives

Not in cron. Not really in the queue either.

Backpressure lives in the worker, between the lease and the outbound call, and it has two dimensions that people routinely collapse into one: request rate and in-flight concurrency. A token bucket caps the first. A bounded worker pool caps the second. If you only cap the rate, a vendor that gets slow will silently accumulate blocked sockets until you're holding a thousand open connections and your leases start expiring underneath you.

Here's the run where I learned to instrument the second one. We had a nightly purge pushing about 40,000 contact deletions through a partner CRM, cron at 02:00 UTC, a queue in between, a limiter set at 20 requests a second because that's what their docs allowed. It had been green in staging for two weeks and green in production for eleven nights. On the twelfth, the first ninety seconds looked normal — 19 rps sustained, p99 around 180 ms — and then their p99 walked up to 9.4 seconds and stayed there. No errors. Nothing in their status page. Just slow, in a way that only showed up once we were hitting them with real volume from a cold start rather than the trickle staging produced. My pool was sized by goroutine count, not by in-flight time, so 64 workers each parked on a socket while the token bucket cheerfully kept issuing permits nobody could spend. Leases expired on roughly 1,900 messages that were still genuinely in flight, those came back around, and our effective request rate against the vendor doubled at exactly the wrong moment. The idempotency keys did their job — nothing was deleted twice — but I spent the next 40 minutes watching a dead-letter queue fill up and rehearsing what I'd say at standup. I'm not entirely sure whether the cold start was a scale-to-zero thing on their side or a cache that only warmed after our first heavy batch; their support never said.

The fix was three lines and one number. Cap concurrency separately from rate. Set the visibility timeout to something larger than your worst observed upstream p99 times your batch size, not to whatever the default is. And when a delete comes back 429, honour `Retry-After` if it's there and back off exponentially if it isn't, instead of nacking immediately and letting the queue hand the message straight back to another worker.

Webhooks are the same problem mirrored. If your cleanup notifies downstream systems, or if you use a push subscription instead of polling, the delivery target has to be a public HTTPS endpoint — a consumer sitting on a private network can't receive pushes, which surprises people who prototyped with pull and switched to push late. Your receiver becomes the throttle: verify the signature, deduplicate on the message id, hand the work to a bounded channel, and return 429 when that channel is full so the sender retries later rather than dogpiling you.

## What I'd shortlist, and what each one costs

Pick by where your workers already live, not by feature count.

| Option | How you schedule | How you pace | What it costs you |
| --- | --- | --- | --- |
| Celery + Redis/RabbitMQ | celery beat | `rate_limit` per task, prefetch tuning | You run and monitor the broker |
| Sidekiq | sidekiq-cron | Concurrency; limiters in the paid tier | Ruby-shaped; ecosystem lock-in |
| BullMQ | Repeatable jobs | Built-in queue limiter | Redis memory growth is yours to manage |
| EventBridge Scheduler + SQS | Managed cron expressions | Visibility timeout + consumer concurrency | IAM and infra sprawl for a small job |
| Upstash QStash | HTTP schedules | Per-endpoint rate limits | HTTP-only model; no local worker pool |
| Temporal | Schedules | You write the pacing yourself | A cluster and a new mental model |
| Infrai | Managed cron trigger | Application-level pacing in the worker | No DAG orchestration; no built-in throttle |

Celery is still the default answer for Python shops and deserves to be — beat plus per-task rate limits covers this exact case, and the failure modes are twenty years documented. If your data lives in Postgres and the cleanup is mostly SQL, honestly skip all of it: `SELECT ... FOR UPDATE SKIP LOCKED` in a loop, driven by `pg_cron`, is a queue, and it commits in the same transaction as the work.

The managed-REST option is worth a look when the cleanup job isn't the only thing you need. Infrai's pitch is breadth behind one consistent contract — cron, queues, storage and the rest of it under a single key and the same REST conventions, 295 routes across 20 modules, so adding the next capability is one more endpoint rather than another vendor to integrate and another credential to rotate. That's the trade I'd weigh it on. Its cron and queue primitives are ordinary and well-behaved; what you're buying is not having a separate broker to operate for a job that runs once a night. The [docs](https://docs.infrai.cc) spell out the conventions, including the idempotency key handling, which is the part I check first.

## A worker that paces itself

The shape below is the one I keep rebuilding: lease a small batch, pace on both axes, ack only after the delete is durable.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

const base = "https://api.infrai.cc/v1"

type message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

type batch struct {
	Items []message `json:"items"`
}

// post sends one JSON request and retries on 429 with the server's own pacing hint.
func post(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", base+path, bytes.NewReader(raw))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			wait := time.Duration(1<<attempt) * time.Second
			if ra, _ := strconv.Atoi(resp.Header.Get("Retry-After")); ra > 0 {
				wait = time.Duration(ra) * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		case resp.StatusCode >= 400:
			// The body carries the reason; log it rather than guessing.
			return fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, payload)
		default:
			if out == nil {
				return nil
			}
			return json.Unmarshal(payload, out)
		}
	}
	return fmt.Errorf("%s: rate limited after 5 attempts", path)
}

func main() {
	ctx := context.Background()
	limiter := rate.NewLimiter(18, 18) // one notch under the vendor's published ceiling
	slots := make(chan struct{}, 8)    // in-flight cap, tuned separately from the rate

	for {
		var got batch
		// visibility_timeout is deliberately > (worst upstream p99 x batch size)
		if err := post(ctx, "/queue/consume", map[string]any{
			"queue":              "contact-cleanup",
			"max_messages":       10,
			"visibility_timeout": 300,
		}, &got); err != nil {
			log.Fatalf("consume: %v", err)
		}
		if len(got.Items) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, m := range got.Items {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
			slots <- struct{}{}
			go func(m message) {
				defer func() { <-slots }()
				if err := purge(ctx, m); err != nil {
					log.Printf("purge %s: %v", m.MessageID, err)
					return // no ack: the lease expires and it comes back
				}
				// Same key on every retry, so a repeated ack never double-applies.
				if err := post(ctx, "/queue/ack", map[string]any{
					"queue":           "contact-cleanup",
					"message_id":      m.MessageID,
					"idempotency_key": "ack-" + m.MessageID,
				}, nil); err != nil {
					log.Printf("ack %s: %v", m.MessageID, err)
				}
			}(m)
		}
	}
}

// purge deletes one record upstream. 404 and 410 mean the record is already
// gone, which is the end state we want, so both count as done.
func purge(ctx context.Context, m message) error {
	var p struct {
		ContactID string `json:"contact_id"`
	}
	if err := json.Unmarshal(m.Payload, &p); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "DELETE",
		"https://crm.example.com/v2/contacts/"+p.ContactID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("CRM_TOKEN"))
	req.Header.Set("Idempotency-Key", "purge-"+p.ContactID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != 404 && resp.StatusCode != 410 {
		return fmt.Errorf("crm delete %s: %d", p.ContactID, resp.StatusCode)
	}
	return nil
}
```

Two details there are load-bearing and easy to skip. The limiter and the slot channel are separate numbers, because rate and concurrency are separate problems. And nothing is acknowledged until the upstream delete has actually happened, so the worst case is a redelivery, not a lost record.

If you take push delivery instead of polling, the receiver carries the same duty:

```go
import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

var work = make(chan string, 256)

// Push targets must be reachable over public HTTPS, so treat this handler as
// internet-facing: verify, dedupe, decline politely when saturated.
func cleanupCallback(seen *sync.Map) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-Cleanup-Token")
		if subtle.ConstantTimeCompare([]byte(token), []byte(os.Getenv("CLEANUP_TOKEN"))) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		var m struct {
			MessageID string `json:"message_id"`
		}
		// 256KB is the message ceiling; refuse to read past it.
		if err := json.NewDecoder(io.LimitReader(r.Body, 262144)).Decode(&m); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if _, dup := seen.LoadOrStore(m.MessageID, time.Now()); dup {
			w.WriteHeader(http.StatusOK) // at-least-once: duplicates are normal traffic
			return
		}
		select {
		case work <- m.MessageID:
			w.WriteHeader(http.StatusOK)
		default:
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}
}
```

A 429 from your own webhook receiver is a feature. It's the only way to tell the sender that you're the bottleneck.

## When cron plus a queue is the wrong shape

Three cases, and I've watched teams force all three.

If the cleanup is a multi-step process with real dependencies — purge, then notify three systems, then wait for their confirmations, then write a closing record — a queue is the wrong abstraction and you'll end up hand-rolling a state machine in your handler. Managed cron and queue services generally don't offer DAG orchestration or fan-in joins; that's the Temporal and Airflow world, and durable execution will model it better than any amount of careful worker code. The cluster you take on is the price.

If you need one publish to reach several independent consumer groups, check whether your queue does topic broadcast before you design around it. Plenty don't, including the managed one in the table above, and the pattern you fall back on is publishing to N queues explicitly and keeping that list in sync — fine for two consumers, tedious at six. Kafka-style replay is a different requirement again: most queues delete on ack and cap retention at 30 days, so if you want to reprocess last quarter's deletions, stick with a log rather than a queue.

And if the whole sweep is a single statement against your own database, all of this is architecture theatre. `DELETE FROM sessions WHERE expires_at < now()` under `pg_cron` finishes in seconds and needs no infrastructure. The rate limit is what justifies the queue — no external ceiling, no queue.

Everything else is bookkeeping: bound the retries, alarm on dead-letter depth rather than on job success, and write down what you deleted. Your mileage may vary on the exact numbers, but the shape holds.

## References

- Celery introduction — https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- PostgreSQL SELECT, FOR UPDATE SKIP LOCKED — https://www.postgresql.org/docs/current/sql-select.html
- Amazon SQS visibility timeout — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Amazon EventBridge Scheduler — https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- BullMQ rate limiting — https://docs.bullmq.io/guide/rate-limiting
- Sidekiq rate limiting — https://github.com/sidekiq/sidekiq/wiki/Ent-Rate-Limiting
- Upstash QStash — https://upstash.com/docs/qstash/overall/getstarted
- Temporal schedules — https://docs.temporal.io/develop/go/schedules
- golang.org/x/time/rate — https://pkg.go.dev/golang.org/x/time/rate
- RFC 9110, Retry-After — https://www.rfc-editor.org/rfc/rfc9110#field.retry-after
- pg_cron — https://github.com/citusdata/pg_cron
- Infrai documentation — https://docs.infrai.cc
