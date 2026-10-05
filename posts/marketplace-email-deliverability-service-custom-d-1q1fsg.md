# Marketplace Email Deliverability Service — Custom Domain Warmup and Audit Evidence

A marketplace cannot treat an accepted email request as proof that a seller learned about a new order. The operational constraint is evidence: each order needs a durable chain from scheduling through suppression checks, submission, and later bounce or complaint observations. **Choose the service boundary that makes that chain exportable and replayable**, then keep the marketplace database authoritative for notification state.

TL;DR: use a dedicated sending subdomain, authenticate it, start with wanted transactional traffic rather than synthetic volume, and require suppression plus machine-readable delivery events. Polling is acceptable for reconciliation, but the scheduler must remain idempotent because every queue and network boundary can be retried.

I have been paged for missed jobs and duplicate deliveries. The painful lesson was not that a cron expression or queue was inherently wrong. It was that we had no single record answering four ordinary questions: Was this seller notification due? Was the recipient suppressed before submission? Which attempt did the sender accept? What happened afterward? A status dashboard could answer part of that story, but not the order-level question during an incident or compliance review.

## What evidence does an order notice need?

For a new order, create the notification record in the same durable workflow that records the order event. Give the logical notice a stable key such as `order_id + seller_id + template_version`; retries must reuse it. Record timestamps and identifiers for scheduling, suppression evaluation, submission, and outcome ingestion. Preserve the reason for suppression without copying more recipient data than the investigation requires.

This is an audit trail, not an inbox guarantee. SPF defines authorization for hosts to use a domain in the SMTP identity, and its result is one input to mail handling; it does not prove that a person read a message. Keep the claim narrow: the system can show what it attempted, which policy it applied, and which downstream signal it later observed.

A useful state model is uneven on purpose. `pending` can become `suppressed` without a send. `submitted` can later become `bounced` or `complained`. An unknown outcome stays unknown until reconciliation resolves it; inventing `delivered` because no error arrived destroys the value of the evidence.

**The suppression decision belongs before every attempt.** Check the current list at execution time, not only when the job is created. A seller may become suppressed while an order notice waits behind other work, and a retry may run hours after its first scheduling decision.

## Why is polling alone a weak control?

Polling looks simple because it keeps inbound networking out of the first release. It also creates a cursor that can stall, skip, or replay. If the event API is paginated, the checkpoint must advance only after the page is committed locally. If an event has a stable identifier, enforce uniqueness on that identifier; otherwise derive a conservative key from the sender reference, event type, and event timestamp, while accepting that the derivation has collision and precision limits.

Callbacks reduce observation delay, but they shift the burden to request authentication, durable intake, retry handling, and out-of-order processing. A hybrid is usually easier to operate: callbacks feed a small inbox table quickly, while a scheduled poll repairs gaps. Both paths call the same idempotent reducer. The poller is then a reconciliation control rather than the only witness.

There is a real trade-off. A small SaaS with low order volume and no timely complaint requirement may begin with polling, provided its maximum page age is monitored and tested. A system whose suppression policy must react quickly needs a push path or sufficiently frequent polling backed by a measured objective. Do not choose the interval from convenience; derive it from the permitted evidence delay and the API's documented retention and rate boundaries.

No signal, no guess.

## Put idempotency in the preventative path

The sending worker should claim one logical notice, recheck suppression, submit with a stable reference, and persist the result. The following Go sketch leaves transport and storage behind interfaces so the invariant is visible. The database implementation must make `Claim` atomic and enforce a unique logical key.

```go
package notice

import (
    "context"
    "errors"
    "time"
)

var ErrAlreadyClaimed = errors.New("notice already claimed")

type Notice struct {
    Key       string
    OrderID   string
    SellerID  string
    Recipient string
    CreatedAt time.Time
}

type Store interface {
    Claim(context.Context, string) error
    RecordSuppressed(context.Context, string, string, time.Time) error
    RecordSubmitted(context.Context, string, string, time.Time) error
    ReleaseClaim(context.Context, string) error
}

type Suppressions interface {
    Lookup(context.Context, string) (bool, string, error)
}

type Sender interface {
    SendOrderNotice(context.Context, Notice) (string, error)
}

type Worker struct {
    Store Store
    Suppressions Suppressions
    Sender Sender
    Now func() time.Time
}

func (w Worker) Run(ctx context.Context, n Notice) error {
    if err := w.Store.Claim(ctx, n.Key); err != nil {
        if errors.Is(err, ErrAlreadyClaimed) {
            return nil
        }
        return err
    }

    blocked, reason, err := w.Suppressions.Lookup(ctx, n.Recipient)
    if err != nil {
        _ = w.Store.ReleaseClaim(ctx, n.Key)
        return err
    }
    if blocked {
        return w.Store.RecordSuppressed(ctx, n.Key, reason, w.Now())
    }

    ref, err := w.Sender.SendOrderNotice(ctx, n)
    if err != nil {
        _ = w.Store.ReleaseClaim(ctx, n.Key)
        return err
    }
    return w.Store.RecordSubmitted(ctx, n.Key, ref, w.Now())
}
```

There is still a hard edge between remote acceptance and `RecordSubmitted`. A timeout can leave the worker unable to tell whether submission occurred. The sender integration therefore needs an idempotency facility or a lookup by the stable reference; without either, automatic retries can duplicate mail. Document that ambiguity in the runbook and stop blind retries after the bounded attempt policy.

The code also elides leases. In production, a crashed worker must not hold a claim forever, but lease expiry cannot authorize a second send without resolving the first attempt. That distinction has prevented more damage than tuning backoff ever did.

## How should a small SaaS evaluate an email deliverability service for custom-domain warmup?

A short proof-of-concept should exercise the failure path, not just send a welcome message. Use a test domain and controlled recipients, then verify the following capabilities against current documentation and an exported sample:

- custom-domain authentication records, including a clear SPF setup that avoids conflicting policy records;
- a queryable suppression list with reason, source, and timestamps;
- stable identifiers connecting a submission to bounce and complaint observations;
- authenticated callbacks, a polling API with pagination and cursors, or both;
- documented retention, export, rate behavior, and retry semantics;
- separation of transactional order notices from unrelated traffic.

Warmup deserves restraint. A new domain or traffic stream should ramp with real, expected mail to opted-in recipients while operators watch bounce and complaint signals. A feature labeled “automatic warmup” does not remove the need to control recipient quality, authentication, and sending cadence. For a marketplace, the safest first traffic is not fabricated engagement; it is low-volume order mail that sellers asked to receive, with suppression enforced from the first attempt.

Do not score a service by the number of dashboard charts. Run a bounce case, place the address on suppression, replay the same job, interrupt event ingestion, and recover from the last committed cursor. Then export the order's evidence without manually stitching screenshots together. This test exposes the integration boundary before production volume does.

Make the exercise concrete. Create one order notice with a stable logical key, let the worker submit it, and save the sender reference. Feed the same queue item to two workers and confirm that only one submission can cross the claim boundary. Next, ingest the same bounce observation twice and verify that the reducer preserves one outcome without losing the raw evidence. Add the recipient to suppression, schedule another order notice, and prove that no submission occurs. Finally, stop event ingestion after a page is fetched but before its checkpoint is committed; on restart, the page should replay and converge on the same state. The exact fixtures depend on the chosen interface, but the questions do not: can an operator join every observation to the order, explain every skipped send, and replay every interrupted step without creating another seller email? This is the point where a superficially simple API either remains simple or hands complexity to the on-call rotation.

Test the ugly path.

Price can break a shortlist, but it should follow the control test. The hidden operational cost is an interface that cannot resolve ambiguous submissions or reconstruct why a recipient was contacted.

## Operate the control, not the dashboard

Alert on conditions that threaten the evidence chain: oldest pending notice age, uncommitted callback inbox depth, polling checkpoint age, suppression lookup errors, ambiguous submissions, and terminal outcomes that cannot be joined to a logical notice. Counts need denominators. A spike in bounces means little without submitted volume, and low complaint counts may mean the feedback path is broken.

Deploy schema and workers in an order that tolerates mixed versions. Add nullable evidence fields first, write them before requiring them, and make reducers accept duplicate and out-of-order events. Test with replayed pages and repeated callbacks. During an incident, pause new attempts if suppression checks are unavailable; an order can surface in the seller portal while email delivery is repaired.

This advice does not apply unchanged to authentication codes. Browser-assisted SMS code retrieval is a distinct flow with origin binding and user consent considerations described by the WebOTP API; an order notification is neither a credential nor a reason to mix SMS verification semantics into email evidence. Likewise, a legally mandated notice may require counsel-defined retention and channel rules beyond this engineering control.

The final decision is deliberately plain: select the boundary that can prove a policy decision and survive replay. A simple integration is one the on-call engineer can reconcile from durable records after callbacks, polling, or workers fail—not one that merely sends the first message quickly.

## Sources

- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- MDN, WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
