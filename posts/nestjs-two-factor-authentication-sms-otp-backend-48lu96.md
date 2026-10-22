# NestJS Two-Factor Authentication SMS OTP Backend: Rental Delivery Evidence

Short answer: for a NestJS two-factor authentication backend, use SMS OTP for the challenge but suppress a recipient only after a classified delivery event. Keep the event, classifier decision, and release path in an append-only audit log. In a property-management marketplace, that gives an operator evidence for why a buyer did not receive a verification challenge without treating every timeout as a permanent invalid address.

The NestJS boundary is not the hard part. The hard part is preserving the difference between a hard bounce, a temporary failure, an expired SMS OTP, and an operator-initiated suppression. Those states have different recovery actions. I use one stable recipient key, an idempotency key per provider event, and an explicit transition table. A message that is retried twice should still produce one state change.

Five minutes and five guesses are reasonable starting limits for one OTP, but they are policy values, not universal truths. Record them with the challenge so an audit log can explain which rule was active.

## How Should NestJS Handle Two-Factor Authentication with SMS OTP?

A delivery status by itself is not an audit trail. The useful record answers five questions: which recipient was affected, which communication attempt produced the signal, how the signal was classified, which policy version made the decision, and who or what released the suppression. Store the received event as evidence before applying policy. Store the policy result separately.

Keep it boring.

For a buyer verifying access to a rental listing, a plausible sequence is: an SMS OTP is issued, the buyer submits it, an email receipt later hard-bounces, and a support agent asks why the address is suppressed. The OTP result must not be overwritten by the email result. Channel, purpose, and attempt belong in the event key.

I would model the durable facts like this:

| Record | Required fields | Operational use |
| --- | --- | --- |
| Delivery event | event ID, channel, recipient hash, provider status, received time | Deduplicate and investigate |
| Policy decision | decision, reason code, policy version, decision time | Explain suppression |
| Verification attempt | challenge ID, purpose, outcome, expiry | Protect the OTP flow |
| Release action | actor, reason, ticket ID, time | Permit controlled recovery |

Keep message bodies and OTP values out of these records. A salted recipient hash gives operators a stable lookup key while reducing the chance that an exported audit file becomes a contact list. The exact retention period is a policy decision; the important engineering property is that expiration removes access to evidence according to that policy, rather than silently deleting the history needed to explain a decision.

The awkward case is a late event. Imagine an email delivery event arrives after the retry worker has already scheduled a second receipt, while the buyer is completing the SMS challenge from a different device. The consumer must deduplicate the event, preserve its source timestamp, and evaluate the current email state without touching the SMS challenge record. If the result is permanent failure, the next email send is blocked; if it is temporary failure, the retry policy remains eligible. That is a longer path through the system, but it is the path an operator needs to reconstruct when the support ticket says “the code worked, but the receipt never arrived.”

## How Do Failure States Avoid Becoming Permanent?

Use a small state machine instead of a boolean called `invalid`. A hard address failure can move an email recipient to `suppressed`; a temporary mailbox or carrier failure should move it to `retryable`; an SMS challenge should move to `expired` only when its challenge deadline passes. The same recipient may be suppressed for email and reachable by SMS.

A practical transition table is:

| Signal | State change | Next action |
| --- | --- | --- |
| Address rejected as undeliverable | email: active -> suppressed | Stop email sends; require verified release |
| Temporary remote failure | email: active -> retryable | Retry under a bounded schedule |
| SMS send accepted, no code submission | challenge: issued -> expired | Require a new challenge |
| Invalid code submission | challenge: issued -> failed | Count attempts; do not suppress the address |
| Correct code submission | challenge: issued -> verified | Permit the intended buyer action |

The distinction matters during an incident. If a queue stalls for 20 minutes, marking every unconfirmed OTP recipient invalid creates a cleanup job and can block legitimate buyers. If a permanent rejection arrives, continuing to send receipts creates repeat failures and weakens the evidence that the suppression rule is working.

Throttling belongs at more than one boundary. Limit challenge creation per account and per recipient, limit code guesses per challenge, and cap retries for a delivery event. Use a monotonic expiry check for the challenge, but use the event's source timestamp for investigation. Those clocks answer different questions.

I initially treated an unconfirmed code as a bad recipient. That was the wrong boundary: authentication failure describes a challenge attempt, while delivery suppression describes a destination. Keeping those records separate makes recovery narrow and explainable.

## A Safe Implementation Boundary in Go

NestJS can own the HTTP controller and queue consumer while a small application service owns the transition. Keeping that service independent of a provider SDK makes tests focus on policy. The provider adapter should normalize external statuses into a finite internal set before this code runs.

```go
package delivery

import (
    "context"
    "time"
)

type Status string

const (
    Active     Status = "active"
    Suppressed Status = "suppressed"
    Retryable  Status = "retryable"
)

type Event struct {
    ID             string
    Channel        string
    RecipientHash  string
    ProviderStatus string
    SourceTime     time.Time
}

type Decision struct {
    RecipientHash string
    Channel       string
    From          Status
    To            Status
    Reason        string
    PolicyVersion string
}

type Store interface {
    RecordEvent(ctx context.Context, event Event) (bool, error)
    Current(ctx context.Context, channel, recipientHash string) (Status, error)
    ApplyDecision(ctx context.Context, decision Decision) error
}

func Classify(ctx context.Context, store Store, event Event) error {
    inserted, err := store.RecordEvent(ctx, event)
    if err != nil || !inserted {
        return err // duplicate event: no second policy transition
    }

    current, err := store.Current(ctx, event.Channel, event.RecipientHash)
    if err != nil {
        return err
    }

    target, reason := Active, "accepted_or_unknown"
    switch event.ProviderStatus {
    case "permanent_failure":
        target, reason = Suppressed, "permanent_delivery_failure"
    case "temporary_failure":
        target, reason = Retryable, "temporary_delivery_failure"
    }

    if current == Suppressed && target != Suppressed {
        return nil // release is an explicit, audited action
    }
    if current == target {
        return nil
    }

    return store.ApplyDecision(ctx, Decision{
        RecipientHash: event.RecipientHash,
        Channel:       event.Channel,
        From:          current,
        To:            target,
        Reason:        reason,
        PolicyVersion: "recipient-policy-v3",
    })
}
```

The `RecordEvent` operation must have a uniqueness constraint on the source event ID. The transition and its audit entry should commit in one database transaction, or the store needs an outbox that makes the missing side observable. A retry after a timeout must not leave an operator guessing whether the decision was applied.

For the NestJS side, pass a normalized event into this service and return an acknowledgement only after the event is durably recorded. Do not put suppression logic in a controller guard that only sees the current request. A guard can prevent one send; it cannot explain a decision six weeks later.

## How Do We Verify and Roll Back the Rule?

Verification starts with fixtures, not a dashboard. Test duplicate delivery events, events arriving out of order, an already suppressed email recipient receiving a temporary failure, and an SMS challenge expiring while a late code submission is in flight. Assert both the current state and the audit rows.

Then run a replay against a sanitized event sample in a dry-run mode. The replay should report proposed transitions by channel and reason without writing them. I want a count of unknown provider statuses in that report; an unseen status is a classification gap, not a reason to guess. Alert on it.

A rollback is a policy release, not a delete. Disable the new policy version for new decisions, preserve the evidence and decisions already made, and use an audited release operation for recipients incorrectly suppressed by that version. Require a ticket or incident identifier. The release should be narrow enough that an operator can name the affected policy version and reason code.

For an SRE runbook, the key checks are simple: event ingestion lag, duplicate-event rate, unknown-status count, retryable backlog, suppression transitions by channel, and OTP verification success by challenge age. These signals connect the system's behavior to the evidence an operator will review. A single delivery percentage cannot do that.

The decision rule is therefore conservative: classify first, suppress only on a durable permanent signal, keep email and SMS state separate, and make recovery an explicit audited transition. That approach is less convenient than a single `valid` flag, but it leaves a defensible record when a property manager needs to explain why a buyer's verification or receipt stopped.

## References

- https://www.rfc-editor.org/rfc/rfc5321
- https://www.rfc-editor.org/rfc/rfc3463
- https://www.rfc-editor.org/rfc/rfc8058
- https://mustache.github.io/mustache.5.html
- https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview
