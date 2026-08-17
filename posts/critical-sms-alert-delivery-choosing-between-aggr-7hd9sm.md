# Critical SMS Alert Delivery (Choosing Between Aggregation and Dedicated APIs)

Use a dedicated SMS API when a compliance notice needs a queryable, per-message delivery history; use a cloud notification aggregator when the operational win from one alerting control plane matters more than detailed carrier feedback. The deciding constraint is evidence, not the send call. Neither choice proves that a patient read a message, so keep the compliance record in your own system and treat provider status as one event in that record.

For critical alerts across the US and EU, the runbook should separate three actions: accepting a send request, observing downstream delivery, and deciding whether another attempt is safe. Don't turn an ambiguous timeout into an automatic resend. First reconcile by your idempotency key or provider message ID, then retry only when policy and the latest terminal state allow it.

That is the short answer.

## Start with the delivery evidence the notice requires

An SMS provider's initial success response normally answers a narrow question: it accepted the request and assigned an identifier. It doesn't establish handset delivery, and it certainly doesn't establish that the intended person read the compliance notice. The durable audit row should therefore begin before the outbound request. Give it an internal notice ID, recipient reference, template version or content hash, jurisdiction, creation time, and an idempotency key. Store the provider ID and every later status event against that row.

The useful split is aggregation versus messaging specialization. Amazon SNS can publish SMS alongside other AWS notification paths, and its SMS delivery status is written through CloudWatch Logs when delivery-status logging is configured. Twilio exposes Message resources, status callbacks, and a fetch operation for a message. Vonage documents delivery receipts sent to a webhook. Those are different operating shapes, not a ranking.

| Operational concern | Amazon SNS | Twilio Messaging | Vonage SMS API |
| --- | --- | --- | --- |
| Initial send evidence | Publish returns a message identifier | Message creation returns a Message resource | Send response includes a message identifier |
| Asynchronous status path | Delivery status through CloudWatch Logs | Status callback URL | Delivery receipt webhook |
| Reconciliation shape | Correlate the publish ID with logs | Fetch the Message resource by SID | Persist receipt events by message ID |
| Best fit | Teams already operating AWS alerting and log controls | Teams needing a message-centric resource and callback workflow | Teams prepared to operate a delivery-receipt endpoint |
| Main trade-off | Status evidence lives in the logging path rather than a message polling resource | A second messaging control plane must be operated | Receipt ingestion and reconciliation remain application responsibilities |

This table is deliberately about control flow. Country coverage, sender identity rules, registration, permitted traffic, and final status vocabularies change by destination and use case. Check the current provider and carrier requirements for every launch country; I'm not sure any static global matrix stays accurate long enough to serve as a production control.

## How should critical SMS alerts compare delivery status, retries, and polling?

Compare state semantics before endpoint ergonomics. Write down which states are merely provider-local, which represent carrier feedback, which are terminal, and whether events can arrive more than once or out of order. Then map each provider's vocabulary into a small internal state machine such as `queued`, `accepted`, `delivered`, `failed`, and `unknown`. Preserve the raw status beside the mapped state so an investigation can recover detail that the common model intentionally discarded.

Polling is reconciliation, not the primary event stream. A dedicated API with a fetchable message resource gives a worker a direct way to close gaps after a callback is late or missing. A log-based integration can perform the same operational job by querying the configured log destination and correlating the message ID, but that is a different interface with different permissions, retention, and alerting. Vonage's documented delivery-receipt flow is webhook-led, so the application should persist receipts as they arrive and define its reconciliation process explicitly rather than assume every provider has an identical polling resource.

Retries are where otherwise tidy designs produce duplicate compliance notices. A transport error before the client receives the provider ID leaves the outcome unknown: the provider may have accepted the request even though the application saw a timeout. Mark that attempt `unknown`, reconcile it, and page or queue it for bounded review according to urgency. Do not immediately create a fresh attempt. If the provider conclusively rejects a request before submission, a retry may be safe after the cause is classified; if a terminal delivery failure arrives, the policy may choose another permitted channel rather than repeatedly sending the same SMS.

Consider a hypothetical notice `N-1042`. The ledger commits its idempotency key at 10:00:00, the send begins, and the client deadline expires at 10:00:03 without a response. A naive worker puts the job back on the queue, where another worker sends it again at 10:00:05. If the first request was accepted, the patient now receives two notices and the audit trail contains two provider IDs for one policy action. The safer sequence records an `unknown` attempt at 10:00:03, blocks another send under the same key, and starts reconciliation. Finding the first provider ID or its delivery event closes the attempt; finding conclusive evidence that submission never occurred permits a policy-controlled retry. If available evidence cannot resolve the outcome before the compliance deadline, the runbook escalates to a human or an approved alternate channel. It does not silently convert uncertainty into duplication.

Keep it boring.

For a healthtech notice, the escalation clock belongs to the application, not to an SDK retry loop. A worker can set a reconciliation deadline after acceptance, inspect the durable state when that deadline expires, and choose one action: wait, query the provider evidence, use an approved alternate channel, or ask an operator to decide. The policy should also cap attempts per notice and recipient. Exact delays and caps depend on notice urgency, consent, local rules, and carrier guidance, so they shouldn't be copied from a generic example.

## Build an auditable, monotonic delivery state machine

The safe implementation has two write paths into one ledger. The sender writes `created` before network I/O and `accepted` only after it stores the provider identifier. The webhook or log consumer appends raw observations, verifies that the observation belongs to the expected provider message, and advances the normalized state only when the transition is allowed. An append-only event table is valuable here because a mutable `status` column alone cannot explain why an alert moved, when an out-of-order callback arrived, or whether an operator initiated an alternate channel.

The focused Go example below shows the boundary. It leaves provider authentication and webhook signature verification to adapters because those mechanisms differ, but it makes idempotency and monotonic transitions part of the domain contract rather than optional caller behavior.

```go
package delivery

import (
	"context"
	"errors"
	"time"
)

type State string

const (
	Created   State = "created"
	Accepted  State = "accepted"
	Delivered State = "delivered"
	Failed    State = "failed"
	Unknown   State = "unknown"
)

type Notice struct {
	ID             string
	RecipientRef   string
	TemplateHash   string
	IdempotencyKey string
}

type Observation struct {
	NoticeID         string
	ProviderMessage  string
	RawStatus        string
	MappedState      State
	ObservedAt       time.Time
}

type Sender interface {
	Send(ctx context.Context, n Notice) (providerMessage string, err error)
}

type Ledger interface {
	Create(ctx context.Context, n Notice) error
	RecordAccepted(ctx context.Context, noticeID, providerMessage string) error
	AppendObservation(ctx context.Context, o Observation) error
	FindByKey(ctx context.Context, key string) (Notice, State, error)
}

var ErrOutcomeUnknown = errors.New("send outcome requires reconciliation")

func Submit(ctx context.Context, ledger Ledger, sender Sender, n Notice) error {
	if existing, _, err := ledger.FindByKey(ctx, n.IdempotencyKey); err == nil {
		// A prior durable attempt owns this key; reconcile it instead of resending.
		_ = existing
		return nil
	}
	if err := ledger.Create(ctx, n); err != nil {
		return err
	}

	providerMessage, err := sender.Send(ctx, n)
	if err != nil {
		return ErrOutcomeUnknown
	}
	return ledger.RecordAccepted(ctx, n.ID, providerMessage)
}
```

Production code needs a transaction or uniqueness constraint around `IdempotencyKey`; the read in this compact example isn't enough to prevent two workers racing. It also needs an adapter-specific classification that distinguishes a confirmed pre-submission rejection from an ambiguous network outcome. That classification must be tested against documented responses, not guessed from error strings. The state consumer should reject transitions such as `delivered` back to `accepted`, while still appending the late raw event for audit. Duplicate callbacks should append at most once by a stable event key or be harmless when reapplied.

Retention deserves the same attention as delivery. Keep enough data to reconstruct who initiated the notice, which approved template was used, what destination reference was selected, which provider ID was assigned, and how the state evolved. Minimize message content and phone-number exposure in operational logs; the audit need is usually correlation and policy evidence, not another uncontrolled copy of sensitive text. The actual retention period and access rules come from the organization's legal and security owners.

## Verify the path, deploy it safely, and know how to roll back

Test the state machine independently from carrier behavior. Table-driven tests should cover duplicate send commands, concurrent commands using the same idempotency key, duplicate callbacks, an older callback after a terminal event, an unknown raw status, and a timeout with no provider ID. Adapter contract tests should use documented test facilities where available and assert that provider IDs and timestamps are captured without putting real patient information in fixtures.

Before enabling a country, run a controlled canary with authorized test recipients and inspect the complete chain: ledger creation, provider acceptance, callback or log arrival, normalized transition, reconciliation closure, and audit export. Record latency as an observed distribution rather than promising a universal delivery time. Alert separately on send rejection rate, notices stuck in `unknown`, callback or log lag, and reconciliation backlog; a single aggregate “SMS success” metric hides the failure mode an operator needs to act on.

Deploy policy and adapters independently. Start with shadow normalization, where incoming statuses are mapped and measured but don't trigger retries or alternate channels. Then enable decisions for a narrow jurisdiction and notice class. A rollback should disable automated follow-up while leaving receipt ingestion and ledger writes running. Otherwise the rollback destroys the evidence needed to explain what happened during the deployment.

One more check: rehearse key rotation, callback authentication failure, log-destination permission loss, and provider substitution. The exercise passes only when an operator can identify affected notice IDs, halt duplicate-producing automation, preserve new observations, and resume reconciliation without editing database rows by hand.

## What are the limitations and the final decision rule?

The catch is that a dedicated SMS API is not suitable when the team cannot own another authenticated webhook, reconciliation worker, retention policy, and on-call surface. In that case, stick with the existing cloud notification aggregator if its log-based evidence satisfies the audit requirement and the team can reliably query, retain, and alert on that evidence. Fewer control planes can be a real reliability advantage.

The reverse trade-off matters too. An aggregator is a poor fit when the audit workflow requires direct, per-message querying, detailed lifecycle events, and rapid reconciliation that its documented status path doesn't expose in the needed form. Choose a dedicated API then, but only after proving its callback and query semantics in the ledger design. A provider dashboard is useful during an investigation; it isn't the system of record.

SMS itself may be the wrong channel when the notice contains sensitive detail, requires verified identity, requires proof of reading, or must support a legally defined acknowledgment. Use SMS as a prompt to enter an approved authenticated experience, or choose another channel under the organization's compliance policy. Delivery receipts have limits — they are transport evidence, not human acknowledgment.

So the decision rule is plain: choose the integration whose documented evidence you can normalize, retain, reconcile, and operate during an ambiguous send. Availability claims and API aesthetics come after that. Your mileage may vary by country and traffic type, which is why a country-specific canary and a rehearsed rollback belong in the acceptance criteria.

## References

- https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html
- https://docs.aws.amazon.com/sns/latest/api/API_Publish.html
- https://www.twilio.com/docs/messaging/api/message-resource
- https://www.twilio.com/docs/messaging/guides/track-outbound-message-status
- https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts
- https://datatracker.ietf.org/doc/html/rfc8058
- https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview
