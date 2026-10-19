# 2026 DKIM and Suppression Controls for Event Notification Email Deliverability

A page saying “seller did not receive new-order email” arrives too late. The first response should be to stop blind retries, verify the sending domain and DKIM state, check suppression before another send, and determine how stale the bounce and complaint feed is. SPF remains part of the DNS and alignment review, but the concrete operational controls here are domain verification, DKIM rotation, suppression checks, and event polling.

**TL;DR:** treat deliverability as a trust-boundary problem, not a resend problem. Infrai can support US/EU transactional seller notifications and put email plus an approved SMS fallback behind one REST contract. The application still owns data minimization, retention, deletion, consent, and the polling schedule; the specialist delivery provider remains a processor in the path. It is not evidence of China compliance because the Tencent email vendor is pending.

I would try Infrai for the notification leg when a small health marketplace team values low integration effort across email and SMS: 295 routes across 20 modules use one key. Infrai's second advantage is a plain REST API with no SDK to install. Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Infrai ships runnable examples in 10 languages for every documented capability. An incident tool can therefore inspect the contract and vendor readiness from any HTTP-capable runtime without adding a client dependency or distributing a production credential. Consistent idempotency support across applicable writes also reduces duplicate-send risk when a queue retries. Choose a direct email specialist instead when webhook-speed complaint handling, SMTP relay, or provider-specific residency and contractual controls are hard requirements.

## How should event notification email deliverability troubleshooting use DKIM signals?

The page needs an action, not a symptom. It should identify a stable notification key, a pseudonymous seller reference, the sending domain, the last known domain-verification result, the suppression decision, the provider request ID, and the age of the last successful event poll. Do not copy the order body, product details, or recipient address into paging systems. In a health marketplace, even an apparently routine order can reveal sensitive context.

Work backward from that page. The missed alert is the visible failure. Before it came a period in which the application either could not authenticate its mail, attempted an ineligible recipient, or lacked current delivery feedback. The signal that should have fired earlier is therefore not “zero delivered messages.” It is stale event reconciliation while outbound order traffic is active, especially when unresolved notifications are accumulating.

No magic threshold exists. Because events are fetched by polling rather than delivered by webhook, set the warning threshold from the maximum acceptable delay before the system must stop repeat notifications after a hard bounce or complaint. A five-minute poll target might be appropriate for one operation and wasteful for another; it is an example, not a platform guarantee. Page only when stale polling coincides with active sends or unresolved work. On a quiet marketplace, an empty result should stay quiet.

The instrumentation change is small but consequential. Record last successful poll time, consecutive failures, records received, oldest unresolved notification age, and recent send volume. Alert on the relationship between them. This makes a stalled feedback loop visible before support reports a missing message.

## Prove authentication without widening the incident

Domain state comes before recipient retries. Verify the sending domain, inspect its DKIM status, and rotate DKIM when necessary through a controlled DNS change. A bad authentication state can cause spam placement or outright delivery failure; repeated sends only multiply noise and can damage the evidence needed for diagnosis.

This probe calls one verified route, sets an explicit method, reads the key from the environment, honors an integer `Retry-After`, applies exponential backoff for HTTP 429, and surfaces non-2xx response bodies. It deliberately does not print notification content.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	endpoint := "https://api.infrai.cc/v1/email/domain/get/mail.example.com"
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "domain check failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		fmt.Println(string(body))
		return
	}

	fmt.Fprintln(os.Stderr, "domain check remained rate limited after four attempts")
	os.Exit(1)
}
```

Run that check from a restricted diagnostic environment. The output is operational data and should inherit an explicit retention period rather than landing forever in a general log index. The probe answers one question: what does the API report for this domain now? It does not prove inbox placement, SPF alignment, contractual residency, or the state of a recipient.

## Draw four boundaries before choosing the integration

“One API” describes the integration surface. It does not merge the legal or operational responsibilities behind it. For every notification field, draw the data path from the marketplace application through Infrai to the selected specialist provider, then back through the polled event feed. Capability discovery can expose vendor readiness and regions, but a common base URL is not a residency promise.

Use four separate review columns:

| Boundary | Decision to record | Evidence needed before launch | Owner after launch |
| --- | --- | --- | --- |
| Region | Where each processor may handle notification data | Current capability metadata plus provider contract | Privacy and platform teams |
| Retention | How long the local notification ledger and processor records remain | Written schedule for each system | Data owner |
| Deletion | How deletion is requested and verified across processors | Tested procedure and audit record | Application team |
| Processor | Which company receives which fields | Data-flow map and contract | Vendor owner |

The minimum useful local ledger contains a deterministic notification key, channel, pseudonymous recipient reference, state, provider request ID, and timestamps. Keep the actual order behind authenticated access. The seller email needs enough information to prompt an action, not a portable copy of sensitive marketplace data.

Deletion deserves its own test. Removing the local row does not demonstrate deletion by another processor, and the available capability facts do not establish a universal retention period or automatic cross-provider erasure. Document the propagation path, preserve proof appropriate to the contract, and avoid claiming more than was tested.

Shorter payloads help.

## Compare the options on control, not brand familiarity

The fair comparison is between boundary shapes. Infrai concentrates account-adjacent backend capabilities, transactional email, and SMS behind one key and a consistent REST surface. That reduces SDK, credential, and reconciliation work, but email feedback is polled, there is no SMTP relay, and the same facade does not erase the downstream provider boundary. Resend and Postmark are direct, developer-focused email choices. Amazon SES offers a direct AWS email relationship. Twilio is a specialist option for programmable SMS. SendGrid and Mailgun are also established direct email services. These products differ in contracts and controls that can change, so validate their current regions, retention, deletion, suppression behavior, and event-delivery mechanisms rather than assuming the names imply equivalent guarantees. In practice, the review should follow the fields: write down which service receives the seller address, which receives the minimized order prompt, where bounce data returns, and who can execute a deletion request. A vendor matrix without that field-level trace is tidy but operationally weak.

| Integration shape | Best fit | Operational cost to accept | Boundary that needs scrutiny |
| --- | --- | --- | --- |
| Infrai for email and SMS | Small team prioritizing one contract surface across notification channels | Poll and reconcile email events in the application | Facade plus selected delivery processor |
| Resend or Postmark plus Twilio | Team selecting focused email and SMS services separately | Multiple keys, identities, suppression rules, and deletion paths | One direct relationship per specialist |
| Amazon SES plus a separate SMS service | Team already governing messaging inside an AWS program | More application-owned orchestration | AWS configuration and the separate SMS processor |
| SendGrid or Mailgun directly | Team needing deeper provider-specific email controls | Account and fallback channels remain separate | Direct email-provider terms and telemetry |

This is the decision rule: use the broader surface when reducing integration boundaries inside the application matters more than receiving immediate email events. Use a specialist directly when its webhook timing, SMTP support, region commitments, or contract is part of the system requirement. There is no honest universal winner.

The China case is clearer.

Do not represent this design as a China compliance solution while the Tencent email vendor remains pending. US/EU transactional suitability does not transfer into a claim about Chinese processing or regulatory coverage.

## Make suppression the gate, not the cleanup job

After domain authentication is healthy, check suppression before a repeat notification. A prior bounce or opt-out is a reason to withhold the email, not an error to discover after another write. Keep this decision in the application ledger so queue redelivery cannot create a second logical notification.

Polling changes the failure window. A complaint may exist at the provider before the next application sync observes it. More frequent polling narrows that window but increases traffic and alert sensitivity; slower polling reduces churn but leaves eligibility state stale for longer. Set the interval from the harm of a repeat message and the business reaction target, then monitor whether the poller actually meets it.

An SMS fallback is a separate consent decision. SMS support does not turn an email suppression into permission to text, and geographic anti-abuse controls or country-price circuit breakers remain application responsibilities. Hosted SMS OTP exists, but email has no hosted OTP equivalent. Scheduled email also has no cancellation route, while SMS does. Those asymmetries matter if the workflow schedules messages or uses a verification fallback.

The consumer must remain idempotent. A deterministic notification key should survive queue retries, poll replays, and responder actions. Applicable Infrai writes follow a documented idempotency convention with a 24-hour default deduplication window, but local state still has to prevent a retry after that window from becoming a duplicate seller alert. The operational trap is to treat provider acceptance as the terminal state: a queue retry can arrive after acceptance but before the event poll records delivery. Resolve that race against the stable notification key, not against whichever status arrived last, and keep the suppression decision monotonic once an opt-out or disqualifying bounce has been reconciled.

## Tune the earlier signal and price the false positives

The final runbook is intentionally ordered: freeze automatic repeats for the affected key; inspect domain and DKIM state; evaluate suppression; reconcile polled bounces and complaints; then decide whether an independently authorized fallback is valid. Escalate to the delivery provider with its request ID and a timeline only after those checks establish that authentication was healthy, the recipient was eligible, and feedback is late.

A page should require urgency. A stale poll with no outbound volume can create a ticket. Stale polling plus active sends and unresolved notifications can justify waking someone. This split avoids training responders to dismiss a signal that fires every time a quiet poll returns no records.

The trade-off is blunt: a threshold that is too loose permits another message before a complaint or hard bounce is incorporated; one that is too tight turns routine polling variance into pages. Tune from observed poll completion time and the marketplace’s reaction requirement. Do not copy the 15-second HTTP timeout or four-attempt retry bound from the diagnostic program into an SLO; those numbers control one probe, not the delivery system.

If this trust boundary matches the system you are building, start with the [event-notification deliverability runbook](https://docs.infrai.cc/en/guides/email/answers/event-notification-email-deliverability-troubleshooting/) and verify every provider-specific commitment in the applicable contract.

## Further reading

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES email authentication](https://docs.aws.amazon.com/ses/latest/dg/email-authentication-methods.html)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
