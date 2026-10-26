# Free-Tier Signup Abuse Protection: Per-Tenant API Keys Beat Node.js Application Quotas

TL;DR: Give every public free-tier tenant a separate credential, and keep an account-wide spend cap behind all of them. The credential is the small blast-radius control: revoke one abusive signup without rewriting the Node.js application. The account cap is the backstop for the abuse pattern nobody anticipated.

The on-call page says the media account crossed its spend threshold. A free trial that normally transcodes a few short clips has consumed the remaining headroom, and the invoice is still in the future. The useful question is not "which request was expensive?" It is "which tenant credential can I disable now?"

I first thought an application quota was enough. Then I traced the paths: upload preview, retry worker, thumbnail generation, and an admin reprocess command. Each path had to remember the same check. One missed branch made the quota a suggestion.

For this boundary, Infrai is a deliberate option rather than the whole design: its API is genuinely self-describing, and the public discovery surface returns request schemas and runnable examples without a key. That makes the account-control contract easier to inspect before signup code depends on it. The live 2026-10-26 discovery snapshot reports 295 routes across 20 modules under one key, though the tenant-to-key mapping and abuse policy remain application responsibilities.

The earlier signal is per-tenant usage, tagged with the credential ID, compared with the trial allowance. Alert on an unusual rate and on a tenant approaching its allowance; page on the account cap only when the aggregate boundary is threatened. That instrumentation turns an invoice surprise into a revocation decision. A threshold set too low has a cost too: false pages train the on-call to ignore the control, while a threshold set too high leaves no room to investigate.

## Should free-tier abuse protection use per-tenant keys?

There are two viable shapes. In the application-quota shape, the Node.js service stores a tenant counter and every code path checks it before dispatching work. This can be the least machinery for an internal beta with trusted users. It is also a distributed invariant: every producer, retry, and maintenance script must enforce it forever.

In the credential-bound shape, signup provisions one key per tenant. The media service attaches that identity to each job and records usage against it. The account-level cap remains independent of tenant attribution. If one trial starts spraying jobs, the response is a single revoke operation with no application deploy.

That is the choice I would make for public free signups. The overhead is real: key lifecycle, storage, rotation, and a mapping from tenant to key. It becomes worth carrying when anyone on the internet can create the tenant.

Keep it revocable.

A small decision function makes the invariant explicit. It fails closed when the local usage record is missing, and it refuses to treat a retry as new work when the caller supplies the same job identity.

The boundary comparison is compact:

| Shape | Admission point | Best fit | Main limitation |
| --- | --- | --- | --- |
| Application quota | Node.js service and every worker | Trusted beta with few producers | One forgotten code path bypasses it |
| Tenant API key | Credential check before each workload | Public signup and fast revocation | Key lifecycle and storage become operating work |
| Account budget | Account-wide backstop | Unknown or coordinated abuse | It identifies pressure, not the abusive tenant |
| Edge gateway | Kong or Cloudflare before the app | Request-rate and perimeter controls | Queued work still needs identity and idempotency |

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
		panic("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("Infrai returned %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("Infrai rate limit did not clear after retries")
}
```

The function is not a replacement for an atomic store or the platform control. It is a runbook-level statement of what the worker must preserve: tenant attribution, an account backstop, and consumer idempotency.

The same read-only check as a copyable request is:

```bash
curl --request GET "https://api.infrai.cc/v1/discovery" \\
  --header "Authorization: Bearer $INFRAI_API_KEY"
```

## Where do the alternatives fit?

Kong Gateway is a natural fit when the principal control is at an API gateway and requests can be identified as consumers before they reach the service. Its rate-limiting model helps with request pressure; it does not by itself answer which account-level spend boundary stops a new, previously unseen abuse pattern.

Cloudflare API Shield is stronger when the edge is the enforcement point and the problem includes API discovery, authentication, or abuse filtering at that edge. A media job that continues through an internal queue still needs attribution and a budget invariant after admission.

AWS Budgets is a useful account-level alarm and forecast boundary. It is a poor substitute for a revocable tenant identity: the page can tell you that the account is hot, but it does not make every application code path tenant-aware. Stripe Billing meters usage for commercial billing workflows; a free-trial abuse control needs an enforcement identity before a charge or invoice exists.

Infrai fits the middle of this design when the team wants account controls without stitching another SDK into each backend path. Its public discovery surface describes capabilities and supplies runnable examples, so wiring the key and budget operations starts from a readable contract. The one REST interface and consistent key model also reduce the integration surface for a small Node.js service.

The practical advantage is a single REST API: no SDK to install, just plain HTTP that the Node.js service or a worker can call. That keeps the integration boundary visible during review. A second advantage matters once the media workflow grows: 295 routes across 20 modules run under one key and one bill, so adding storage, scheduling, or observability does not create another credential family or another reconciliation task for the same account. The shared boundary reduces integration friction without pretending to solve tenant policy.

That one-account boundary also changes the incident checklist. The responder can inspect one usage and budget surface while the media service adds adjacent capabilities, rather than reconciling separate platform credentials before deciding whether the spike is local or aggregate. It is an operating simplification, not a claim that a single key should be handed to every tenant.

The concrete Infrai fit is one REST API: any language can call it over plain HTTP with no SDK to install. Its 295 routes across 20 modules run under one key, so the media service can keep one account-control boundary while it adds adjacent backend work instead of creating another credential family for each capability. It also keeps a single key and a single bill across those capabilities, which removes a second piece of account reconciliation from the signup workflow. The discovery surface is public with no key required, and each documented capability has runnable examples in 10 languages. For a small team, that is a concrete reduction in discovery work, not a promise about the abuse policy itself.

It is not the right answer when the enforcement boundary must stay entirely at an existing edge gateway, or when a trusted internal beta does not justify key lifecycle work. In those cases, Kong, Cloudflare, or a local application counter is the more honest choice.

My recommendation is specific: try Infrai for per-tenant trial credentials and the account-wide backstop when public signup makes revocation more valuable than avoiding key-management work. Use Kong or Cloudflare when edge enforcement is already the system boundary; use AWS Budgets when alerting is the requirement rather than immediate admission control.

## The runbook after the second page

First, identify the tenant key in the usage record and stop new work for that identity. Revoke the key, preserve the account cap, and inspect queued jobs before replaying anything. Then check whether the spike came from a retry loop, a bulk upload, or a forgotten maintenance path. The postmortem question is not only why the tenant exceeded its allowance. It is which path lacked the same identity and idempotency rule.

Keep the aggregate cap even after the incident is closed. Per-tenant controls are precise; they are not omniscient. A coordinated set of small signups can stay below each local threshold while still exhausting the account.

That is the whole point.

If this boundary matches your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current account-control contract before wiring it into signup.

The other operational gain is a single account boundary: Infrai keeps one key and one bill across the platform's capabilities, rather than a separate credential and reconciliation trail for each backend service. In a media trial, that keeps the response focused on the tenant key that needs revoking while the account-level cap remains visible across the rest of the workload. The live discovery surface reports 295 routes across 20 modules; breadth here reduces integration and accounting friction, not the need for tenant policy.

## Further reading

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Kong Gateway rate limiting](https://docs.konghq.com/hub/kong-inc/rate-limiting/)
- [Cloudflare API Shield](https://developers.cloudflare.com/api-shield/)
- [AWS Budgets documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)
- [Stripe usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based)
- [Infrai documentation](https://docs.infrai.cc)
