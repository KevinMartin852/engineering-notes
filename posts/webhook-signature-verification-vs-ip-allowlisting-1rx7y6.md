# Webhook Signature Verification vs IP Allowlisting: Node.js Inbound Platform Events

## Short answer

Short answer: verify a signed webhook body first, then use IP allowlisting as a secondary network signal. For a gaming platform's leaked-key drill, signatures preserve attribution because they bind the exact bytes to a credential that can be rotated and audited; an IP address only says where a request appeared to come from. Accept an event only after the signature, timestamp, replay nonce, and account mapping pass, and record the evidence used for the billing decision.

The page that matters is not “webhook returned 401.” It is “premium match credits went to the wrong account.” In an incident drill, the on-call needs to explain which platform event created a charge, which key verified it, and whether the sender's network changed during the window. That is why I treat allowlists as a useful tripwire, never as the identity check. The ledger entry has to carry that chain of evidence through retries, queue handoff, and the eventual charge reconciliation; otherwise a clean HTTP response can still leave finance unable to prove who caused the debit.

No guesswork.

Keep the first line of the runbook blunt: reject before enqueueing. A request that cannot be authenticated must not enter the billing queue, where a later retry can make a bad attribution look legitimate.

## How should Node.js verify webhook signatures and IP allowlists for inbound platform events?

Read the raw request bytes, not a re-serialized JSON object. Compute an HMAC with the active secret, compare it in constant time, and require a bounded timestamp skew. Store a digest of the body, key version, source address, and verification result in an audit record. Then apply the IP policy as an independent signal. A matching address can't rescue a bad signature; a changed address should page the owner rather than silently change the identity rule.

Here is the small part of the receiver that should be boring. The caller has already read the body and parsed the provider's `t=` and `v1=` fields from its signature header.

```go
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

func Verify(body []byte, secret []byte, sentAt int64, providedHex string, now time.Time, maxSkew time.Duration) bool {
	if maxSkew <= 0 || now.Unix() < sentAt {
		return false
	}
	if now.Sub(time.Unix(sentAt, 0)) > maxSkew {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(sentAt, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	expected, err := hex.DecodeString(providedHex)
	if err != nil {
		return false
	}
	return hmac.Equal(mac.Sum(nil), expected)
}
```

The exact signed string and digest algorithm belong to the sender's published contract; the example shows the verification shape, not a universal header format. Rotate secrets by accepting the current and previous key versions for a measured overlap, and log which version succeeded. Never log the secret or the complete payload if it contains player data.

The IP check has a narrower job. Evaluate the address observed at the trusted edge, after proxy forwarding headers have been validated. Keep ranges in versioned configuration, test both IPv4 and IPv6 forms, and make a range change produce an audit event. An allowlist can catch a misrouted staging sender quickly. It cannot distinguish two tenants sharing a cloud egress address.

## What does the leaked-key drill prove about billing attribution?

Start with a fixture that represents the failure, not a happy-path delivery. Use one valid event for player `p-1842`, a duplicate delivery with the same event ID, a valid body signed by a retired key, and a body whose JSON fields were changed after signing. Add a request from an unlisted address and one from a listed address with an invalid signature. The expected result is deterministic: only the first valid event is eligible for billing, and every rejected case has a reason code.

I once saw a drill report “signature checks passed” while the test harness had parsed JSON and then marshaled it again before verification. Whitespace and key ordering were lost, so the test never exercised the production boundary. The fix was less clever: capture bytes at the HTTP handler, persist a SHA-256 digest, and make the fixture replay those bytes. One line of evidence beat a green dashboard.

Deduplicate on the sender's immutable event ID, but keep the signature evidence beside the deduplication record. A duplicate with a valid signature is not a new charge; a duplicate with a different body is a security event. The billing ledger should receive an internal event reference, not a client-supplied account name copied from an unverified payload.

Short receipts help.

They also make the postmortem shorter.

Emit `verified`, `key_version`, `event_id`, `body_digest`, `received_at`, `source_ip`, and `decision`. Put the reason for rejection in a controlled vocabulary such as `bad_signature`, `stale_timestamp`, `replay`, or `ip_policy`. This gives the incident commander enough material to reconcile charges without exposing player secrets.

## Where do signatures and allowlists fail, and what should replace them?

| Control | Strong signal | Failure boundary | Best use |
| --- | --- | --- | --- |
| HMAC signature | Proves possession of a shared secret for exact bytes | A leaked secret can authorize forged events until rotation | Primary authenticity check |
| IP allowlist | Detects unexpected network origin | Shared egress, proxies, and address churn weaken identity | Secondary anomaly signal |
| mTLS | Authenticates a client certificate at the transport layer | Certificate issuance and rollover become a new operational system | Private, tightly managed links |
| Signed event with asymmetric key | Verifies without sharing a signing secret | Key distribution and revocation need clear ownership | Many independent consumers |

The catch is replay. A correctly signed request can still be harmful when an attacker captures it. Timestamp windows, a nonce or event ID, and an idempotent ledger are separate controls. An IP allowlist does not solve replay, and a signature check does not tell you whether the event was already applied.

This design is not suitable when a sender cannot expose a stable signing contract or when the receiving edge cannot preserve raw bytes. In that case, isolate the endpoint, require mTLS or a mutually authenticated gateway, and make billing reconciliation manual until the identity evidence is trustworthy. Stick with an allowlist only for low-impact operational callbacks where misattribution cannot create money movement.

For the gaming drill, the decision rule is simple: no signature, no attribution; no unique event ID, no second charge; an IP mismatch is an alert and quarantine signal. The runbook should rehearse key rotation, replay, proxy changes, and queue retries together, because production incidents rarely respect one control's boundary.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rfc-editor.org/rfc/rfc8446
- https://nodejs.org/api/crypto.html
