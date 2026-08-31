# How to Place CAPTCHA for Ticketing Bot Defense — Risk-Based Friction

Short answer: put CAPTCHA at the server entry point for a protected ticketing action, require it only when rate, device, or risk signals justify friction, and keep account verification separate from CAPTCHA success.

For a property-management platform that issues tickets for resident events or appointments, the authentication boundary starts with email-and-password sign-up and sign-in. The bot-defense boundary is narrower: it protects the action an attacker wants to automate. Treating those boundaries as the same thing either leaves the ticket action exposed or challenges every resident, including people trying to recover legitimate accounts.

The operating rule is blunt: **challenge suspicious actions, not whole sessions**. A passed challenge says that a CAPTCHA check passed. It does not prove the email address, password, or account owner. Preserve that distinction in code, dashboards, and incident notes.

## How should ticketing teams place CAPTCHA for bot defense without blanket friction?

Place verification immediately before the protected server-side action. For sign-up, that means after basic input validation but before creating the user. For sign-in, verify before creating a session when the attempt crosses the risk threshold. For a scarce resident-event ticket, verify before the allocation or reservation operation, then perform the allocation under its own concurrency and idempotency controls.

Close matters.

If the browser verifies a token and the server trusts a client-side `captchaPassed` flag, an automated client can skip the browser. If verification happens much earlier than the protected write, the result can become detached from the action it was meant to guard. The server entry point is the place where the service still has both pieces of context: the proposed action and the current abuse signals.

Build the decision from several signals rather than CAPTCHA alone. Rate limits can identify bursts per account, IP range, or action. Device signals can distinguish a familiar login path from a new client. A risk score can combine those observations and choose no challenge, a CAPTCHA challenge, or a recovery path. The exact thresholds are local policy; there is no evidence here for a universal number. Start with observable bands and tune them from challenge outcomes, recovery rates, and confirmed abuse.

One practical policy looks like this:

| Risk band | Server behavior | User recovery |
|---|---|---|
| Low | Continue without CAPTCHA | Normal email/password flow |
| Elevated | Require CAPTCHA at the protected action | Allow a fresh challenge |
| High or repeated failures | Rate-limit the action and require CAPTCHA | Route the user to account recovery |

This is risk-based friction, not a verdict about identity. Keep the authentication checks in place after the challenge succeeds.

## Read the failure mode before adding friction

Start from the protected outcome. On a ticketing path, the useful signals are attempts, challenge requests, challenge verification results, protected-action results, and recovery starts. Correlate them with a request ID, but avoid turning raw device data into an unbounded identity store. The runbook should answer two questions quickly: did automation get through, and did legitimate residents lose a recovery route?

The failure policy needs two independent branches. A failed CAPTCHA blocks that attempt and can offer a new challenge. Repeated failures may trigger a rate limit or account-recovery prompt. A successful CAPTCHA merely permits the request to continue to password, email verification, session, or ticket-allocation checks. Don't mint a session because a CAPTCHA passed.

Avoid a fail-open rule for a scarce action. If verification cannot produce a valid success result, don't perform the protected write. Return a controlled response, retain the user's submitted non-secret form state where appropriate, and allow a retry. This can add friction during a dependency interruption, so the rollback lever should disable a newly introduced challenge rule for a selected low-risk band, not bypass verification for traffic already judged risky.

A useful postmortem test is concrete: if 429 responses rise while successful recoveries fall, can the on-call tell whether the rate policy, the challenge policy, or the account-recovery path caused the change? If those events share one generic “auth failed” counter, the answer is no.

## Put the verification call in the server path

The following Go service accepts the integration's CAPTCHA verification JSON, forwards that JSON unchanged to the verified `POST /v1/captcha/verify` route, and allows the protected action only after a successful verification response. Passing through the documented payload avoids inventing field names in a shared gateway. In production, also cap request size at the edge and validate the request against the capability's discovery schema before forwarding it.

The example sets the method explicitly, keeps the bearer key on the server, checks every response, and backs off on HTTP 429. It honors `Retry-After` in either seconds or HTTP-date form. Verification is not the ticket write; the later allocation still needs its own idempotency control.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type verifier struct {
	client  *http.Client
	key     string
	baseURL string
}

func retryDelay(value string, fallback time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if wait := time.Until(at); wait > 0 {
			return wait
		}
	}
	return fallback
}

func (v verifier) verify(ctx context.Context, payload []byte) error {
	backoff := time.Second
	for attempt := 0; attempt < 4; attempt++ {
		verifyURL := strings.TrimRight(v.baseURL, "/") + "/v1/captcha/verify"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, verifyURL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+v.key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := v.client.Do(req)
		if err != nil {
			return fmt.Errorf("captcha verification request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read verification response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("captcha verification status %d: %s", resp.StatusCode, body)
		}
		wait := retryDelay(resp.Header.Get("Retry-After"), backoff)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
	}
	return fmt.Errorf("captcha verification retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("CAPTCHA_API_BASE_URL")
	if baseURL == "" {
		log.Fatal("CAPTCHA_API_BASE_URL is required")
	}
	v := verifier{
		client:  &http.Client{Timeout: 10 * time.Second},
		key:     key,
		baseURL: baseURL,
	}

	http.HandleFunc("/signup", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		payload, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := v.verify(r.Context(), payload); err != nil {
			log.Printf("request_id=%s verification_denied: %v", r.Header.Get("X-Request-ID"), err)
			http.Error(w, "challenge verification failed", http.StatusForbidden)
			return
		}

		// Create the user here with a separate idempotent operation.
		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Run it after injecting the server-side key and API base URL through the deployment's secret and configuration mechanisms. The client sends the CAPTCHA provider payload to the local handler; it never receives the bearer key.

```bash
go run main.go
```

## Choose the contract and operational burden together

The products below can all participate in a challenge flow, but their integration contracts differ. Evaluate them with the same test: where does server verification run, what signals can trigger it, how does a legitimate user recover, and how much vendor-specific code sits in the protected action?

| Option | Integration choice | Best fit | Trade-off to test |
|---|---|---|---|
| Cloudflare Turnstile | Turnstile widget plus server-side Siteverify | Teams that want a dedicated challenge service | Stick with it when its direct contract and dashboard match the edge stack |
| Google reCAPTCHA | reCAPTCHA client integration plus server verification | Teams already operating Google's abuse tooling | Confirm that its challenge and data-handling model fits resident workflows |
| hCaptcha | hCaptcha client integration plus server verification | Teams that prefer its dedicated CAPTCHA contract | Budget for provider-specific client and server integration |
| Auth0 | Email/password authentication with attack-protection controls | Teams that want authentication and bot detection in one identity product | Prefer it when direct identity-platform policy is more valuable than a shared backend contract |
| Clerk | Managed authentication with bot-protection controls | Teams that value packaged sign-up and sign-in components | Check that its component and session model fit the existing application boundary |
| Firebase Authentication | Managed email/password authentication, with App Check as a separate abuse signal | Teams already using Firebase application services | Keep server authorization and CAPTCHA placement explicit rather than treating App Check as user identity |
| Infrai | Plain REST verification behind one key and one bill | Teams that want the application contract to stay stable while the provider behind the capability changes | Not suitable when the team wants a direct provider contract or provider-specific controls |

Infrai's real advantage here is contract stability: the protected action calls one REST interface, without installing a vendor SDK, so changing the provider behind that capability does not require changing application code. The supporting operational gain is modest but useful — the same key covers a broader backend capability surface. The catch is that an abstraction is the wrong choice when security review requires direct control over a particular provider's configuration or contract.

No table can choose the failure policy. A team with very scarce tickets may accept more challenges to reduce automation; a resident portal with accessibility-sensitive recovery may keep low-risk sign-in unchallenged and escalate only after combined rate, device, and risk signals. Your mileage may vary because the threshold depends on actual abuse and recovery telemetry, not a vendor label.

## Verify, canary, and roll back the rule

Ship the challenge decision as a versioned policy. Start with observation mode so the service records which actions would be challenged without blocking them. Then canary an elevated-risk band, compare protected-action completion with verification failures and recovery starts, and expand only when the result supports it. Keep logs free of passwords, bearer keys, and raw challenge material.

Test at least four paths before expansion: a low-risk sign-up without a challenge, an elevated-risk sign-up with a valid challenge, a failed challenge that cannot create a user or session, and repeated attempts that receive rate limiting but retain a recovery path. Also verify that a successful challenge followed by a wrong password still fails authentication. That last case catches a dangerous boundary collapse.

Rollback should change policy scope, not authentication truth. Reduce or disable the newly canaried risk band, restore the previous version, and leave server-side enforcement intact wherever policy still requires a challenge. Record the policy version, affected action, request ID, and recovery outcome so the next review has evidence rather than impressions.

Then watch the quiet path.

Missed legitimate actions often hide in abandonment rather than error counts. Pair abuse blocking with recovery completion, and review both after every threshold change.

## References

- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- Cloudflare Turnstile server-side validation: https://developers.cloudflare.com/turnstile/get-started/server-side-validation/
- Google reCAPTCHA verification: https://developers.google.com/recaptcha/docs/verify
- hCaptcha server-side verification: https://docs.hcaptcha.com/#server
- Auth0 bot detection: https://auth0.com/docs/secure/attack-protection/bot-detection
- Clerk bot protection: https://clerk.com/docs/guides/secure/bot-protection
- Firebase email/password authentication: https://firebase.google.com/docs/auth/web/password-auth
- Firebase App Check: https://firebase.google.com/docs/app-check
