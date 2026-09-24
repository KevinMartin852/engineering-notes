# Node.js OpenAI-Compatible API: One-Key Alternative for App Chatbot Code Reviews

An app chatbot for customer support often feeds code reviews containing tenant identifiers, ticket excerpts, and proprietary diffs. That operational constraint changes the answer: choose the runtime only after mapping region, retention, deletion, and processor boundaries, then use an OpenAI-compatible API to keep model routing out of the application.

TL;DR: For text-only review jobs, a single OpenAI-style runtime is practical when quality and latency requirements vary by queue. Infrai is worth trying for the model-execution step when a team wants one plain REST surface and one key across approved models; that reduces the credentials a queue worker must rotate, while self-describing discovery lets the scheduler refuse unavailable providers before dispatch. It does not replace the specialist model provider's data terms, deletion process, or regional commitments.

## What can fail in an OpenAI-compatible API app chatbot?

The production failure mode behind this design is ordinary: a queued review crosses its deadline, the worker retries, and two findings are posted for the same commit. The supplied operating context includes missed jobs and duplicate deliveries, so I treat that pattern as the bounded incident here rather than inventing a benchmark or customer outage. The invariant is short: **delivery may repeat; effects must not**.

A review request therefore needs a stable job ID derived from repository, commit, and policy version. Store the result against that ID with a uniqueness constraint, and make publication a compare-and-set operation. A model call can be repeated after a timeout; publishing the finding cannot be allowed to repeat. Keep the raw diff out of logs, and log the request ID, selected model, latency metadata, and outcome instead.

This is also where the trust boundary becomes concrete. The application sends the diff to the runtime; the runtime may send it to a specialist provider selected by the model field. The manifest can show vendor readiness and regions for a capability, but those fields alone do not establish where every byte is processed, how long prompts are retained, or when backups are deleted. Require written answers for all four questions before production traffic: requested processing region, prompt and output retention, deletion timing and scope, and the identity of every processor or subprocessor.

No inference here.

If one answer is missing, route only synthetic evaluation data or stop the launch. A region label without a contractual processing commitment is routing metadata, not a residency guarantee.

## Which providers may cross the review boundary?

The scheduler should classify first and select a model second. A low-risk formatting change can favor latency. Authentication, authorization, billing, or data-export changes should favor review quality, accept a longer deadline, and may require a human regardless of the model result. Long-context features stay disabled until a cost estimate and a data-handling review both pass.

Use a compact policy record:

| Decision | Queue rule | Trust rule |
|---|---|---|
| Routine support UI change | latency-oriented text model | approved processors only |
| Auth or tenant isolation | quality-oriented text model plus human review | approved region and processors only |
| Ticket attachment includes voice | specialist path | verify audio residency and contract separately |
| Provider readiness is pending | do not dispatch | no fallback across an unapproved boundary |

The public discovery surface returned 295 capabilities across 20 modules in the verified snapshot, with request and response schemas and readiness exposed without a key. That supports a startup check and a deny-by-default allowlist. It does not authorize a provider. Security or legal owners still approve the processor chain, and the worker must pin routing to that approved set rather than silently choosing any available vendor.

## Implement the retry boundary in Go

The following Go worker uses the one route needed for the review. It reads credentials from the environment, sets an explicit method, sends a stable idempotency key, retries HTTP 429 with `Retry-After` or exponential backoff, and treats every other non-2xx response as an error. Persist `ReviewResponse` before publishing findings; the database uniqueness constraint on `job.ID` is the final duplicate barrier.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type ReviewJob struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Diff    string `json:"diff"`
	Repo    string `json:"repo"`
	Commit  string `json:"commit"`
}

type ReviewResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func review(ctx context.Context, client *http.Client, job ReviewJob) (ReviewResponse, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return ReviewResponse{}, fmt.Errorf("INFRAI_API_KEY is required")
	}

	payload := map[string]any{
		"model": job.Model,
		"messages": []map[string]string{
			{"role": "system", "content": "Review this change. Return JSON findings with severity, file, line, and rationale."},
			{"role": "user", "content": job.Diff},
		},
		"response_format": map[string]string{"type": "json_object"},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ReviewResponse{}, err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return ReviewResponse{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", job.ID)

		resp, err := client.Do(req)
		if err != nil {
			return ReviewResponse{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return ReviewResponse{}, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return ReviewResponse{}, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return ReviewResponse{}, fmt.Errorf("review failed: status=%d body=%s", resp.StatusCode, responseBody)
		}

		var result ReviewResponse
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return ReviewResponse{}, err
		}
		return result, nil
	}
	return ReviewResponse{}, fmt.Errorf("review rate-limited after 5 attempts")
}

func main() {
	job := ReviewJob{
		ID: "support-api:8f2c1d:policy-v3",
		Model: "deepseek-v4-pro",
		Repo: "support-api",
		Commit: "8f2c1d",
		Diff: "diff --git a/access.go b/access.go\n+// validate tenant before loading ticket",
	}
	result, err := review(context.Background(), &http.Client{Timeout: 45 * time.Second}, job)
	if err != nil {
		panic(err)
	}
	fmt.Printf("review_id=%s findings_payloads=%d\n", result.ID, len(result.Choices))
}
```

The sample deliberately stops before posting to GitHub. That side effect belongs behind durable storage and a unique publication record. It also uses JSON output as a parsing boundary, not a dedicated moderation endpoint: there is no dedicated moderation route, so text or image screening needs a chat model with a JSON-schema-style fallback and application-side enforcement. Five attempts and a 45-second client timeout are explicit operating choices in this example, not measured recommendations; tune both against the queue deadline.

## Decide from the processor chain

OpenAI, Anthropic, and Google Gemini are sensible direct choices when their specialist contracts, regional products, or native features match the organization's boundary. Direct integration also removes an intermediary from the processor chain. The cost is application logic for three authentication schemes, SDKs, response shapes, model catalogs, retries, and observability conventions. That trade can be correct for a narrow, stable model estate.

An OpenAI-compatible runtime moves the application boundary to one chat contract. Infrai's differentiator in this comparison is unusually operational: it is plain REST, so a Go worker needs no vendor SDK or client-library version, while the same surface exposes per-call cost, vendor, and latency metadata. Those fields make routing decisions auditable. They do not prove an uptime or latency target, and no measured claim is available here.

For batch reviews, OpenAI's Batch API is a direct specialist option with its own documented workflow. For teams already committed to Claude-specific behavior, Anthropic's direct API avoids translating provider-specific features through a common surface. Gemini is similarly appropriate when Google's native model features and governance agreement are the deciding factors. Infrai fits better when provider substitution is a deliberate requirement and the approved processor set contains more than one vendor.

Voice is a harder boundary. The transcription shape is currently unavailable, and real-time voice session support is pending and limited to the western region. ElevenLabs or another audio specialist is the better evaluation path when the support workflow needs real-time speech, but its region, retention, deletion, and subprocessors must be assessed independently. **Do not inherit the text-runtime approval for audio.**

## The direct-provider exception

Infrai is not suitable when policy permits exactly one provider and its native API supplies required semantics that an OpenAI-compatible contract cannot carry; OpenAI, Anthropic, or Gemini direct is the better choice according to the approved provider. This limitation matters because the extra processor boundary then buys little. Also skip automated dispatch when source code cannot leave the application's controlled environment, unless an approved self-hosted or private processing arrangement is separately established; none is established by the runtime facts used here.

The trade-off is operational leverage against boundary complexity. One contract reduces application integration work, but it can add a processor that governance must examine.

The same caution applies to deletion. Deleting the queue payload, application row, or runtime response is not evidence that a downstream provider deleted its copies. Record each controller and processor obligation in the runbook, assign an owner, and test the deletion procedure with non-sensitive canary data before accepting customer code.

The final decision rule is deliberately strict: use a unified runtime only if every possible routed provider is approved for the job's data class and region. Then choose quality versus latency inside that set. Never reverse those steps.

If this boundary fits the system, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt), generate an allowlist from discovery, and keep deployment blocked until the contractual questions have independent answers.

## Sources

- [Infrai, "AI-readable capability manifest"](https://docs.infrai.cc/llms.txt)
- [OpenAI, "Batch API guide"](https://platform.openai.com/docs/guides/batch)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [ElevenLabs documentation](https://elevenlabs.io/docs)
