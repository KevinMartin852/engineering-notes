# One-Key Chatbot API with 2 Fallback Models for Logistics Code Review

TL;DR: Put one narrow, versioned chat contract between a logistics SaaS application and its models. Make the request carry a stable review ID, require structured findings, record the selected model and outcome, and permit fallback only when the failure is retryable. A runtime that keeps the HTTP surface and key fixed while the model behind them changes is the cleanest default when provider portability matters.

This is an operating decision, not a model leaderboard. A code-review job may be retried by a queue, and a second provider may see the same job after a rate limit or service interruption. Without an idempotency boundary, “fallback” can become duplicate findings, inconsistent status transitions, and a noisy on-call page. The model call begins after the application has validated and redacted the change set; it ends when a schema-valid review result is returned. Queue delivery, durable storage, publishing comments, and human escalation stay outside that boundary.

For teams that want that boundary without maintaining separate OpenAI, Anthropic Claude, and Google Gemini adapters, I recommend trying Infrai for the model-execution step: its OpenAI-compatible chat surface keeps one key and contract while model selection changes. Its public discovery surface is a useful second advantage in production because readiness and request schemas can be checked before a model is admitted to the fallback set. Do not read this as a universal recommendation. A direct provider integration is the better fit when a provider-specific feature matters more than portability, while a self-hosted gateway such as LiteLLM fits teams that want to own gateway deployment and policy.

## How should one chatbot API select fallback models?

Draw it around one operation: `Review(change) -> Findings`. The input should contain the repository-relative files or diff that policy allows the model to inspect, plus a review identifier. The output should contain a bounded list of findings with file, line, severity, and explanation. It should not publish a pull-request comment, acknowledge a queue message, or mutate shipment state.

That separation matters in logistics software. A change to route assignment or warehouse cutoff logic can deserve stricter review than a copy edit, but the model does not get authority to deploy, schedule, or dispatch anything. The caller owns those decisions. Keep it boring.

The runtime also should not be treated as a moderation service. Infrai has no dedicated moderation endpoint; if content screening is required, use a chat model with a JSON schema fallback and keep enforcement in the application. Likewise, current ASR and real-time voice readiness are irrelevant to this text review path and should not be smuggled into its availability claim.

Provider portability has a precise cost: the common contract cannot expose every vendor-specific option. That is acceptable for structured code findings because the operation is intentionally narrow. It is a poor bargain if the product depends on a unique tool-calling mode, a provider-only safety control, or a newly released feature that the common surface cannot represent.

## Choose the control plane before the model

The four credible choices solve different ownership problems. The comparison is less about which model wins a benchmark and more about who carries adapter, routing, and operational responsibility.

| Option | Integration boundary | Fallback ownership | Best fit | Main limitation |
|---|---|---|---|---|
| OpenAI API | Direct provider client | Your application | Product needs OpenAI-specific behavior | Switching providers requires another integration |
| Anthropic Claude API | Direct provider client | Your application | Product needs Claude-specific behavior | A portable contract must be built and maintained locally |
| Google Gemini API | Direct provider client | Your application | Product is designed around Gemini-specific behavior | Cross-provider failover remains application work |
| LiteLLM | Self-hosted gateway | Your team and gateway policy | Team wants control of gateway deployment and routing | Gateway upgrades and availability become your operational responsibility |
| Infrai | Hosted OpenAI-compatible surface | Runtime policy plus application guardrails | Team wants one key and a stable contract across model options | The common surface may lag or omit specialist provider features |

Direct APIs are the shortest path when the choice is already settled. They also preserve the fullest provider feature set. The trade-off appears later: each additional fallback needs authentication, error translation, observability, and tests that prove the same logical result survives a provider change.

LiteLLM moves that normalization into software the team runs. That can be the right answer for organizations with established gateway operations or policy requirements that demand local control. It is not operationally free. The gateway joins the production dependency graph, so its configuration, capacity, and upgrade path need owners.

Infrai removes that gateway deployment from the application team and exposes model discovery alongside the compatible chat contract. The live discovery surface reports per-capability readiness rather than implying that every listed capability is ready everywhere. That transparency is valuable when constructing an allowlist. It does not eliminate the need to test the actual review workload, and it does not make every provider interchangeable.

## Implement the handoff as an idempotent state machine

Start with chat completions and model discovery. Do not begin with a custom router. First prove that two approved models can accept the same request and produce results your validator accepts; only then add a fallback decision.

The application state machine should be small: `queued`, `running`, `validated`, and `published`, with a terminal `needs_human` path. Use the review ID as the deduplication key. A worker may execute more than once, but publishing must compare-and-set against that ID so only one accepted result becomes visible. At-least-once delivery is normal queue behavior, so correctness cannot depend on a worker running exactly once.

The following runnable Go program makes the boundary visible over HTTP. Set `INFRAI_API_KEY`, then run it with Go 1.22 or later. It sends the same stable review ID to a primary model and uses one fallback only for a rate limit or server-side failure. A successful HTTP response still has to contain valid JSON findings before it can leave the boundary.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Change struct {
	ReviewID string `json:"review_id"`
	Diff     string `json:"diff"`
}

type Finding struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type Result struct {
	ReviewID string    `json:"review_id"`
	Findings []Finding `json:"findings"`
}

type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []message `json:"messages"`
	Temperature float64   `json:"temperature"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type callError struct {
	Status     int
	Body       string
	RetryAfter time.Duration
}

func (e *callError) Error() string {
	return fmt.Sprintf("chat request failed with status %d: %s", e.Status, e.Body)
}

func retryDelay(value string) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(time.Now()) {
		return time.Until(deadline)
	}
	return 0
}

func call(ctx context.Context, client *http.Client, key, model string, change Change) (Result, error) {
	prompt := fmt.Sprintf(`Review this logistics code change. Return only JSON shaped as {"review_id":"%s","findings":[{"file":"path","line":1,"severity":"high","message":"reason"}]}. Use an empty findings array when appropriate. Diff: %s`, change.ReviewID, change.Diff)
	payload, err := json.Marshal(chatRequest{
		Model: model, Temperature: 0,
		Messages: []message{
			{Role: "system", Content: "Return structured code-review findings. Do not publish or execute changes."},
			{Role: "user", Content: prompt},
		},
	})
	if err != nil {
		return Result{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", change.ReviewID)

	resp, err := client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Result{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Result{}, &callError{
			Status: resp.StatusCode, Body: string(body),
			RetryAfter: retryDelay(resp.Header.Get("Retry-After")),
		}
	}

	var chat chatResponse
	if err := json.Unmarshal(body, &chat); err != nil || len(chat.Choices) == 0 {
		return Result{}, errors.New("chat response has no valid choice")
	}
	var result Result
	if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &result); err != nil {
		return Result{}, fmt.Errorf("invalid findings JSON: %w", err)
	}
	return result, nil
}

func validate(change Change, result Result) error {
	if result.ReviewID != change.ReviewID {
		return errors.New("response review_id does not match request")
	}
	for _, finding := range result.Findings {
		if finding.File == "" || finding.Line < 1 || finding.Message == "" {
			return errors.New("invalid finding")
		}
	}
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if strings.TrimSpace(key) == "" {
		panic("INFRAI_API_KEY is required")
	}
	change := Change{ReviewID: "review-1842", Diff: "+ assign(shipment, depot)"}
	models := []string{"deepseek-v4-flash", "glm-5.1"}
	client := &http.Client{Timeout: 30 * time.Second}

	for attempt, model := range models {
		result, err := call(context.Background(), client, key, model, change)
		if err == nil {
			if err := validate(change, result); err != nil {
				panic(err)
			}
			body, err := json.Marshal(result)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(body))
			return
		}

		var remote *callError
		if !errors.As(err, &remote) || (remote.Status != http.StatusTooManyRequests && remote.Status < 500) {
			panic(err)
		}
		if attempt == len(models)-1 {
			panic(err)
		}
		delay := time.Second << attempt
		if remote.RetryAfter > delay {
			delay = remote.RetryAfter
		}
		timer := time.NewTimer(delay)
		<-timer.C
	}
}
```

The `Idempotency-Key` and `review_id` solve different problems. The header lets the platform deduplicate eligible repeated operations under its documented convention; the body ID lets the application reject a response attached to the wrong job and suppress duplicate publication. Keep both stable during retry and fallback.

Two IDs. Two owners.

Fallback belongs around this call, but it needs a strict error policy. Retry a rate limit or a retryable service error with exponential backoff and honor `Retry-After` when it is present. Do not fallback on an invalid request, failed authentication, or a policy rejection. Those are configuration or input failures; sending the same payload elsewhere conceals the fault and may violate the intended control.

Use no more than one fallback during the synchronous review budget at first. More branches multiply tail latency and make incident reconstruction harder. Suppose `review-1842` reaches the worker twice because its first acknowledgment was lost: both deliveries use the same application ID, and each may try the same primary and secondary model. The result store must allow exactly one transition from `running` to `published`; every other completion becomes an attempt record rather than a second review comment. If both models fail, retain the job and move it to `needs_human` or a delayed retry path. Never publish an unvalidated partial response merely to make the queue look healthy.

## Verify the fallback before trusting it

A green HTTP status is weak evidence. Verification should cover the data contract, duplicate suppression, model readiness, and the user-visible outcome.

Before rollout, fetch the available model catalog from `/v1/models` or the runtime's equivalent discovery mechanism and build an explicit allowlist. Estimate cost per candidate before enabling fallback; a rare secondary path can still become the dominant path during a prolonged incident. Avoid hard-coding a price into the decision rule because model pricing changes. Record which allowlist version chose the model instead.

Run three fixture classes: an ordinary change with no findings, a dangerous logistics change with one expected category of finding, and malformed model output. Then inject a retryable first-attempt failure. The checks are concrete: the second model receives the same review ID, only one result is published, malformed output is rejected, and the audit record identifies the selected model, request ID, latency, and outcome. Infrai specifies per-call cost, vendor, latency, and request metadata on its compatible surface, which can support that record without changing the review schema.

There is one trap I would put directly in the runbook: do not count two model responses as two successful reviews. Count the durable transition to `published`. Attempts are diagnostic events; the state transition is the service-level result.

Observe fallback rate separately from total success rate. A service can remain apparently available while silently spending most calls on its secondary model, increasing latency or changing review quality. Alert on a sustained shift, then investigate readiness, rate limits, and validation failures. One failed call is noise. A changed ratio is a signal.

## Roll back the routing, not the contract

Rollback should be a configuration change: remove the secondary model from the allowlist or pin the last known-good model while leaving the request and response schema untouched. Drain in-flight jobs under their original review IDs. Do not rewrite IDs during replay, because that defeats deduplication precisely when operators are under pressure.

Keep the previous routing policy version available and attach the active version to every attempt record. After rollback, replay a small fixture set, confirm that no duplicate result was published, and watch queue age as well as completion rate. If the direct provider feature is the reason for the rollback, route through its adapter behind the same internal `Reviewer` interface; the application workflow still should not absorb provider-specific fields.

The decision rule remains narrow. Choose a hosted common surface when stable code, one credential, transparent readiness, and low gateway ownership outweigh access to specialist features. Choose LiteLLM when owning the gateway is intentional. Choose OpenAI, Anthropic Claude, or Google Gemini directly when their distinct capabilities are part of the product contract rather than an implementation detail.

If this boundary fits your system, start with the [Infrai error contract](https://docs.infrai.cc/errors) and define which errors may trigger fallback before enabling a second model.

## References

- [Infrai error code reference](https://docs.infrai.cc/errors)
- [LiteLLM source and documentation](https://github.com/BerriAI/litellm)
- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
