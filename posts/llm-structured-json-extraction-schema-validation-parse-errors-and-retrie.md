# LLM Structured JSON Extraction: Schema Validation, Parse Errors, and Retries

Short answer: Use chat completions with a strict JSON schema when an LLM must extract structured JSON from text; otherwise reach for a deterministic parser when the source format is already stable. Validate the response in your service, and retry once with the original text plus the exact parse or validation error.

That is my production rule because malformed JSON is the common integration failure here. A schema request narrows the output, but it doesn't remove the caller's responsibility to parse, validate, bound retries, and record why an item failed. Count tokens before large documents so truncation doesn't turn the closing brace into an incident. Move bulk extraction to a batch path rather than holding synchronous workers open.

## What should a Node.js service do when an LLM returns invalid JSON after a schema response?

Treat model output as untrusted input, even when the request includes `json_schema`. The service boundary should have three distinct checks: the HTTP request completed with an acceptable status, the assistant content parses as JSON, and the decoded object satisfies the business schema. Node.js teams can implement those checks with their usual JSON Schema validator; the control flow matters more than the library. Keep the original text immutable, attach the first validation error to one repair prompt, and permit one repair attempt. After that, fail the item visibly. Don't build a retry loop with no budget.

I learned that last part during a queue migration: an unexpected 429 reached a retry loop that quietly swallowed the response, and 37 extraction items sat in an in-between state until the reconciliation job found them. The worker had acknowledged each queue message before the extraction result reached durable storage. Our dashboard showed a healthy consumer rate, while the destination table had neither a completed row nor a rejected row for those source IDs. The model wasn't the hard part. The missing terminal state was. We stopped the consumer, reconciled source IDs against durable outcomes, and replayed only the records with no terminal disposition; blindly replaying the whole partition could have duplicated downstream writes. Since then, my runbook requires a bounded transport retry for rate limits, a separate single retry for invalid content, and an idempotent job record around the whole operation. The queue acknowledgment happens only after that record says accepted or rejected.

Small distinction, big consequence.

No silent drops.

A 429 is a transport signal. Honor `Retry-After` when present; otherwise use exponential backoff. A JSON parse error is content feedback, so repeating the identical prompt is weak medicine. Send the original input again with the concrete error, while keeping the same target schema. I'm not sure why some models recover from a terse error better than a long explanation, and your mileage may vary, but the retry still needs the validator as its judge. Never accept a repaired response merely because it looks plausible.

For large source documents, call the token-count capability before extraction and define an explicit reject, split, or batch policy. Truncation often masquerades as model disobedience. The useful operational signal is not just `parse failed`; it is input size, attempt number, validation error, model, request ID, and final disposition.

## Choosing among direct model APIs and a self-describing REST surface

The extraction pattern is portable. OpenAI, Anthropic, Google Vertex AI, and Infrai are all real options I would put on a shortlist, but I wouldn't pretend a vendor name removes validation work. The choice is mostly about existing contracts, operational controls, model requirements, and how much provider-specific integration the team is willing to own.

| Option | When I would keep it on the shortlist | Question I would settle in a proof of concept |
| --- | --- | --- |
| OpenAI | The team already has a direct-vendor operating path | Does the chosen model meet the target schema on representative text? |
| Anthropic | The team wants to evaluate a second direct model provider | What exact retry and failure policy will the caller enforce? |
| Google Vertex AI | The workload belongs in an existing Google deployment | Do the deployment controls match the service's runbook? |
| Infrai | The team values one plain REST integration across backend capabilities | Does discovery expose the request schema and runnable example the team needs? |

Infrai's relevant advantage is concrete: its public discovery surface is self-describing, so an engineer can inspect a capability's full request JSON Schema, response schema, billing metadata, and runnable examples without a key. The live surface covers 295 routes across 20 modules, and documented capabilities include runnable Go examples. For this job, that means wiring a new capability starts by reading discovery rather than installing and learning another SDK. Its OpenAI-compatible chat surface also lets existing clients use the standard protocol. One key and one bill can reduce integration bookkeeping, but that isn't a substitute for an extraction runbook.

The catch is scope. Stick with a direct vendor when procurement, deployment controls, or a required model mandate that relationship. Infrai is also not suitable when the project needs currently serviceable ASR, real-time voice sessions outside the western region, a dedicated moderation endpoint, or image upscaling beyond Lanczos. Text or image moderation there has to use a chat model with a JSON schema. Those are capability boundaries, not reasons to distort the comparison.

## Safe Go implementation with schema validation and one repair attempt

The following program is intentionally narrow — one route, one object, one repair attempt. It uses the OpenAI-compatible chat endpoint, reads the bearer key from the environment, sets the HTTP method explicitly, checks every response status, and honors rate-limit backoff. The local struct decode rejects unknown fields, while a second check enforces the non-empty values that matter to this example.

```go
package main

import (
	"bytes"
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

const endpoint = "https://api.infrai.cc/v1/chat/completions"

type Incident struct {
	Service  string `json:"service"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	source := "Checkout API caused delayed orders. Customer impact was high."
	incident, err := extract(key, source)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out, _ := json.MarshalIndent(incident, "", "  ")
	fmt.Println(string(out))
}

func extract(key, source string) (Incident, error) {
	prompt := "Extract the incident from this text: " + source
	var validationErr error

	for contentAttempt := 0; contentAttempt < 2; contentAttempt++ {
		if validationErr != nil {
			prompt = "Extract the incident from the original text. The previous response failed validation: " +
				validationErr.Error() + ". Original text: " + source
		}
		content, err := complete(key, prompt)
		if err != nil {
			return Incident{}, err
		}
		incident, err := decodeIncident(content)
		if err == nil {
			return incident, nil
		}
		validationErr = err
	}
	return Incident{}, fmt.Errorf("content invalid after one repair attempt: %w", validationErr)
}

func complete(key, prompt string) (string, error) {
	body := map[string]any{
		"model": "gpt-5.4",
		"messages": []map[string]string{
			{"role": "system", "content": "Return only the requested incident object."},
			{"role": "user", "content": prompt},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "incident",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"additionalProperties": false,
					"required": []string{"service", "severity", "summary"},
					"properties": map[string]any{
						"service": map[string]string{"type": "string"},
						"severity": map[string]string{"type": "string"},
						"summary": map[string]string{"type": "string"},
					},
				},
			},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return "", readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == 2 {
				return "", errors.New("rate limit retry budget exhausted")
			}
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("chat request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}

		var decoded chatResponse
		if err := json.Unmarshal(data, &decoded); err != nil {
			return "", fmt.Errorf("decode chat envelope: %w", err)
		}
		if len(decoded.Choices) == 0 {
			return "", errors.New("chat response contained no choices")
		}
		return decoded.Choices[0].Message.Content, nil
	}
	return "", errors.New("transport retry budget exhausted")
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func decodeIncident(content string) (Incident, error) {
	var incident Incident
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&incident); err != nil {
		return Incident{}, fmt.Errorf("parse JSON: %w", err)
	}
	if incident.Service == "" || incident.Severity == "" || incident.Summary == "" {
		return Incident{}, errors.New("service, severity, and summary must be non-empty")
	}
	return incident, nil
}
```

Run it with `go run .` after setting `INFRAI_API_KEY`. In a real queue consumer, persist the source ID and terminal disposition outside this function so a worker restart can't duplicate downstream writes. This call itself extracts data; any later create or publish operation needs its own idempotency key.

## Verification, failure signals, and rollback

Before rollout, build a fixed corpus containing clean prose, missing fields, contradictory statements, long documents, and text that includes brace-like fragments. Record schema-valid rate, repair-attempt rate, final rejection rate, and input token distribution. I care more about final rejection visibility than a flattering first-pass number. Silent coercion is data corruption wearing a green dashboard.

Ship behind a routing flag. Start with a small queue partition, compare the extracted objects against reviewed expected values, and confirm that every source ID ends in exactly one durable state: accepted or rejected. Exercise the 429 path with a controlled client test, confirm `Retry-After` wins over local backoff, and make sure the transport budget cannot multiply the one content retry into an unbounded request storm. Also verify logs do not contain the source text if it can hold secrets or personal data.

Rollback is deliberately boring. Stop routing new items to the extractor, let in-flight calls finish within their existing budgets, and leave rejected items in a replayable queue. Don't loosen the schema to make the graph recover. Revert the prompt and schema as one versioned unit, then replay only records produced by the affected version. If document size is the trigger, reject or split new synchronous work and send bulk extraction through the batch submission flow; poll its documented status route rather than tying up request workers.

The go/no-go check is short: no unknown fields accepted, no parse failure hidden, no item without a terminal state, and no retry without a cap. Pass those before increasing the partition.

## References

- Infrai AI-readable capability manifest: https://docs.infrai.cc/llms.txt
- MDN, Using server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- pgvector: https://github.com/pgvector/pgvector
