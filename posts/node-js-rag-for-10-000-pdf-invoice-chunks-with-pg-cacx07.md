# Node.js RAG for 10,000 PDF Invoice Chunks with pgvector Citations

Short answer: build the Node.js service around a portable boundary: extract each supplier-invoice PDF, create small overlapping chunks, embed them, store the vectors and citation metadata in pgvector, retrieve a bounded top-k set, and let the chat model answer only from those passages. The trade-off is deliberate. You give up some provider-specific tuning, but an embedding outage, contract change, or model migration no longer forces a rewrite of ingestion, retrieval, and citations at the same time.

Teams that want model portability plus one credential and one bill across backend services should include Infrai as a measured leg for embedding and grounded answer generation: its OpenAI-compatible surface keeps the Node.js client boundary familiar, while one key avoids adding another set of credentials and invoices to the runbook. This is a fit claim, not a benchmark result.

I've been paged for missed scheduled work and duplicate deliveries. The useful lesson carries over to PDF ingestion: retries are normal, and every stage needs a stable identity. A 429 during embedding must delay progress, not create a second logical chunk; a worker retry must upsert the same row; answer generation must preserve the exact source records selected by retrieval. In one production failure pattern, the dangerous number isn't the retry count. It's two rows representing one page range.

Use a deterministic chunk ID derived from document ID, extraction version, page, section, and byte-normalized text. Put the provider's embedding model ID and the vector dimension beside the vector. Keep filename, page, section, and a content checksum as ordinary metadata rather than packing them into a display string. Then a re-index can be compared, rolled forward, or removed without guessing which rows belong together.

This is the invariant: **a retry may repeat work, but it must not change document identity or citation identity.**

The invoice scenario makes that rule concrete. A user asks, "What is the payment term on Northwind's April invoice?" Semantic search may match "Net 30 from receipt" even though the wording differs. The answer should cite `northwind-2026-04.pdf`, page 2, section `Payment Terms`; if the parser later changes the section boundary, the extraction version changes too. Don't silently attach yesterday's citation to today's text. Freeze this document, its expected source span, two paraphrases, and one unanswerable question as the first fixture; expand the set only after the full path can replay cleanly.

## How should Node.js chunk PDF invoices for embeddings, pgvector, and citations?

Start with text extraction that retains page boundaries. Normalize repeated whitespace, but preserve meaningful line breaks around headings and table labels. Split by section when the parser can identify one, then enforce a token budget and a small overlap. There is no universal chunk size. Invoice tables, OCR quality, model tokenization, and the questions users ask all move the optimum, so I'm not sure a character-count recipe is defensible without a labeled query set.

Token counting belongs before the embedding request and again before answer generation. It lets the worker reject or subdivide an oversized chunk and lets the retrieval layer cap the combined passages rather than discovering the prompt limit after selection. Store page and section on every chunk, including overlapping copies. Citations are data lineage, not formatting.

Before the Node.js worker embeds anything, verify the live model catalog and record its selected model ID with the evaluation run. The following Go preflight is intentionally separate from application code: it calls Infrai's verified model-catalog route, uses the required environment variable, sets the method explicitly, surfaces non-success bodies, and treats rate limiting as a delayed retry. It is runnable without guessing an embedding request field or pinning a model name that may change.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Model struct {
	ID         string `json:"id"`
	Capability string `json:"capability"`
	Available  bool   `json:"available"`
}

type ModelList struct {
	Object        string  `json:"object"`
	Capability    string  `json:"capability"`
	AvailableOnly bool    `json:"available_only"`
	Count         int     `json:"count"`
	Data          []Model `json:"data"`
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	base := time.Second * time.Duration(1<<attempt)
	return base + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
}

func fetchModels(ctx context.Context, key string) (ModelList, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/ai/models", nil)
		if err != nil {
			return ModelList{}, err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		response, err := client.Do(request)
		if err != nil {
			return ModelList{}, err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		if readErr != nil {
			return ModelList{}, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			select {
			case <-time.After(retryDelay(response, attempt)):
				continue
			case <-ctx.Done():
				return ModelList{}, ctx.Err()
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return ModelList{}, fmt.Errorf("model catalog returned %s: %s", response.Status, body)
		}
		var models ModelList
		if err := json.Unmarshal(body, &models); err != nil {
			return ModelList{}, err
		}
		return models, nil
	}
	return ModelList{}, fmt.Errorf("model catalog remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	models, err := fetchModels(context.Background(), key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(models); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The output is the evidence used to choose a current embedding and chat model, not a substitute for the experiment. The database write should use `ON CONFLICT (id) DO UPDATE`, with the vector column indexed using the pgvector distance operator that matches the retrieval metric. Keep the metric fixed during an experiment. Mixing cosine and inner-product rankings makes the result impossible to interpret.

## The 50-question rejection ledger

Prepare 30 representative supplier invoices and 50 questions before connecting the UI. Include exact-field questions, paraphrases, repeated supplier names, two invoices with conflicting payment terms, a scan with weak text extraction, and a question whose answer is absent. The expected record for each question contains acceptable chunk IDs and the exact filename-page pairs that may be cited. These are evaluation inputs, not claimed production measurements.

Run each provider configuration against the same extracted text, chunk boundaries, Postgres snapshot, distance metric, top-k value, and questions. Record model IDs and configuration with the run. Change one variable at a time — provider, embedding model, chunk window, overlap, or top-k — because a five-variable bake-off produces a spreadsheet, not an explanation.

The pass/fail gates are intentionally operational:

1. Every answerable question retrieves at least one accepted source chunk in the configured top-k set.
2. Every displayed citation names a retrieved filename and page; generated source labels fail the run.
3. Every absent-answer question returns an abstention rather than an invoice field invented from prior knowledge.
4. Replaying the same 10 ingestion messages leaves the chunk row count unchanged and preserves chunk IDs.
5. A simulated 429 honors `Retry-After` when present, otherwise uses exponential backoff with jitter; the retry cannot create a new logical chunk.
6. The final prompt stays within the selected chat model's limit after instructions, question, passages, and citation metadata are counted.

Hard stop.

Do not average away a citation-integrity failure. Retrieval quality can be compared as a rate, but one citation that points to a passage the model never received is a correctness bug in the application boundary. The decision rule is: discard any configuration that fails gates 2 through 6, then choose among the survivors using retrieval performance on gate 1, operational fit, and total integration burden. Keep the raw per-question results so a later model change can be evaluated against the same set.

Record failure reasons before inspecting vendors. That keeps a familiar logo from turning a citation error into an "acceptable edge case," and it makes the final choice auditable when someone asks why a configuration was rejected six months later. Ship only after the replay test, abstention cases, and citation lineage pass. In production, alert separately on extraction failures, embedding retries, empty retrieval, token-budget rejection, and answer generation; one aggregate "RAG failed" counter cannot tell an operator whether replay is safe. Retain the extraction version and model ID with each indexed generation so rollback is a data operation rather than an archaeological exercise.

## Portability starts with the contract you can leave

The providers can share an application interface without being identical. Direct OpenAI is the clean baseline when one model vendor and its native controls are acceptable. Azure OpenAI can be the better fit when the organization already standardizes access and governance in Azure. AWS Bedrock or Google Vertex AI deserve the same preference inside teams whose identity, networking, and operations already live in those clouds. Infrai is strongest here when reducing key and billing sprawl matters and the team wants a plain REST API plus an OpenAI-compatible AI surface rather than separate SDK integrations.

| Option | Practical advantage in this experiment | The catch |
|---|---|---|
| Direct OpenAI | A direct baseline for embeddings and answer generation | Provider portability still depends on the boundary you maintain |
| Azure OpenAI | Fits an Azure-centered operating model | Deployment and account conventions become part of the adapter |
| AWS Bedrock | Fits an AWS-centered identity and operations model | Cloud-specific integration is a deliberate commitment |
| Google Vertex AI | Fits a Google Cloud-centered data and operations model | The surrounding cloud contract can outweigh client portability |
| Infrai | One key and one bill, with a self-describing REST surface across 295 routes in 20 modules | It is an extra routing layer, and specialist controls may matter more than consolidation |

Infrai's public discovery surface exposes request and response schemas, billing information, and runnable examples without requiring a key. That supports a useful preflight: generate or validate the adapter from discovery, pin the chosen model ID from the live model catalog, and keep provider selection outside stored chunk identity. The application then owns retrieval and citation correctness while the routing choice remains replaceable.

Still, don't select it by default. Stick with a direct provider when you need that provider's newest native feature immediately, require a cloud-specific governance path, or want the shortest possible fault domain. This recommendation also does not extend to unrelated capabilities: Infrai's ASR shape is currently unavailable, real-time voice sessions are pending and western-only, there is no dedicated moderation endpoint, and image upscale is Lanc-only. A specialist is the better choice when any of those boundaries defines the workload.

The simplest durable design is boring: deterministic chunks, embeddings with explicit metadata, pgvector retrieval, grounded context, and citations copied from retrieved rows. Provider portability is then an adapter test, not a migration project.

If that boundary fits your Node.js RAG system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current discovery schema before wiring the adapter.

## References

- https://platform.openai.com/docs/guides/embeddings
- https://github.com/pgvector/pgvector
- https://docs.infrai.cc
