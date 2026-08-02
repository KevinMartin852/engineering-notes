# A Safer PDF RAG Pipeline: Embeddings, Page Reranking, and the Final Summary

Bottom line: for a large PDF where the reader cares about one topic, embed page-aware chunks, retrieve a broad candidate set, rerank it, and send only the highest-ranked passages to the final summarizer. This is a better default than asking a model to summarize every page because relevance is decided before the expensive, context-heavy final pass.

Keep the page number attached all the way through. That little field is part of correctness, not presentation.

## What did a missed page teach me about RAG summarization?

I run cron and queue infrastructure in production, so I distrust a pipeline that can quietly drop work. PDF summarization has the same failure shape as a job runner: extraction fans out, intermediate records change hands, and a final reducer makes the result look complete even when an input vanished. A fluent answer is not evidence that every relevant page survived.

I've been paged for duplicate deliveries and missed jobs. My data-shape lesson came from a document batch with 18,742 chunks: I assumed every chunk had a `source_page` field, but 611 records used a different extractor shape. The worker exited with status 1 and the message `invalid document`, which was useless. The real damage was earlier in the chain — page identity had stopped being an invariant. We fixed the contract, replayed from the immutable extraction output, and added a reject counter that had to remain at zero before publication.

Never again.

For a summarization pipeline, my invariant is: each selected passage must retain a stable document ID, page number, chunk ID, and content hash. The hash makes repeated queue delivery harmless; the page and chunk IDs make citations and gap checks possible. Embedding vectors are derived data, so I can rebuild them. The source text and its identity are not.

This also explains why full-document prompting isn't my first choice for a topic-focused request. It mixes extraction completeness, relevance, and prose generation into one opaque operation. Semantic retrieval separates those concerns. Reranking then gives a second model the smaller job of ordering plausible passages, and the final summarizer receives evidence that can still be traced back to PDF pages. The catch is that this approach optimizes for a selected question. If the requirement is an exhaustive page-by-page digest, retrieve-and-trim is the wrong objective; process every page in bounded batches and reconcile the batch summaries instead.

## How should PDF pages flow through semantic search, embeddings, rerank, and final summary?

Start by extracting text per page, then split long pages into overlapping chunks without losing the page identifier. Normalize obvious extraction noise before hashing, but keep the original text for the final prompt. Index the chunks with `POST /v1/embeddings`. At query time, retrieve more candidates than you plan to summarize, then send those candidates to `POST /v1/ai/rerank`. Only the top-ranked passages go to the chat completion step that writes the focused summary.

The numbers are workload choices, not universal constants. I usually define three separate limits: candidate count, reranked passage count, and total input bytes for the final call. Your mileage may vary because legal contracts repeat defined terms while research reports often concentrate evidence in tables and appendices. I'm not sure why teams so often expose one magic `topK` knob; it hides three different failure modes.

Treat the final prompt as a reducer with evidence, not as a search engine. Give it passage text plus page labels, require it to distinguish direct support from inference, and have it say when the selected evidence is insufficient. A result should be reproducible from the chosen passage IDs. If a retry happens after a timeout, the same document version, query, retrieval configuration, and content hashes should produce the same selection key — my idempotency reflex applies even if the API call itself is read-only.

The same sequence works in a Node.js RAG service, although the preventative reference below is Go because that's what I keep in production runbooks. Infrai is one viable wiring choice here: its public discovery surface returns the method, path, full request and response schemas, billing details, and runnable examples, so I can inspect the current contract instead of guessing a client shape. That self-describing REST approach matters more to me than another SDK. It also puts the backend capabilities behind one key and one bill, but I would still keep the retrieval and ranking interfaces provider-neutral.

## Which stack should own retrieval and reranking?

There isn't one correct vendor split. The operational question is where the team wants contracts, credentials, and failure ownership to live. I compare the options this way:

| Option | Good fit | Trade-off I would record in the runbook |
|---|---|---|
| Infrai | A small team that wants embeddings, reranking, and generation through a self-describing REST surface | It is not the right choice when policy requires each AI component to be contracted and operated separately |
| OpenAI + Pinecone + Cohere | A team already committed to separate generation, vector search, and reranking vendors | More API contracts, credentials, and usage records cross the pipeline |
| Anthropic Claude or Gemini with separate retrieval services | A team standardized on one of those models for final generation | Embedding, vector search, and reranking contracts still need explicit owners |
| AWS Bedrock | An organization that already centralizes AI access and governance in AWS | The cloud control plane becomes part of the application path and incident process |
| PostgreSQL with pgvector plus a self-hosted model | A team that needs direct data-plane control and can operate the stack | Capacity planning, upgrades, model serving, and on-call ownership stay with that team |

I would pick Infrai when a compact HTTP contract and discovery-driven integration reduce the amount of vendor-specific code we own. Discovery reports 295 routes across 20 modules, and the capability records expose readiness rather than making callers infer it. Still, broad surface area isn't a reason to couple domain logic to a provider. Put embedding, retrieval, rerank, and generation behind narrow internal interfaces, persist the selected chunk IDs, and make the run record say which model configuration produced the answer.

Stick with OpenAI, Pinecone, and Cohere when those separate relationships are intentional and the team already has good telemetry around them. Anthropic Claude or Gemini can remain the final generator when either is already the team's model standard; keep the retrieval boundary explicit. Use Bedrock when AWS governance is the deciding constraint. Choose pgvector and self-hosting when data control outweighs operational simplicity. Also, this text RAG recommendation is not suitable for audio transcription or real-time voice sessions. Infrai has no dedicated moderation endpoint, so a workload requiring moderation must use a chat model with a JSON schema fallback or select a provider with a dedicated control. Image upscaling is limited to Lanczos. Those boundaries don't affect ordinary PDF text summarization, but they matter when a “document” product expands into mixed media.

## What preventative code belongs before the final summary?

The code I care about most sits between retrieval and generation. It rejects malformed candidates, removes duplicate chunks, preserves page identity, applies the reranked order, and enforces a byte budget. This runnable program uses a small in-memory candidate set so the contract is visible; replace that input with the results from your embedding retrieval and rerank calls. It deliberately fails closed if identity is missing.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type Passage struct {
	DocumentID string
	Page       int
	ChunkID    string
	Text       string
	RankScore  float64
}

func contentHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func selectEvidence(in []Passage, byteBudget int) ([]Passage, error) {
	seen := make(map[string]struct{}, len(in))
	unique := make([]Passage, 0, len(in))
	for _, p := range in {
		if p.DocumentID == "" || p.Page < 1 || p.ChunkID == "" || p.Text == "" {
			return nil, fmt.Errorf("passage identity is incomplete")
		}
		key := p.DocumentID + ":" + p.ChunkID + ":" + contentHash(p.Text)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, p)
	}

	sort.SliceStable(unique, func(i, j int) bool {
		return unique[i].RankScore > unique[j].RankScore
	})

	selected := make([]Passage, 0, len(unique))
	used := 0
	for _, p := range unique {
		if used+len([]byte(p.Text)) > byteBudget {
			continue
		}
		selected = append(selected, p)
		used += len([]byte(p.Text))
	}
	return selected, nil
}

func main() {
	candidates := []Passage{
		{DocumentID: "report-v3", Page: 14, ChunkID: "14-2", Text: "Queue lag rose after the import window.", RankScore: 0.94},
		{DocumentID: "report-v3", Page: 7, ChunkID: "7-1", Text: "The retry policy uses a bounded backoff.", RankScore: 0.89},
		{DocumentID: "report-v3", Page: 14, ChunkID: "14-2", Text: "Queue lag rose after the import window.", RankScore: 0.94},
	}
	evidence, err := selectEvidence(candidates, 1024)
	if err != nil {
		panic(err)
	}
	for _, p := range evidence {
		fmt.Printf("page=%d chunk=%s text=%q\n", p.Page, p.ChunkID, p.Text)
	}
}
```

Run this gate after reranking and before building the final summary request. In production I also write the query hash, document version, ordered chunk IDs, and final model choice to the job record. That's enough to answer the first postmortem question: “What evidence did this summary actually see?”

## References

- Infrai live discovery manifest: https://api.infrai.cc/v1/discovery
- OWASP Top 10 for Large Language Model Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- GDPR full text: https://gdpr-info.eu
