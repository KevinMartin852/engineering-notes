# How to Compare Go Text Summarization API Cost — Startup Batch Processing

TL;DR: Put asynchronous code-review summaries behind a small Go interface, persist an idempotency key before calling any model, and page on growing job age rather than on individual failures. Compare providers with measured input and output tokens per accepted review, not an advertised input-token rate. This gives an edtech startup a batch path that can move between providers without changing the queue, result schema, or runbook.

The page fires at 02:17. It says the oldest code-review job is 38 minutes old, 612 submissions are waiting, and completions have stopped advancing. The on-call does not need a model error string first; they need to know whether workers are stalled, capacity is exhausted, or completed work cannot be committed. I have been paged by both missed jobs and duplicate deliveries. The expensive lesson is that a request-success counter cannot distinguish those states.

Retries lie.

## What should wake the on-call?

Page when students or instructors are at risk of missing the review window. For this workflow, queue age is the closest operational proxy: record `enqueued_at`, observe the oldest uncompleted job, and set a threshold from the service objective for returning findings. A burst of HTTP errors can remain a ticket if retries are draining the queue. A quiet queue whose oldest item keeps aging deserves attention.

The first dashboard view should put four signals on one timeline: oldest-job age, queue depth, completion rate, and terminal-failure rate. Split completions into accepted and rejected structured results. If the provider returns text that cannot satisfy the local schema, transport succeeded but the review did not. Count it accordingly.

There is an earlier signal than the page: completion rate falling below arrival rate while oldest-job age is still inside the review window. Alert that condition to the work queue, not the pager, and use it to add workers, reduce concurrency against a constrained dependency, or pause low-priority rechecks. The page remains tied to user impact.

## Make the batch boundary portable

Keep provider-specific request and response types at the adapter edge. The durable job should contain the repository, commit identifier, rubric version, source digest, and priority. It should not contain a provider model name. The durable result should contain structured findings, usage reported by the adapter, and provenance sufficient to reproduce the decision.

A useful idempotency key is a digest of immutable review inputs: repository, commit, rubric version, and source digest. Insert that key with a uniqueness constraint before inference. A retry then resumes the same logical job; it does not create another review or another student-visible result. If a worker loses its lease after inference, commit the result with a compare-and-set against the job state. Duplicate delivery becomes routine recovery.

This design has a limitation: it is not suitable for an interactive editor that needs partial findings while a student types. The durable batch boundary favors recoverability and provider portability over first-token latency. Keep interactive assistance on a separate path with its own objective instead of weakening the review queue's commit rules.

The adapter can stay narrow:

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
)

type ReviewJob struct {
    Repository    string `json:"repository"`
    Commit        string `json:"commit"`
    RubricVersion string `json:"rubric_version"`
    SourceDigest  string `json:"source_digest"`
}

type Finding struct {
    Path     string `json:"path"`
    Line     int    `json:"line"`
    Severity string `json:"severity"`
    Summary  string `json:"summary"`
}

type Usage struct {
    InputTokens  int `json:"input_tokens"`
    OutputTokens int `json:"output_tokens"`
}

type Reviewer interface {
    Review(context.Context, ReviewJob) ([]Finding, Usage, error)
}

func key(j ReviewJob) string {
    sum := sha256.Sum256([]byte(j.Repository + "\x00" + j.Commit + "\x00" + j.RubricVersion + "\x00" + j.SourceDigest))
    return hex.EncodeToString(sum[:])
}

func validate(findings []Finding) error {
    for _, f := range findings {
        if f.Path == "" || f.Line < 1 || f.Summary == "" {
            return errors.New("invalid structured finding")
        }
        switch f.Severity {
        case "info", "warning", "error":
        default:
            return fmt.Errorf("invalid severity %q", f.Severity)
        }
    }
    return nil
}

func main() {
    job := ReviewJob{"course/compiler", "7f3a9c1", "intro-go-v4", "sha256:4a1d"}
    encoded, _ := json.Marshal(job)
    fmt.Printf("job=%s key=%s\n", encoded, key(job))
}
```

The example deliberately omits a provider client. Each adapter must translate the same `ReviewJob`, return the same `Finding` shape, expose token usage, and classify errors into retryable, terminal, and throttled outcomes. Portability lives in those tests, not in an interface declaration alone. Run contract fixtures against every adapter before deployment: valid multi-file findings, empty findings, malformed output, cancellation, timeout, and a repeated idempotency key.

## How should a startup compare text summarization API cost?

Per-1,000-token rates are inputs to a decision, not the decision. A cheap text summarization API can still be a poor fit for structured code-review processing. For a representative set of student changes, capture input tokens, output tokens, retry count, schema rejection, end-to-end latency, and whether a human accepts the findings. Keep the rubric and source samples fixed while comparing adapters.

Use a normalized calculation:

`job cost = (input tokens / 1,000 × input rate) + (output tokens / 1,000 × output rate) + retry cost`

Then divide total spend by accepted reviews. A lower token rate can lose if the adapter produces verbose output, needs more retries, or fails local validation. Batch discounts, when available, belong in a separate asynchronous lane because they change completion time and cancellation behavior. Store the rate card version beside the experiment rather than embedding a mutable price in application code.

| Signal | Decision it supports | Trade-off it exposes |
| --- | --- | --- |
| Tokens per accepted review | Capacity and budget forecast | Verbosity versus useful detail |
| Schema rejection rate | Adapter readiness | Strict contracts versus salvageable output |
| Oldest-job age | Paging and scaling | Recovery margin versus false alarms |
| Replay count | Retry policy | Fast recovery versus duplicate work |

Quality needs a gate as well. Build a versioned evaluation set from consented or synthetic code changes, strip credentials and unnecessary student data, and score actionable findings separately from formatting compliance. Do not send a failed review directly to a learner. Terminal failures go to a dead-letter queue with a reason code and a controlled replay path after the cause is fixed.

This is the trade: a local schema and evaluation harness add engineering work, but they make provider changes measurable. A universal prompt without contract tests is cheaper for a week and opaque during the next migration.

## Instrument the alert-to-action trace

Emit one metric event for every state transition: queued, leased, inference started, inference finished, validated, committed, or terminally failed. Attach low-cardinality dimensions such as rubric version, adapter, priority, and outcome. Keep repository names, commit identifiers, and idempotency keys in logs or traces, not metric labels.

The runbook should follow the same state machine. If leases rise but inference starts do not, inspect worker admission and dependency concurrency. If inference finishes but commits do not, inspect validation and result storage. If completion rate is healthy but oldest-job age rises, check priority starvation: a steady stream of recent high-priority jobs can hide an old stranded job from aggregate throughput.

Deploy the instrumentation before changing the alert. Observe normal class periods, assignment deadlines, and quiet hours. Choose warning and paging thresholds from the review-window objective and measured drain time, then test them with a controlled backlog. The test passes only when the dashboard identifies the blocked transition and the runbook names a reversible action.

## Tune thresholds without training people to ignore them

A threshold that pages on every transient retry teaches the on-call to distrust the page. One that waits for the full user deadline leaves no recovery budget. Use a warning for a sustained capacity deficit, a page for oldest-job age consuming the agreed recovery margin, and an escalation when the queue cannot drain before the review window closes.

Revisit the thresholds after workload or concurrency changes. Record every alert as actionable, non-actionable, or late; that small post-incident field gives the next review evidence instead of opinion. False positives have a real cost: interrupted sleep, rushed mitigation, and eventual alert blindness. The goal is not zero pages. It is a page that points to a user-visible risk and a specific state transition worth fixing.

## Further reading

- Cohere, "Rerank overview": https://docs.cohere.com/docs/rerank-overview
- pgvector, "Open-source vector similarity search for Postgres": https://github.com/pgvector/pgvector
