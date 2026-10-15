# Node.js Responsive Image API — Generate Moderated WebP and AVIF Variants

The least complex production design is to moderate each original blog cover once, record that decision, and only then fan out a fixed set of WebP and AVIF derivatives. **Do not treat successful resizing as proof that an image was safe to publish.** For a B2B SaaS blog, the page should fire when an approved cover is missing its expected variants at publish time, while a separate, earlier signal catches originals whose moderation decision is absent, stale, or inconclusive.

TL;DR: put an immutable upload ID and a moderation decision ahead of the variant job. Generate only widths smaller than or equal to the source, keep the original aspect ratio, and store a manifest that names every expected output. Serve that manifest through `srcset` and `sizes`; do not guess file names in the page template. The practical API choice is therefore less about codec support than about whether moderation coverage, retries, and output identity remain observable across an asynchronous fan-out.

## How should a Node.js API generate responsive image variants?

The visible symptom is blunt: a scheduled article has gone live with a broken cover, or with only the largest fallback loading. The page reports something like `cover_variant_completeness < 1` for an article whose publication timestamp has passed. An operator can see the article ID, upload ID, moderation state, source dimensions, expected variant count, completed count, and age of the oldest unfinished job. That context separates a slow encoder from a cover that was intentionally held.

The first question is not "Is the image service up?" It is "Was this upload ever eligible for fan-out?" A moderation result of `approved` may proceed. `rejected` must not produce public derivatives. `review` remains private until a human decision arrives. A missing or expired decision is also ineligible; silently treating uncertainty as approval creates a coverage gap that a green resize-success graph will never reveal. This distinction changes the page. A rejected cover is a workflow outcome, not an encoder incident. An approved upload with six expected objects and five stored objects is an operational failure. A pending review that reaches the publication deadline is an editorial escalation. They need different owners even if all three appear to readers as a missing image. For each accepted source, choose a small width ladder from actual layout slots rather than a round-number contest. As an example, a blog with 360 px cards, 720 px article columns, and 1200 px social or wide layouts can request those three widths in both WebP and AVIF. That makes six derivatives when the original is at least 1200 px wide, fewer for a smaller source. Never upscale merely to satisfy the manifest.

Stop there.

## Work backward to the earlier signal

The late completeness page should be the last defense. The earlier signal is a coverage ratio over uploads approaching publication: eligible uploads with a current moderation decision divided by all uploads attached to scheduled articles. Alert on state and deadline together, not on a raw queue depth. A queue of 500 jobs might be normal after a bulk import; one undecided cover due in two minutes is actionable.

The useful timeline has four durable events: `upload.accepted`, `moderation.decided`, `variants.planned`, and `variant.stored`. Each event carries the upload ID and content digest. The decision also carries a policy version, outcome, and decision time. Variant events add width, codec, pixel dimensions, byte size, and attempt number. These are facts an operator can reconcile. Log lines alone are not a ledger.

Keep the source private while its decision is unresolved. Once approved, derive a deterministic job key from the source digest, policy version, width, codec, and transformation version. A retry can then converge on the same object instead of creating another public asset. If moderation policy changes, the policy version makes the scope of re-evaluation explicit; overwriting history would hide which decision authorized an existing derivative.

There is a sharp edge here. Moderation coverage is not the same as moderation accuracy. Coverage tells you every relevant upload received a decision under a known policy. Accuracy requires labeled review data and a separate analysis of false accepts and false rejects. Do not combine those measures into one comforting percentage.

That is the trade-off.

## Instrument the fan-out boundary

The API boundary should accept one plan and return one manifest identity. The queue may execute individual variants, but the planner owns the expected set. This focused Go example shows the invariants even when the surrounding application runs on Node.js; production code still needs its chosen decoder, encoder, object store, and moderation adapter.

```go
package covers

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type Decision struct {
	Outcome       string
	PolicyVersion string
}

type Variant struct {
	Width  int
	Format string
	Key    string
}

func Plan(sourceDigest string, sourceWidth int, decision Decision) ([]Variant, error) {
	if decision.Outcome != "approved" {
		return nil, fmt.Errorf("source is not approved for derivatives")
	}

	widths := []int{360, 720, 1200}
	formats := []string{"avif", "webp"}
	variants := make([]Variant, 0, len(widths)*len(formats))

	for _, width := range widths {
		if width > sourceWidth {
			continue
		}
		for _, format := range formats {
			raw := fmt.Sprintf("%s:%s:%d:%s:v3", sourceDigest, decision.PolicyVersion, width, format)
			sum := sha256.Sum256([]byte(raw))
			variants = append(variants, Variant{
				Width: width, Format: format,
				Key: hex.EncodeToString(sum[:]) + "." + format,
			})
		}
	}

	sort.Slice(variants, func(i, j int) bool {
		if variants[i].Width == variants[j].Width {
			return variants[i].Format < variants[j].Format
		}
		return variants[i].Width < variants[j].Width
	})
	return variants, nil
}
```

The deterministic key is the idempotency mechanism. It also prevents two retrying workers from inventing separate identities for the same output. The storage write should be conditional, and the database transition should accept repeated completion events without incrementing the completed count twice. Exactly-once delivery is not required; stable effects are.

Instrument the boundary with counters for moderation outcomes, plans created, variant attempts, stored variants, and terminal failures. Add histograms for decision latency and approved-to-complete latency. Cardinality matters: article and upload IDs belong in traces or structured logs, not metric labels. Labels should remain bounded, such as outcome, codec, width bucket, policy version, and failure class.

One invariant deserves a periodic reconciler: every public manifest references only stored objects derived from an approved source under its recorded policy. The reconciler should also find stored objects absent from any manifest and manifests whose expected count differs from their completed set. It is slower than the hot path by design. This is where drift becomes visible.

## Choose an API by the evidence it preserves

Codec checkboxes are table stakes. The selection axis for this system is moderation coverage across the entire lifecycle, including retries and policy changes.

| Decision point | Evidence required | Failure it prevents |
| --- | --- | --- |
| Before planning | Upload ID, digest, decision, policy version | Unreviewed source entering fan-out |
| During encoding | Stable job key, attempt, width, codec | Duplicate or ambiguous outputs |
| Before publishing | Expected and stored manifest entries | Partial `srcset` reaching readers |
| After policy change | Decision lineage and affected uploads | Old approvals becoming invisible |

Ask whether the moderation step can return an explicit non-success state without losing the source, whether a human decision can resume the same workflow, and whether every derivative can be traced to the decision that permitted it. Also ask how cancellation works. Deleting a queued message is insufficient if an encoder already holds the source; publication authorization must be checked again before the manifest becomes public.

This moderation-first approach is a poor fit for a trusted, closed asset library where every source has already passed an equivalent review and publication is not deadline-driven. In that case, the decision ledger and paging path add state without adding meaningful coverage. It is also insufficient for a system that needs semantic cropping or art direction: width-only derivatives preserve the entire frame, so an editor must supply crop intent or separate source images. The extra state buys traceability, but it raises storage, reconciliation, and runbook complexity. Choose it when proving authorization matters more than minimizing workflow steps.

Image dimensions and file signatures should be validated independently of a client-supplied content type. Decode within memory, pixel-count, and execution-time limits. Strip unnecessary metadata from public derivatives unless the application has a documented reason to retain it. These controls address malformed or unexpectedly expensive inputs; they do not replace content moderation.

On the delivery side, HTML format selection should remain declarative. Put AVIF and WebP candidates in separate `source` elements within `picture`, then retain an `img` fallback with dimensions to reserve layout space. `srcset` width descriptors tell the browser what exists, while `sizes` describes the rendered slot. The browser chooses the candidate. Server-side user-agent branching adds another cache and correctness surface without improving the manifest's audit trail.

## Release without turning readers into probes

Test the state machine before testing image quality. A duplicate completion must leave one manifest entry. A rejected source must yield none. An approved 640 px source must skip 720 px and 1200 px outputs. A policy update must identify the previously approved set for re-evaluation. Then use representative covers to inspect crops, transparency, color, text legibility, and codec fallback in supported browsers.

Deployment should separate authorization from encoder changes. Roll out a new transformation version behind a shadow manifest, compare dimensions and decode success, and publish it only after validation. Preserve the old manifest long enough for rollback. A content digest plus transformation version makes that rollback mechanical rather than forensic.

No reader should be the canary.

Runbooks should begin with the state split: moderation pending, moderation rejected, plan absent, encoding incomplete, or manifest unpublished. The operator needs a bounded action for each state, such as retrying an idempotent job, requesting editorial review, or republishing a complete manifest. "Restart the workers" is not a diagnosis.

The final threshold is a policy choice with a real false-positive cost. Paging whenever any moderation decision is pending will punish the on-call for drafts that are weeks from publication. Waiting until publication misses the chance to act. A practical alert combines unresolved state with time-to-publish, routes long-horizon gaps to a ticket, and reserves a page for an approved or undecided cover close enough to its deadline that a person must intervene. Measure how often that page leads to action. If operators routinely acknowledge it and do nothing, the threshold is consuming trust faster than it protects coverage.

## Further reading

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
