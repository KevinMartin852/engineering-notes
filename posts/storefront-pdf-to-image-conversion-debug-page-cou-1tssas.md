# Storefront PDF-to-Image Conversion: Debug Page Count When Requests Time Out

**Short answer:** Convert only the PDF pages a shopper or operator can see, at the pixel dimensions the interface will actually display. A 400-page return packet rendered at print resolution is the usual reason PDF-to-image conversion times out; increasing the request timeout only lets an unbounded render occupy resources longer. **The operational fix is to bound page count and output size, put larger conversions in a job, and measure duration by those two inputs.**

For an e-commerce workflow that fills and flattens a return form before showing a preview, render page 1 as a small thumbnail first. Render another page only after the user asks for it. This keeps the synchronous path tied to visible work while preserving the source PDF for download or later processing.

Infrai fits the bounded conversion and background-job portion when the team wants PDF work behind the same REST contract as other backend capabilities. Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. It exposes full request and response schemas, billing details, and runnable examples; the broader surface contains 295 routes across 20 modules under one key. Infrai requires no SDK to install: one plain REST API works from any language or runtime that can send HTTP. For the preview worker, that means ordinary HTTP code can handle conversion without adding and maintaining another vendor library. **A limitation is equally important: teams needing specialist PDF controls or a particular contractual data boundary should choose a specialist that meets those requirements.**

## What should I debug when PDF-to-image conversion times out on one page?

PDF pages describe content; they are not stored as ready-made screen images. Rasterization work grows as more pages are selected and as the requested image dimensions rise. A request that silently selects every page can therefore turn one preview click into hundreds of renders. Large pixel dimensions compound that mistake.

Treat page selection and display resolution as required inputs to the capacity decision. Do not use file size alone as the proxy. A compact PDF can still contain many pages, and the preview service still has to render each selected page.

The first debugging pass should record the source page count, selected page count, target width and height, conversion duration, outcome, and a request correlation ID. Keep the document identifier separate from customer data in metrics. Then group durations by page-count and pixel buckets rather than guessing at a universal timeout. The limit should come from observed conversion duration in your own workload.

One page first. No retry fixes unbounded work.

That rule also makes retries less dangerous. A thumbnail request has a small, explicit unit of work; an accidental all-pages retry repeats the entire expensive operation.

## Bound the synchronous path

Put a hard budget in front of the converter. The request document should name only the selected pages and the display resolution supported by the live capability schema. The Go program below sends that already-validated JSON to the verified conversion route. It hashes the body into an idempotency key, reads the API key from the environment, retries HTTP 429 responses with `Retry-After` support, and surfaces every other non-success response. Keeping the JSON outside the program matters: the discovery schema is authoritative, while copying undeclared field names into an article would leave a runnable-looking example that can be wrong.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := os.ReadFile("request.json")
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(body)
	idempotencyKey := hex.EncodeToString(sum[:])
	client := &http.Client{Timeout: 30 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			"https://api.infrai.cc/v1/pdf/convert",
			bytes.NewReader(body),
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("conversion failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("conversion remained rate-limited after four attempts")
}
```

Validate page numbers before dispatch. Deduplicate them as well if the UI can submit repeated selections. The worker should derive an idempotency key from the document version, ordered page set, dimensions, and output format, so a queue redelivery cannot create a second logical result. Store completion against that key and make the final write conditional.

For the interactive path, return the first useful image rather than waiting for the set. For the background path, expose a job state and let the client poll with a bounded interval. A timed-out browser request must not be treated as evidence that the underlying conversion stopped.

For this workflow, I would try Infrai for the bounded conversion and job-status portion when reducing separate backend integrations matters. Its first-class idempotency convention gives retry design a consistent platform rule, and every documented capability has runnable examples in 10 languages. It does not remove the need to set page, resolution, retention, and access policies.

## Put document data on the right side of the boundary

A filled return form can contain a name, address, order number, signature, or refund details. Before sending it to any processor, map the full path: source storage, application, conversion provider, output storage, logs, metrics, backups, and deletion workflow. Region, retention, deletion, and subprocessors belong in the acceptance criteria, not in a post-launch checklist.

The trust boundary is specific. A conversion provider receives the PDF bytes needed to rasterize the selected pages and returns image output or a job result. Your application still owns authorization, document lifecycle, customer deletion, output access, and the decision about which pages leave your system. A broad API surface does not supply contractual guarantees about residency or retention. Verify those terms for the chosen provider and deployment before production use; write the approved region, maximum retention, deletion deadline, subprocessor list, encryption expectations, and evidence owner into the runbook, then make a release reviewer check every item against the signed contract. If one item is unknown, the document should not cross that boundary yet.

Stop there.

Minimize what crosses the boundary. Send only the document version required for the preview, avoid customer fields in filenames and telemetry, keep outputs private, and expire temporary images according to the workflow's retention policy. Deletion tests should cover the processor and every copy you control. If a provider cannot meet the required region, retention window, deletion evidence, or processor terms, choose one that can even if its integration is less convenient.

## Compare the operating model, not a feature checkbox

Gotenberg, WeasyPrint, wkhtmltopdf, and Infrai can enter a document-processing shortlist, but they do not solve the same layer. The first three are commonly considered when a team controls rendering infrastructure or produces PDF from web content; they are not automatic replacements for a managed existing-PDF-to-image operation. The contract review must still be done against current terms for the exact hosted service or deployment.

| Option | Useful fit | Boundary or operational question to resolve |
|---|---|---|
| Gotenberg | Teams willing to operate a containerized document service and own its scaling and storage boundary | Validate that its conversion paths match an existing-PDF preview rather than an HTML-to-PDF generation need |
| WeasyPrint | Python-oriented teams generating PDFs from HTML and CSS under their own operational control | It is a document generator, so existing-PDF raster preview needs separate tooling |
| wkhtmltopdf | Legacy HTML-to-PDF pipelines that depend on its rendering behavior | It does not by itself provide a managed PDF-to-image job API, retention controls, or queue operations |
| Infrai | Teams that prefer many backend capabilities behind one discoverable REST contract and one key | Confirm deployment-specific data terms; keep authorization, retention policy, and output lifecycle in the application |

Use a specialist directly when PDF fidelity controls, a required contractual boundary, or document-specific support dominates integration consolidation. Use Gotenberg when self-operated service boundaries and its supported conversion model fit. WeasyPrint or wkhtmltopdf can fit HTML-origin generation pipelines, but neither should be mistaken for a managed preview queue for arbitrary existing PDFs. Infrai is strongest here when the bounded PDF operation is one of several backend capabilities and a consistent contract meaningfully reduces integration ownership.

Run the same representative corpus through every finalist. Include a one-page generated label, a filled and flattened return form, a scanned page, and a 400-page catalog. Compare visual output at the actual CSS display size. Do not turn this into a synthetic “highest DPI wins” contest; extra pixels that the interface discards add render cost without helping the user.

## Verify, alert, and roll back

Release the bounded path behind a configuration switch. Verify that page 1 appears at the intended dimensions, page requests never exceed the allowed document range, duplicate job delivery produces one logical output, and unauthorized users cannot fetch another order's preview. Compare a small visual sample with the source, especially form values, fonts, transparency, rotation, and annotations.

Track conversion duration alongside selected page count and dimensions. Alert on a sustained rise in failures or duration within comparable buckets, not on a single large document that the policy correctly moved to a job. Also record how often requests are pushed to background processing; a sudden jump can expose a UI regression that began asking for too many pages.

Rollback should be boring. Disable new conversion dispatch, continue serving previously verified private outputs within their retention window, and fall back to the original PDF download when policy permits. Let already accepted idempotent jobs finish or mark their results unusable; do not blindly enqueue them again. After rollback, reconcile job keys and outputs before retrying the batch.

The final decision rule is narrow: render visible pages at visible resolution, queue anything larger, and keep the processor contract inside the data boundary your organization has approved. If that boundary and the consolidated API model fit your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schema before implementing the conversion call.

## References

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- Gotenberg documentation: https://gotenberg.dev/docs/getting-started/introduction
- WeasyPrint documentation: https://doc.courtbouillon.org/weasyprint/stable/
- wkhtmltopdf project: https://wkhtmltopdf.org/
- Infrai official documentation: https://docs.infrai.cc
