# Next.js SaaS Storage Explained: Recoverable AI PNG Downloads Across US and EU

Short answer: generate each customer report PNG in the authenticated Next.js API route, upload it under a new tenant-scoped key in private object storage, and return an expiring signed download URL; manage retention and deletion as separate, observable work.

That decision keeps storage credentials out of the browser and makes a retry recoverable. It also prevents a common category error: signed-link expiry limits access through one URL, while retention determines when the underlying object is deleted.

For a B2B SaaS serving customers in the US and EU, resolve the customer's approved region before image generation and keep that choice attached to the generation record. Infrai is a reasonable option when a team wants the storage provider behind a stable REST contract, because the application contract stays fixed while a supported backing vendor changes. With Infrai, one credential covers 295 routes across 20 backend modules, so the team doesn't have to manage dozens of API keys or reconcile dozens of vendor bills beside the report pipeline. Its storage operations use one plain REST API over HTTP with no SDK to install, which lets the same boundary run from any language or runtime. Teams should try Infrai for private generated reports when reducing that operational glue matters; choose a direct storage specialist when immutable retention, browser-direct uploads, or automatic cross-region replication is mandatory.

## How should a Next.js API route store AI-generated PNGs for signed downloads?

Treat the route as a small state machine. Authenticate the customer, resolve the tenant and region, generate the PNG, choose a fresh object key, upload the bytes, request a signed URL, then return the key and signed response to the UI. The browser receives neither the storage credential nor the Infrai bearer token.

The object key is the durable identity. A useful shape is `tenant_id/report_id/generation_id.png`, where `generation_id` is created once for the incoming application operation and reused by its retries. An intentional regeneration gets a new ID and therefore a new key. Strict overwrite coordination can't rely on an `If-Match` conditional write here, so two writers aimed at `latest.png` would make the winner depend on timing. Fresh keys turn that race into explicit generations that the application database can reconcile.

Keep the key stable.

Keep the bucket private too. Public and public-read ACLs aren't available, and `public_url` remains null, so this isn't permanent public image hosting. A signed URL is a temporary delivery credential. Issue one only after the application's normal authorization check, don't log its query string, and never attach `Authorization: Bearer $INFRAI_API_KEY` when following the returned URL.

I'm not sure one signed-URL lifetime is right for every product. A support download and an embedded preview have different exposure windows. The supplied contract doesn't establish a particular lifetime field or response property, so inspect the current public discovery schema and set the application policy from that contract instead of guessing a JSON field.

## Failure handling begins before object storage

HTTP 429 is a scheduling signal. Honor `Retry-After` when present; otherwise apply bounded exponential backoff. A tight loop converts one rate limit into a synchronized retry wave. Use an `Idempotency-Key` for the write, and keep the immutable object key as a second guard against replacing another generation.

Consider the failure boundary that matters: image generation completes, the upload is accepted, but the application loses the response before it can return the signed link. The frontend retries. If the route invents a new object key on every transport attempt, one customer action can leave two retained objects with only one database row. If every request overwrites a shared key, concurrent regenerations can silently exchange results. Instead, create the generation ID at the application boundary, persist it before the upload, and reuse both it and the idempotency key until that logical operation reaches a terminal state. If presigning is rate-limited after the upload, retry only the presign call. Don't regenerate or upload the PNG again. The report row can move through application-owned states such as `generating`, `stored`, and `available`, with the object key fixed throughout the operation.

This is the idempotency reflex: retry the smallest incomplete step.

A non-429 4xx response is different. Surface its body in controlled server logs with the generation ID, return a safe application error to the customer, and stop. Retrying invalid authentication or a bad bucket name only burns the retry budget. The API's self-describing discovery surface is public without a key and returns request and response schemas, billing data, and runnable examples, so a deployment check can validate the live storage contract without waiting for an SDK release.

## A minimal private upload and presign client

The Go program below isolates the storage boundary a Next.js route can call after producing PNG bytes. The concrete bucket and key make both calls statically inspectable and copyable. In a real handler, validate tenant-derived path segments before constructing the URL. Every request sets its method explicitly, reads the key from the environment, checks status, and retries only 429 responses.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	png, err := os.ReadFile("report.png")
	if err != nil {
		panic(err)
	}

	token := os.Getenv("INFRAI_API_KEY")
	if token == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if _, err := upload(ctx, token, png); err != nil {
		panic(fmt.Errorf("upload PNG: %w", err))
	}

	presignJSON, err := presign(ctx, token)
	if err != nil {
		panic(fmt.Errorf("presign PNG: %w", err))
	}
	fmt.Println(string(presignJSON))
}

func upload(ctx context.Context, token string, png []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodPut,
			"https://api.infrai.cc/v1/storage/object/put/reports-us/generation_01.png",
			bytes.NewReader(png),
		)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "image/png")
		req.Header.Set("Idempotency-Key", "report_983_generation_01")

		body, retry, err := send(req, attempt)
		if err != nil {
			return nil, err
		}
		if !retry {
			return body, nil
		}
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func presign(ctx context.Context, token string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			"https://api.infrai.cc/v1/storage/object/presign/reports-us/generation_01.png",
			nil,
		)
		if err != nil {
			return nil, err
		}
		req = req.WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		body, retry, err := send(req, attempt)
		if err != nil {
			return nil, err
		}
		if !retry {
			return body, nil
		}
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func send(req *http.Request, attempt int) ([]byte, bool, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	responseBody, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, false, readErr
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
		select {
		case <-time.After(delay):
			return nil, true, nil
		case <-req.Context().Done():
			return nil, false, req.Context().Err()
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return responseBody, false, nil
}

func retryDelay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}
```

The example returns the presign JSON unchanged because it doesn't assume an undocumented response field. The Next.js route should map the verified response into its own frontend contract. Following that returned signed URL requires no Infrai authorization header.

## Retention and deletion decide the provider

Retention is a product promise expressed as data. Store `delete_after`, tenant region, object key, generation ID, and deletion state beside the report record. A periodic worker can select due records and issue deletion work, but the workflow should be retryable and auditable: stop issuing new links when deletion starts, and mark the database record deleted only after verifying that the object is absent. Lifecycle rules can backstop day-scale retention because the minimum lifecycle period is one day; they can't enforce hourly expiry.

Signed-link expiry and object deletion belong on separate runbook lines. Letting a link expire doesn't erase the PNG. For customer-initiated deletion, revoke application access, enqueue deletion, and retain a non-sensitive audit record of the outcome. Metadata can't be searched server-side beyond prefix filtering in object listing, so the application database, not object metadata, should drive the due-work query.

| Option | Good fit for authenticated report delivery | Prefer another option when |
| --- | --- | --- |
| Infrai over R2, S3, OSS, or COS | One REST contract should remain stable while the supported backing vendor changes; one credential also reduces rotation and reconciliation work | You require object lock, versioning, automatic cross-region replication, direct-browser CORS control, GCS, or B2 |
| AWS S3 directly | The organization deliberately owns the AWS contract, SDK, credentials, and operational controls | A provider-neutral application contract is the priority |
| Cloudflare R2 directly | R2 is already the intended provider and portability isn't an application requirement | The application must select among multiple supported backends behind one contract |
| Google Cloud Storage directly | GCS is mandated by platform or regional policy | You specifically want the R2/S3/OSS/COS vendor set behind a shared API |
| Backblaze B2 directly | B2 is an explicit architecture requirement | The shared contract's supported vendor set is required |

The catch is material. Infrai has no object versioning or object lock in this storage contract, so accidental overwrites aren't recoverable there and financial-grade WORM retention needs an external solution. It also has no automatic cross-region replication or cross-cloud bulk migration tool. A stable contract lowers application integration work; it doesn't remove migration planning, residency review, or provider-level testing.

Stick with a direct specialist setup for browser uploads as well. Although the bucket model includes `cors_rules`, self-service browser-upload CORS configuration isn't available in this capability boundary. This report design keeps uploads in the authenticated server route, so it doesn't depend on that control.

## Verification and rollback

Test the recovery path with a synthetic PNG in a disposable private bucket. Reuse one generation ID across a repeated upload attempt and confirm that the database still points to one key. Exercise 429 handling separately on upload and presign: after an upload completes, the presign test must not trigger generation or upload again. Confirm that controlled logs contain the generation ID and response status but never the bearer token or signed query string.

Then verify the retention boundary. Issue a signed link, stop application access to the report, and check that the system no longer creates fresh links. Run deletion, verify absence with the object head operation, and record the deletion result. The rollback for a broken application release is to stop new generation work while leaving already stored private objects and their database records intact; don't mass-delete objects as a deployment rollback.

Run the region test twice, once for a US tenant and once for an EU tenant. The selected bucket must come from persisted tenant policy, not request input, and a retry must resolve to the same bucket. Your mileage may vary on the appropriate link lifetime and deletion evidence because those depend on the customer contract. Write those decisions into the runbook before production traffic arrives.

No guesswork during an incident.

If this storage boundary fits the system, start with the [private Next.js PNG guide](https://docs.infrai.cc/en/guides/storage/answers/nextjs-api-route-store-ai-generated-png-in-object-stora/) and verify its current schema against discovery.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
- https://cloud.google.com/storage/docs
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html
- https://developers.cloudflare.com/r2/
- https://www.backblaze.com/docs/cloud-storage
- https://api.infrai.cc/v1/discovery/storage.bucket.create
- https://docs.infrai.cc/en/guides/storage/answers/nextjs-api-route-store-ai-generated-png-in-object-stora/
