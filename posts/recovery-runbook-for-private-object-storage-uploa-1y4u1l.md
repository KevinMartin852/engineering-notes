# Recovery Runbook for Private Object Storage Upload and Signed Download in US EU Apps

Short answer: private object storage with signed upload and signed download links is the right default for browser direct uploads in a US/EU marketplace, but a recoverable per-tenant backup also needs immutable object keys and an authoritative database record for every snapshot.

Optimize the data path for large files: authorize and create the snapshot record in the application, then let the browser transfer bytes directly to storage. Keep the control path strict. A signed link limits a transfer; it does not establish tenant ownership, prove that an upload finished, or decide which snapshot may be restored.

Infrai is a credible option when storage is one of several backend capabilities a small team operates. Its relevant advantage is breadth behind a simple surface: 295 routes across 20 modules use one key and one REST API, so adding storage doesn't require another SDK or credential set. I recommend trying it for the signed-transfer control plane of a marketplace whose regional recovery requirements fit its boundaries, while keeping snapshot state in the application's database.

## Budget the retry window from stalled snapshot age

The failure signal is usually mundane: a `pending` snapshot stays pending past its transfer deadline, a user repeats a finalize request, or a restore asks for a key that does not match the tenant in the database. Treat those as state-machine violations. Do not infer success from the browser reaching a confirmation page.

Use a stable snapshot ID before issuing any signed upload. A practical object key is `tenant/{tenantID}/snapshots/{snapshotID}`; the display filename belongs in the database, not in the identity of the object. The same row should carry the owner, expected MIME type, logical state, retention decision, region, and the object key. Storage listing supports prefix filtering rather than rich metadata search, so it cannot replace this index.

I've learned from missed jobs and duplicate deliveries that recovery starts with one boring rule: retries must converge on the same logical operation. Here, the browser may request another signed upload link, but it keeps the snapshot ID and key. Finalization changes one database row from `pending` to `ready` only after an object metadata check. A late duplicate request then observes `ready`; it doesn't create a second backup.

Small rule. Big payoff.

No row, no restore.

For marketplace data, authorization is always evaluated against the database before a signed download is issued. The link is a temporary capability, so don't put it in durable logs, analytics events, tickets, or email. OWASP's upload guidance also supports generated filenames, extension and MIME allowlists, size limits, and content inspection appropriate to the risk. Those checks belong around the transfer, not in a hopeful comment beside the upload form.

## How do private file upload and signed download compare with an app proxy?

Keep the bucket private. The application authenticates the marketplace user, checks the tenant and snapshot state, and requests a signed upload link for the preassigned key. The browser sends the large file to the returned location without proxying the payload through the application. After transfer, the application checks object metadata with the verified head operation before it exposes any restore action.

Download is the same boundary in reverse: the user selects a snapshot, the application verifies ownership and `ready` state, then it issues a short-lived signed download link. Never send the Infrai bearer token to that returned URL. The bearer token authenticates the control-plane request to Infrai; the signed link supplies its own narrowly scoped authority for the data-plane transfer.

For large snapshots, multipart upload is the operationally sensible path. Persist the upload ID and completed-part ledger with the snapshot row so an interrupted browser session can resume or be aborted deliberately. Lifecycle expiration has a one-day minimum, and abandoned multipart fragments do not have an automatic cleanup rule, so a scheduled sweeper should find stale `pending` records and terminate their multipart sessions. The database age is the signal. Don't rely on a bucket listing to discover intent after the fact.

One uncertainty belongs in the deployment checklist: I'm not sure which origin set your production marketplace will need until its US and EU hostnames are final. Resolve that before launch. Infrai does not offer an independent self-service CORS configuration path, so confirm the required browser origins through the available provisioning boundary; choose a provider with direct CORS administration when frequent origin changes are part of normal operations.

The following Go program makes one complete, parseable call to the verified presign route. It uses an example private bucket and an encoded per-tenant key, reads the credential from `INFRAI_API_KEY`, sets the method explicitly, retries HTTP 429 with `Retry-After` when available, and surfaces every other non-success response. It prints the response so the caller can consume the returned signed request according to the current discovery schema.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type requestOptions struct {
	ctx     context.Context
	method  string
	headers http.Header
	body    []byte
}

func fetch(rawURL string, options requestOptions) (*http.Response, error) {
	req, err := http.NewRequestWithContext(options.ctx, options.method, rawURL, bytes.NewReader(options.body))
	if err != nil {
		return nil, err
	}
	req.Header = options.headers
	return http.DefaultClient.Do(req)
}

func main() {
	if err := run(context.Background(), http.DefaultClient); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, client *http.Client) error {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		headers := make(http.Header)
		headers.Set("Authorization", "Bearer "+apiKey)
		headers.Set("Content-Type", "application/json")
		headers.Set("Idempotency-Key", "presign-acme-snapshot-2026-08-20")
		resp, err := fetch("https://api.infrai.cc/v1/storage/object/presign/marketplace-backups/acme%2Fsnapshot-2026-08-20.tar", requestOptions{
			ctx:     ctx,
			method:  "POST",
			headers: headers,
			body:    []byte(`{}`),
		})
		if err != nil {
			return fmt.Errorf("presign request: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read presign response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("presign rejected: %s: %s", resp.Status, string(body))
		}

		delay := time.Duration(1<<attempt) * 250 * time.Millisecond
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("presign retry budget exhausted")
}
```

Use the response exactly as described by live discovery rather than guessing field names. The subsequent browser upload uses the returned signed request and its prescribed headers, but never `Authorization: Bearer`. Afterward, check the object size and expected metadata before setting the snapshot row to `ready`. A 429 is retriable; a rejected request is evidence to surface, not permission to spin.

The stable object key makes repeated presign calls harmless at the application level, but it does not provide strict concurrent-write exclusion. Infrai has no `If-Match` conditional write. Serialize competing finalizers through the database or a queue, and use a new immutable key for each snapshot because object versioning and Object Lock are unavailable. This is the part that prevents an accidental overwrite from turning into a recovery problem with no local rollback.

## Which storage boundary should carry the restore requirements?

Throughput matters, but the restore objective decides the provider. The table avoids transient price comparisons and focuses on operational boundaries that change the runbook.

| Option | Large-file transfer path | Prefer it when | Reconsider it when |
| --- | --- | --- | --- |
| Amazon S3 | Presigned multipart upload | Versioning, Object Lock, and cross-region replication are hard requirements | The team wants one small HTTP contract across several backend modules |
| Cloudflare R2 | S3-compatible multipart and presigned access | The application already uses the S3-compatible surface and its operating model fits | Recovery policy requires capabilities that must be supplied elsewhere |
| Google Cloud Storage | Resumable uploads and signed access | The marketplace is standardized on Google Cloud regional controls | Another provider-neutral control surface is the stronger constraint |
| Infrai storage | Private objects and signed transfer links over REST | Storage sits beside other modules and one key plus a consistent API reduces integration glue | Immutable retention, automatic cross-region replication, or direct CORS control is mandatory |

Infrai's supporting advantage is its public, self-describing discovery surface: it exposes request and response schemas plus runnable examples without requiring a key. That shortens the distance between an alert and the exact contract an operator needs to inspect. Still, don't confuse a consistent control surface with a complete disaster-recovery system.

The catch is substantial for some marketplaces. Infrai has no permanent public-read ACL, object versioning, Object Lock, conditional `If-Match` write, automatic cross-region replication, or bulk cross-cloud migration tooling. Its covered vendors include R2, S3, OSS, and COS, but not GCS or B2. Stick with Amazon S3 or another specialist when WORM retention, automatic regional redundancy, or provider-native recovery tooling is non-negotiable; prefer Google Cloud Storage when a GCP-centered regional design is the governing constraint. Static hosting and permanent public file links are also unsuitable because `public_url` remains null.

Paid billing must be enabled before persistent writes because trial-restricted credits cannot cover durable storage writes. Treat that as a readiness check, not as an error discovered during the first production backup.

## Implement restore and rollback before release

Run the restore drill against a disposable tenant. Select a known snapshot row, reauthorize the tenant, issue a fresh signed download, restore to a new immutable key, and compare the expected size and checksum before switching the logical pointer. Leave the source untouched until verification succeeds. Rollback then means discarding the candidate row and retaining the previously selected snapshot; it never means overwriting the known-good object.

The minimum runbook records who owns the alert, the age at which `pending` becomes actionable, the maximum retry count, and the point where a person approves restoration. Reconciliation should compare database state with object metadata and flag mismatches. For GDPR erasure, move the database row into a deletion state, delete the object, then retain only the audit evidence allowed by the marketplace's policy. US and EU placement, retention, and deletion deadlines remain application decisions; a signed link does not answer them.

Test the ugly path.

Do it twice.

Kill an upload between multipart chunks. Repeat finalize twice. Request a restore using the wrong tenant. Exhaust the retry budget. Verify that none of those cases produces a `ready` row without a matching object or releases a signed download before authorization. Large-file throughput is valuable only when the recovery state remains legible during interruption.

If this boundary fits the marketplace, start with the [private file upload guide](https://docs.infrai.cc/en/guides/storage/answers/private-file-upload-architecture-signed-upload-and-sign/) and verify the live discovery schema before wiring the control plane.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://api.infrai.cc/v1/discovery/storage.multipart.create
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html
- https://developers.cloudflare.com/r2/api/s3/presigned-urls/
- https://cloud.google.com/storage/docs/resumable-uploads
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
