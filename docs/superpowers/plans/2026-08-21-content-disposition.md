# Content-Disposition Upload Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a public `WithContentDisposition` option and propagate it through normal and multipart uploads for every HTTP object-storage driver.

**Architecture:** Extend `fs.Options` without changing its functional-option pattern. Each cloud driver will map the value to its SDK-native request field or option; small pure request builders will make those mappings testable without cloud credentials. The local driver remains unchanged because it has no HTTP object attributes.

**Tech Stack:** Go 1.25, standard `testing`, MinIO Go SDK v7, Huawei OBS SDK, AWS SDK for Go v2 S3, Aliyun OSS SDK, Tencent COS SDK.

---

### Task 1: Public Functional Option

**Files:**
- Create: `option_test.go`
- Modify: `option.go`

- [ ] **Step 1: Write the failing option test**

```go
package fs

import "testing"

func TestWithContentDisposition(t *testing.T) {
	options := &Options{}

	WithContentDisposition("attachment; filename=file.pdf")(options)

	if options.ContentDisposition != "attachment; filename=file.pdf" {
		t.Fatalf("ContentDisposition = %q, want %q", options.ContentDisposition, "attachment; filename=file.pdf")
	}
}
```

- [ ] **Step 2: Run the option test and verify RED**

Run: `go test . -run '^TestWithContentDisposition$'`

Expected: build failure because `WithContentDisposition` and `Options.ContentDisposition` do not exist.

- [ ] **Step 3: Add the public option**

Add the field beside `ContentType` in `Options`:

```go
ContentDisposition string
```

Add the setter beside `WithContentType`:

```go
// WithContentDisposition 设置文件的 Content-Disposition
func WithContentDisposition(contentDisposition string) Option {
	return func(o *Options) {
		o.ContentDisposition = contentDisposition
	}
}
```

- [ ] **Step 4: Run the option test and verify GREEN**

Run: `go test . -run '^TestWithContentDisposition$'`

Expected: PASS.

- [ ] **Step 5: Commit the public API change**

```bash
git add option.go option_test.go
git commit -m "feat: add content disposition option"
```

### Task 2: MinIO Mapping

**Files:**
- Create: `driver/minio/writer_test.go`
- Modify: `driver/minio/writer.go`
- Modify: `driver/minio/uploader.go`

- [ ] **Step 1: Write failing MinIO mapping tests**

```go
package minio

import (
	"context"
	"testing"

	"github.com/goairix/fs"
)

func TestBuildPutObjectOptionsIncludesContentDisposition(t *testing.T) {
	options := buildPutObjectOptions("application/pdf", "inline", nil)

	if options.ContentDisposition != "inline" {
		t.Fatalf("ContentDisposition = %q, want inline", options.ContentDisposition)
	}
}

func TestBuildPutObjectOptionsOmitsEmptyContentDisposition(t *testing.T) {
	options := buildPutObjectOptions("application/pdf", "", fs.Metadata{"source": "test"})

	if options.ContentDisposition != "" {
		t.Fatalf("ContentDisposition = %q, want empty", options.ContentDisposition)
	}
}

func TestNewMinioWriterPreservesContentDisposition(t *testing.T) {
	writer := newMinioWriter(context.Background(), nil, "bucket", "file.pdf", fs.WithContentDisposition("inline"))

	if writer.contentDisposition != "inline" {
		t.Fatalf("contentDisposition = %q, want inline", writer.contentDisposition)
	}
}
```

- [ ] **Step 2: Run the MinIO tests and verify RED**

Run: `go test ./driver/minio -run '^TestBuildPutObjectOptions'`

Expected: build failure because `buildPutObjectOptions` does not exist.

- [ ] **Step 3: Add the MinIO request builder and writer propagation**

Add `contentDisposition string` to `minioWriter`. Copy `o.ContentDisposition` in `newMinioWriter` when non-empty.

Add this pure builder to `driver/minio/writer.go`:

```go
func buildPutObjectOptions(contentType, contentDisposition string, metadata fs.Metadata) minio.PutObjectOptions {
	options := minio.PutObjectOptions{
		ContentType:        contentType,
		ContentDisposition: contentDisposition,
	}
	if metadata != nil {
		options.UserMetadata = make(map[string]string, len(metadata))
		for key, value := range metadata {
			options.UserMetadata[key] = fmt.Sprintf("%v", value)
		}
	}
	return options
}
```

Replace the inline option construction in `minioWriter.Close` with:

```go
opts := buildPutObjectOptions(w.contentType, w.contentDisposition, w.metadata)
```

- [ ] **Step 4: Use the same builder for MinIO multipart initialization**

Replace the `minio.PutObjectOptions` construction in `InitMultipartUpload` with:

```go
options := buildPutObjectOptions(o.ContentType, o.ContentDisposition, nil)
```

- [ ] **Step 5: Run the MinIO tests and verify GREEN**

Run: `go test ./driver/minio`

Expected: PASS.

- [ ] **Step 6: Commit the MinIO mapping**

```bash
git add driver/minio/writer.go driver/minio/writer_test.go driver/minio/uploader.go
git commit -m "feat(minio): set content disposition on uploads"
```

### Task 3: Huawei OBS Mapping

**Files:**
- Create: `driver/hwobs/writer_test.go`
- Modify: `driver/hwobs/writer.go`
- Modify: `driver/hwobs/uploader.go`

- [ ] **Step 1: Write failing OBS mapping tests**

```go
package hwobs

import (
	"bytes"
	"context"
	"testing"

	"github.com/goairix/fs"
)

func TestBuildPutObjectInputIncludesContentDisposition(t *testing.T) {
	input := buildPutObjectInput("bucket", "file.pdf", bytes.NewReader(nil), "application/pdf", "inline", nil)

	if input.ContentDisposition != "inline" {
		t.Fatalf("ContentDisposition = %q, want inline", input.ContentDisposition)
	}
}

func TestBuildInitiateMultipartUploadInputIncludesContentDisposition(t *testing.T) {
	options := &fs.Options{ContentType: "application/pdf", ContentDisposition: "inline"}
	input := buildInitiateMultipartUploadInput("bucket", "file.pdf", options)

	if input.ContentDisposition != "inline" {
		t.Fatalf("ContentDisposition = %q, want inline", input.ContentDisposition)
	}
}

func TestNewObsWriterPreservesContentDisposition(t *testing.T) {
	writer := newObsWriter(context.Background(), nil, "bucket", "file.pdf", fs.WithContentDisposition("inline"))

	if writer.contentDisposition != "inline" {
		t.Fatalf("contentDisposition = %q, want inline", writer.contentDisposition)
	}
}
```

- [ ] **Step 2: Run the OBS tests and verify RED**

Run: `go test ./driver/hwobs -run 'ContentDisposition'`

Expected: build failure because both request builders are missing.

- [ ] **Step 3: Add the OBS normal-upload builder and propagation**

Add `contentDisposition string` to `obsWriter`, populate it from `o.ContentDisposition`, and add:

```go
func buildPutObjectInput(bucket, path string, body io.Reader, contentType, contentDisposition string, metadata fs.Metadata) *obs.PutObjectInput {
	input := &obs.PutObjectInput{Body: body}
	input.Bucket = bucket
	input.Key = path
	input.ContentType = contentType
	input.ContentDisposition = contentDisposition
	if metadata != nil {
		input.Metadata = make(map[string]string, len(metadata))
		for key, value := range metadata {
			input.Metadata[key] = fmt.Sprintf("%v", value)
		}
	}
	return input
}
```

Use it in `obsWriter.Close`:

```go
input := buildPutObjectInput(
	w.bucket,
	w.path,
	bytes.NewReader(w.buffer.Bytes()),
	w.contentType,
	w.contentDisposition,
	w.metadata,
)
```

- [ ] **Step 4: Add and use the OBS multipart builder**

Add to `driver/hwobs/uploader.go`:

```go
func buildInitiateMultipartUploadInput(bucket, path string, options *fs.Options) *obs.InitiateMultipartUploadInput {
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = bucket
	input.Key = path
	input.ContentType = options.ContentType
	input.ContentDisposition = options.ContentDisposition
	return input
}
```

Use it in `InitMultipartUpload`:

```go
input := buildInitiateMultipartUploadInput(driver.config.BucketName, path, o)
```

- [ ] **Step 5: Run the OBS tests and verify GREEN**

Run: `go test ./driver/hwobs`

Expected: PASS.

- [ ] **Step 6: Commit the OBS mapping**

```bash
git add driver/hwobs/writer.go driver/hwobs/writer_test.go driver/hwobs/uploader.go
git commit -m "feat(hwobs): set content disposition on uploads"
```

### Task 4: AWS S3 Mapping

**Files:**
- Create: `driver/s3/writer_test.go`
- Modify: `driver/s3/writer.go`
- Modify: `driver/s3/uploader.go`

- [ ] **Step 1: Write failing S3 mapping tests**

```go
package s3

import (
	"bytes"
	"context"
	"testing"

	"github.com/goairix/fs"
)

func TestBuildPutObjectInputIncludesContentDisposition(t *testing.T) {
	input := buildPutObjectInput("bucket", "file.pdf", bytes.NewReader(nil), "application/pdf", "inline", nil)

	if input.ContentDisposition == nil || *input.ContentDisposition != "inline" {
		t.Fatalf("ContentDisposition = %v, want inline", input.ContentDisposition)
	}
}

func TestBuildCreateMultipartUploadInputIncludesContentDisposition(t *testing.T) {
	options := &fs.Options{ContentType: "application/pdf", ContentDisposition: "inline"}
	input := buildCreateMultipartUploadInput("bucket", "file.pdf", options)

	if input.ContentDisposition == nil || *input.ContentDisposition != "inline" {
		t.Fatalf("ContentDisposition = %v, want inline", input.ContentDisposition)
	}
}

func TestNewS3WriterPreservesContentDisposition(t *testing.T) {
	writer := newS3Writer(context.Background(), nil, "bucket", "file.pdf", fs.WithContentDisposition("inline"))

	if writer.contentDisposition != "inline" {
		t.Fatalf("contentDisposition = %q, want inline", writer.contentDisposition)
	}
}
```

- [ ] **Step 2: Run the S3 tests and verify RED**

Run: `go test ./driver/s3 -run 'ContentDisposition'`

Expected: build failure because both request builders are missing.

- [ ] **Step 3: Add the S3 normal-upload builder and propagation**

Add `contentDisposition string` to `s3Writer`, populate it from `o.ContentDisposition`, and add:

```go
func buildPutObjectInput(bucket, path string, body io.Reader, contentType, contentDisposition string, metadata fs.Metadata) *s3.PutObjectInput {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   body,
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	if contentDisposition != "" {
		input.ContentDisposition = aws.String(contentDisposition)
	}
	if metadata != nil {
		input.Metadata = make(map[string]string, len(metadata))
		for key, value := range metadata {
			input.Metadata[key] = fmt.Sprintf("%v", value)
		}
	}
	return input
}
```

Use the builder from `s3Writer.Close` with `w.contentType`, `w.contentDisposition`, and `w.metadata`.

- [ ] **Step 4: Add and use the S3 multipart builder**

Add to `driver/s3/uploader.go`:

```go
func buildCreateMultipartUploadInput(bucket, path string, options *fs.Options) *s3.CreateMultipartUploadInput {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	}
	if options.ContentType != "" {
		input.ContentType = aws.String(options.ContentType)
	}
	if options.ContentDisposition != "" {
		input.ContentDisposition = aws.String(options.ContentDisposition)
	}
	return input
}
```

Use it in `InitMultipartUpload`:

```go
input := buildCreateMultipartUploadInput(driver.config.BucketName, path, o)
```

- [ ] **Step 5: Run the S3 tests and verify GREEN**

Run: `go test ./driver/s3`

Expected: PASS.

- [ ] **Step 6: Commit the S3 mapping**

```bash
git add driver/s3/writer.go driver/s3/writer_test.go driver/s3/uploader.go
git commit -m "feat(s3): set content disposition on uploads"
```

### Task 5: Aliyun OSS Mapping

**Files:**
- Create: `driver/alioss/writer_test.go`
- Modify: `driver/alioss/writer.go`
- Modify: `driver/alioss/uploader.go`

- [ ] **Step 1: Write failing OSS mapping tests**

```go
package alioss

import (
	"context"
	"testing"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/goairix/fs"
)

func TestBuildPutObjectOptionsIncludesContentDisposition(t *testing.T) {
	options := buildPutObjectOptions(context.Background(), "application/pdf", "inline", nil)
	value, err := oss.FindOption(options, oss.HTTPHeaderContentDisposition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != "inline" {
		t.Fatalf("Content-Disposition = %v, want inline", value)
	}
}

func TestBuildPutObjectOptionsOmitsEmptyContentDisposition(t *testing.T) {
	options := buildPutObjectOptions(context.Background(), "application/pdf", "", nil)
	set, _, err := oss.IsOptionSet(options, oss.HTTPHeaderContentDisposition)
	if err != nil {
		t.Fatal(err)
	}
	if set {
		t.Fatal("Content-Disposition is set, want omitted")
	}
}

func TestNewOssWriterPreservesContentDisposition(t *testing.T) {
	writer := newOssWriter(context.Background(), nil, "file.pdf", fs.WithContentDisposition("inline"))

	if writer.contentDisposition != "inline" {
		t.Fatalf("contentDisposition = %q, want inline", writer.contentDisposition)
	}
}
```

- [ ] **Step 2: Run the OSS tests and verify RED**

Run: `go test ./driver/alioss -run '^TestBuildPutObjectOptions'`

Expected: build failure because `buildPutObjectOptions` does not exist.

- [ ] **Step 3: Add the OSS option builder and writer propagation**

Add `contentDisposition string` to `ossWriter`, populate it from `o.ContentDisposition`, and add:

```go
func buildPutObjectOptions(ctx context.Context, contentType, contentDisposition string, metadata fs.Metadata) []oss.Option {
	options := []oss.Option{oss.WithContext(ctx)}
	if contentType != "" {
		options = append(options, oss.ContentType(contentType))
	}
	if contentDisposition != "" {
		options = append(options, oss.ContentDisposition(contentDisposition))
	}
	for key, value := range metadata {
		options = append(options, oss.Meta(key, fmt.Sprintf("%v", value)))
	}
	return options
}
```

Replace the inline option construction in `ossWriter.Close` with:

```go
options := buildPutObjectOptions(w.ctx, w.contentType, w.contentDisposition, w.metadata)
```

- [ ] **Step 4: Use the OSS builder for multipart initialization**

Replace its inline option construction with:

```go
options := buildPutObjectOptions(ctx, o.ContentType, o.ContentDisposition, nil)
```

- [ ] **Step 5: Run the OSS tests and verify GREEN**

Run: `go test ./driver/alioss`

Expected: PASS.

- [ ] **Step 6: Commit the OSS mapping**

```bash
git add driver/alioss/writer.go driver/alioss/writer_test.go driver/alioss/uploader.go
git commit -m "feat(alioss): set content disposition on uploads"
```

### Task 6: Tencent COS Mapping

**Files:**
- Create: `driver/txcos/writer_test.go`
- Modify: `driver/txcos/writer.go`
- Modify: `driver/txcos/uploader.go`

- [ ] **Step 1: Write failing COS mapping tests**

```go
package txcos

import (
	"context"
	"testing"

	"github.com/goairix/fs"
)

func TestBuildObjectPutHeaderOptionsIncludesContentDisposition(t *testing.T) {
	options := buildObjectPutHeaderOptions("application/pdf", "inline")

	if options == nil || options.ContentDisposition != "inline" {
		t.Fatalf("ContentDisposition = %v, want inline", options)
	}
}

func TestBuildObjectPutHeaderOptionsOmitsEmptyValues(t *testing.T) {
	options := buildObjectPutHeaderOptions("", "")
	if options.ContentDisposition != "" {
		t.Fatalf("ContentDisposition = %q, want empty", options.ContentDisposition)
	}
}

func TestNewCosWriterPreservesContentDisposition(t *testing.T) {
	writer := newCosWriter(context.Background(), nil, "file.pdf", fs.WithContentDisposition("inline"))

	if writer.contentDisposition != "inline" {
		t.Fatalf("contentDisposition = %q, want inline", writer.contentDisposition)
	}
}
```

- [ ] **Step 2: Run the COS tests and verify RED**

Run: `go test ./driver/txcos -run '^TestBuildObjectPutHeaderOptions'`

Expected: build failure because `buildObjectPutHeaderOptions` does not exist.

- [ ] **Step 3: Add the COS header builder and writer propagation**

Add `contentDisposition string` to `cosWriter`, populate it from `o.ContentDisposition`, and add:

```go
func buildObjectPutHeaderOptions(contentType, contentDisposition string) *cos.ObjectPutHeaderOptions {
	return &cos.ObjectPutHeaderOptions{
		ContentType:        contentType,
		ContentDisposition: contentDisposition,
	}
}
```

Initialize the writer request with:

```go
opt := &cos.ObjectPutOptions{
	ObjectPutHeaderOptions: buildObjectPutHeaderOptions(w.contentType, w.contentDisposition),
}
```

Retain the existing metadata loop unchanged.

- [ ] **Step 4: Use the COS builder for multipart initialization**

Replace its inline header construction with:

```go
options := &cos.InitiateMultipartUploadOptions{
	ObjectPutHeaderOptions: buildObjectPutHeaderOptions(o.ContentType, o.ContentDisposition),
}
```

- [ ] **Step 5: Run the COS tests and verify GREEN**

Run: `go test ./driver/txcos`

Expected: PASS.

- [ ] **Step 6: Commit the COS mapping**

```bash
git add driver/txcos/writer.go driver/txcos/writer_test.go driver/txcos/uploader.go
git commit -m "feat(txcos): set content disposition on uploads"
```

### Task 7: Full Regression Verification

**Files:**
- Verify: `option.go`
- Verify: `driver/minio/writer.go`
- Verify: `driver/minio/uploader.go`
- Verify: `driver/hwobs/writer.go`
- Verify: `driver/hwobs/uploader.go`
- Verify: `driver/s3/writer.go`
- Verify: `driver/s3/uploader.go`
- Verify: `driver/alioss/writer.go`
- Verify: `driver/alioss/uploader.go`
- Verify: `driver/txcos/writer.go`
- Verify: `driver/txcos/uploader.go`

- [ ] **Step 1: Format all changed Go files**

Run:

```bash
gofmt -w option.go option_test.go \
  driver/minio/writer.go driver/minio/writer_test.go driver/minio/uploader.go \
  driver/hwobs/writer.go driver/hwobs/writer_test.go driver/hwobs/uploader.go \
  driver/s3/writer.go driver/s3/writer_test.go driver/s3/uploader.go \
  driver/alioss/writer.go driver/alioss/writer_test.go driver/alioss/uploader.go \
  driver/txcos/writer.go driver/txcos/writer_test.go driver/txcos/uploader.go
```

Expected: command exits successfully.

- [ ] **Step 2: Run focused mapping tests**

Run:

```bash
go test . ./driver/minio ./driver/hwobs ./driver/s3 ./driver/alioss ./driver/txcos -run 'ContentDisposition'
```

Expected: PASS for every package.

- [ ] **Step 3: Run the full test suite**

Run: `go test ./...`

Expected: all packages PASS with no build failures.

- [ ] **Step 4: Check the final diff**

Run:

```bash
git diff --check
git status --short
git diff --stat HEAD~6..HEAD
```

Expected: no whitespace errors; status only contains the implementation-plan document if it has not yet been committed; the diff includes the public option, five cloud drivers, and their tests, with no Local driver changes.

- [ ] **Step 5: Commit the implementation plan if still uncommitted**

```bash
git add docs/superpowers/plans/2026-08-21-content-disposition.md
git commit -m "docs: add content disposition implementation plan"
```
