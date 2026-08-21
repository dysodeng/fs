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
