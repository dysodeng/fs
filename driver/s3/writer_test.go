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
