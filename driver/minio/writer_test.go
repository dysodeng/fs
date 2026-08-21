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
