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
