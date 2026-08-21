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
