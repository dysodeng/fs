package fs

import "testing"

func TestWithContentDisposition(t *testing.T) {
	options := &Options{}

	WithContentDisposition("attachment; filename=file.pdf")(options)

	if options.ContentDisposition != "attachment; filename=file.pdf" {
		t.Fatalf("ContentDisposition = %q, want %q", options.ContentDisposition, "attachment; filename=file.pdf")
	}
}
