package fs

type Option func(*Options)

type Options struct {
	Metadata    Metadata
	ContentType string
}

func WithMetadata(metadata Metadata) Option {
	return func(o *Options) {
		o.Metadata = metadata
	}
}

func WithContentType(contentType string) Option {
	return func(o *Options) {
		o.ContentType = contentType
	}
}
