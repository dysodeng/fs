package fs

import "time"

// AccessMode 访问模式
type AccessMode uint8

const (
	Private         AccessMode = iota // 私有读写
	PublicRead                        // 公共读
	PublicReadWrite                   // 公共读写
)

type Option func(*Options)

type Options struct {
	Metadata       Metadata
	ContentType    string
	CdnDomain      string
	SignUrlExpires time.Duration
}

// WithMetadata 设置元数据
func WithMetadata(metadata Metadata) Option {
	return func(o *Options) {
		o.Metadata = metadata
	}
}

// WithContentType 设置文件类型
func WithContentType(contentType string) Option {
	return func(o *Options) {
		o.ContentType = contentType
	}
}

// WithCdnDomain 设置cdn域名
func WithCdnDomain(cdnDomain string) Option {
	return func(o *Options) {
		o.CdnDomain = cdnDomain
	}
}

func WithSignUrlExpires(expires time.Duration) Option {
	return func(o *Options) {
		o.SignUrlExpires = expires
	}
}
