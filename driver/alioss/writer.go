package alioss

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/goairix/fs"
)

// ossWriter 实现 io.WriteCloser 接口
type ossWriter struct {
	ctx                context.Context
	bucket             *oss.Bucket
	path               string
	buffer             *bytes.Buffer
	metadata           fs.Metadata
	contentType        string
	contentDisposition string
}

func newOssWriter(ctx context.Context, bucket *oss.Bucket, path string, opts ...fs.Option) *ossWriter {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	writer := &ossWriter{
		ctx:    ctx,
		bucket: bucket,
		path:   path,
		buffer: bytes.NewBuffer(nil),
	}
	if o.ContentType != "" {
		writer.contentType = o.ContentType
	}
	if o.ContentDisposition != "" {
		writer.contentDisposition = o.ContentDisposition
	}
	if o.Metadata != nil {
		writer.metadata = o.Metadata
	}

	return writer
}

func (w *ossWriter) Write(p []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.buffer.Write(p)
	}
}

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

func (w *ossWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		options := buildPutObjectOptions(w.ctx, w.contentType, w.contentDisposition, w.metadata)

		return w.bucket.PutObject(w.path, bytes.NewReader(w.buffer.Bytes()), options...)
	}
}

// ossReadWriter 实现 io.ReadWriteCloser 接口
type ossReadWriter struct {
	*ossWriter
	reader io.ReadCloser
}

func newOssReadWriter(ctx context.Context, bucket *oss.Bucket, path string, opts ...fs.Option) *ossReadWriter {
	return &ossReadWriter{
		ossWriter: newOssWriter(ctx, bucket, path, opts...),
	}
}

func (rw *ossReadWriter) Read(p []byte) (n int, err error) {
	if rw.reader == nil {
		var err error
		rw.reader, err = rw.bucket.GetObject(rw.path, oss.WithContext(rw.ctx))
		if err != nil {
			return 0, err
		}
	}
	return rw.reader.Read(p)
}

func (rw *ossReadWriter) Close() error {
	if rw.reader != nil {
		_ = rw.reader.Close()
	}
	return rw.ossWriter.Close()
}

// ossReadOnlyWrapper 包装只读流为 ReadWriteCloser
type ossReadOnlyWrapper struct {
	reader io.ReadCloser
}

func newOssReadOnlyWrapper(reader io.ReadCloser) *ossReadOnlyWrapper {
	return &ossReadOnlyWrapper{reader: reader}
}

func (w *ossReadOnlyWrapper) Read(p []byte) (n int, err error) {
	return w.reader.Read(p)
}

func (w *ossReadOnlyWrapper) Write(_ []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *ossReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
