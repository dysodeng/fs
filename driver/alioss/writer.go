package alioss

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dysodeng/fs"
)

// ossWriter 实现 io.WriteCloser 接口
type ossWriter struct {
	ctx     context.Context
	bucket  *oss.Bucket
	path    string
	buffer  *bytes.Buffer
	options fs.CreateOptions
}

func newOssWriter(ctx context.Context, bucket *oss.Bucket, path string, options fs.CreateOptions) *ossWriter {
	return &ossWriter{
		ctx:     ctx,
		bucket:  bucket,
		path:    path,
		buffer:  bytes.NewBuffer(nil),
		options: options,
	}
}

func (w *ossWriter) Write(p []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.buffer.Write(p)
	}
}

func (w *ossWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		options := []oss.Option{
			oss.WithContext(w.ctx),
		}

		// 设置 ContentType
		if w.options.ContentType != "" {
			options = append(options, oss.ContentType(w.options.ContentType))
		}

		// 处理metadata
		if w.options.Metadata != nil {
			for k, v := range w.options.Metadata {
				options = append(options, oss.Meta(k, fmt.Sprintf("%v", v)))
			}
		}

		return w.bucket.PutObject(w.path, bytes.NewReader(w.buffer.Bytes()), options...)
	}
}

// ossReadWriter 实现 io.ReadWriteCloser 接口
type ossReadWriter struct {
	*ossWriter
	reader io.ReadCloser
}

func newOssReadWriter(ctx context.Context, bucket *oss.Bucket, path string) *ossReadWriter {
	return &ossReadWriter{
		ossWriter: newOssWriter(ctx, bucket, path, fs.CreateOptions{}),
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

func (w *ossReadOnlyWrapper) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *ossReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
