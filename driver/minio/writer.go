package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dysodeng/fs"
	"github.com/minio/minio-go/v7"
)

// minioWriter 实现 io.WriteCloser 接口
type minioWriter struct {
	ctx         context.Context
	client      *minio.Client
	bucket      string
	path        string
	buffer      *bytes.Buffer
	metadata    fs.Metadata
	contentType string
}

func newMinioWriter(ctx context.Context, client *minio.Client, bucket, path string, opts ...fs.Option) *minioWriter {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	writer := &minioWriter{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		path:   path,
		buffer: bytes.NewBuffer(nil),
	}

	if o.ContentType != "" {
		writer.contentType = o.ContentType
	}
	if o.Metadata != nil {
		writer.metadata = o.Metadata
	}

	return writer
}

func (w *minioWriter) Write(p []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.buffer.Write(p)
	}
}

func (w *minioWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		opts := minio.PutObjectOptions{}

		// 设置 ContentType
		if w.contentType != "" {
			opts.ContentType = w.contentType
		}

		// 处理metadata
		if w.metadata != nil {
			userMetadata := make(map[string]string)
			for k, v := range w.metadata {
				userMetadata[k] = fmt.Sprintf("%v", v)
			}
			opts.UserMetadata = userMetadata
		}

		_, err := w.client.PutObject(
			w.ctx,
			w.bucket,
			w.path,
			bytes.NewReader(w.buffer.Bytes()),
			int64(w.buffer.Len()),
			opts,
		)
		return err
	}
}

// minioReadWriter 实现 io.ReadWriteCloser 接口
type minioReadWriter struct {
	*minioWriter
	reader io.ReadCloser
}

func newMinioReadWriter(ctx context.Context, client *minio.Client, bucket, path string, opts ...fs.Option) *minioReadWriter {
	return &minioReadWriter{
		minioWriter: newMinioWriter(ctx, client, bucket, path, opts...),
	}
}

func (rw *minioReadWriter) Read(p []byte) (n int, err error) {
	if rw.reader == nil {
		var err error
		rw.reader, err = rw.client.GetObject(
			rw.ctx,
			rw.bucket,
			rw.path,
			minio.GetObjectOptions{},
		)
		if err != nil {
			return 0, err
		}
	}
	return rw.reader.Read(p)
}

func (rw *minioReadWriter) Close() error {
	if rw.reader != nil {
		_ = rw.reader.Close()
	}
	return rw.minioWriter.Close()
}

// minioReadOnlyWrapper 包装只读流为 ReadWriteCloser
type minioReadOnlyWrapper struct {
	reader io.ReadCloser
}

func newMinioReadOnlyWrapper(reader io.ReadCloser) *minioReadOnlyWrapper {
	return &minioReadOnlyWrapper{reader: reader}
}

func (w *minioReadOnlyWrapper) Read(p []byte) (n int, err error) {
	return w.reader.Read(p)
}

func (w *minioReadOnlyWrapper) Write(_ []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *minioReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
