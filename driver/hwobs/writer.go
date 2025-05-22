package hwobs

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/dysodeng/fs"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

// obsWriter 实现 io.WriteCloser 接口
type obsWriter struct {
	ctx         context.Context
	client      *obs.ObsClient
	bucket      string
	path        string
	buffer      *bytes.Buffer
	metadata    fs.Metadata
	contentType string
}

func newObsWriter(ctx context.Context, client *obs.ObsClient, bucket, path string, opts ...fs.Option) *obsWriter {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	writer := &obsWriter{
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

func (w *obsWriter) Write(p []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.buffer.Write(p)
	}
}

func (w *obsWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		input := &obs.PutObjectInput{
			Body: bytes.NewReader(w.buffer.Bytes()),
		}
		input.Bucket = w.bucket
		input.Key = w.path

		// 设置 ContentType
		if w.contentType != "" {
			input.ContentType = w.contentType
		}

		// 处理metadata
		if w.metadata != nil {
			input.Metadata = make(map[string]string)
			for k, v := range w.metadata {
				input.Metadata[k] = fmt.Sprintf("%v", v)
			}
		}

		_, err := w.client.PutObject(input)
		return err
	}
}

// obsReadWriter 实现 io.ReadWriteCloser 接口
type obsReadWriter struct {
	*obsWriter
	reader io.ReadCloser
}

func newObsReadWriter(ctx context.Context, client *obs.ObsClient, bucket, path string, opts ...fs.Option) *obsReadWriter {
	return &obsReadWriter{
		obsWriter: newObsWriter(ctx, client, bucket, path, opts...),
	}
}

func (rw *obsReadWriter) Read(p []byte) (n int, err error) {
	if rw.reader == nil {
		input := &obs.GetObjectInput{}
		input.Bucket = rw.bucket
		input.Key = rw.path
		output, err := rw.client.GetObject(input)
		if err != nil {
			return 0, err
		}
		rw.reader = output.Body
	}
	return rw.reader.Read(p)
}

func (rw *obsReadWriter) Close() error {
	if rw.reader != nil {
		_ = rw.reader.Close()
	}
	return rw.obsWriter.Close()
}

// obsReadOnlyWrapper 包装只读流为 ReadWriteCloser
type obsReadOnlyWrapper struct {
	reader io.ReadCloser
}

func newObsReadOnlyWrapper(reader io.ReadCloser) *obsReadOnlyWrapper {
	return &obsReadOnlyWrapper{reader: reader}
}

func (w *obsReadOnlyWrapper) Read(p []byte) (n int, err error) {
	return w.reader.Read(p)
}

func (w *obsReadOnlyWrapper) Write(_ []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *obsReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
