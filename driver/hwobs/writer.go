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
	ctx     context.Context
	client  *obs.ObsClient
	bucket  string
	path    string
	buffer  *bytes.Buffer
	options fs.CreateOptions
}

func newObsWriter(ctx context.Context, client *obs.ObsClient, bucket, path string, options fs.CreateOptions) *obsWriter {
	return &obsWriter{
		ctx:     ctx,
		client:  client,
		bucket:  bucket,
		path:    path,
		buffer:  bytes.NewBuffer(nil),
		options: options,
	}
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
		if w.options.ContentType != "" {
			input.ContentType = w.options.ContentType
		}

		// 处理metadata
		if w.options.Metadata != nil {
			input.Metadata = make(map[string]string)
			for k, v := range w.options.Metadata {
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

func newObsReadWriter(ctx context.Context, client *obs.ObsClient, bucket, path string) *obsReadWriter {
	return &obsReadWriter{
		obsWriter: newObsWriter(ctx, client, bucket, path, fs.CreateOptions{}),
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

func (w *obsReadOnlyWrapper) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *obsReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
