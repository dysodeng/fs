package txcos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/dysodeng/fs"
	"github.com/tencentyun/cos-go-sdk-v5"
)

type cosWriter struct {
	ctx         context.Context
	client      *cos.Client
	path        string
	buffer      *bytes.Buffer
	metadata    fs.Metadata
	contentType string
}

func newCosWriter(ctx context.Context, client *cos.Client, path string, opts ...fs.Option) *cosWriter {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	writer := &cosWriter{
		ctx:    ctx,
		client: client,
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

func (w *cosWriter) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *cosWriter) Close() error {
	opt := &cos.ObjectPutOptions{}
	if w.contentType != "" {
		opt.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{
			ContentType: w.contentType,
		}
	}
	if w.metadata != nil {
		if opt.ObjectPutHeaderOptions == nil {
			opt.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{}
		}
		opt.ObjectPutHeaderOptions.XCosMetaXXX = &http.Header{}
		for k, v := range w.metadata {
			opt.ObjectPutHeaderOptions.XCosMetaXXX.Set(fmt.Sprintf("x-cos-meta-%s", k), fmt.Sprintf("%v", v))
		}
	}

	_, err := w.client.Object.Put(w.ctx, w.path, bytes.NewReader(w.buffer.Bytes()), opt)
	return err
}

type cosReadWriter struct {
	*cosWriter
	reader io.ReadCloser
}

func newCosReadWriter(ctx context.Context, client *cos.Client, path string, opts ...fs.Option) *cosReadWriter {
	return &cosReadWriter{
		cosWriter: newCosWriter(ctx, client, path, opts...),
	}
}

func (rw *cosReadWriter) Read(p []byte) (n int, err error) {
	if rw.reader == nil {
		output, err := rw.client.Object.Get(rw.ctx, rw.path, nil)
		if err != nil {
			return 0, err
		}
		defer func() {
			_ = output.Body.Close()
		}()
		rw.reader = output.Body
	}
	return rw.reader.Read(p)
}

func (rw *cosReadWriter) Close() error {
	if rw.reader != nil {
		_ = rw.reader.Close()
	}
	return rw.cosWriter.Close()
}

type cosReadOnlyWrapper struct {
	reader io.ReadCloser
}

func newCosReadOnlyWrapper(reader io.ReadCloser) *cosReadOnlyWrapper {
	return &cosReadOnlyWrapper{reader: reader}
}

func (w *cosReadOnlyWrapper) Read(p []byte) (n int, err error) {
	return w.reader.Read(p)
}

func (w *cosReadOnlyWrapper) Write(_ []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *cosReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
