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
	ctx     context.Context
	client  *cos.Client
	path    string
	options fs.CreateOptions
	buffer  *bytes.Buffer
}

func newCosWriter(ctx context.Context, client *cos.Client, path string, options fs.CreateOptions) *cosWriter {
	return &cosWriter{
		ctx:     ctx,
		client:  client,
		path:    path,
		options: options,
		buffer:  bytes.NewBuffer(nil),
	}
}

func (w *cosWriter) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *cosWriter) Close() error {
	opt := &cos.ObjectPutOptions{}
	if w.options.ContentType != "" {
		opt.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{
			ContentType: w.options.ContentType,
		}
	}
	if w.options.Metadata != nil {
		if opt.ObjectPutHeaderOptions == nil {
			opt.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{}
		}
		opt.ObjectPutHeaderOptions.XCosMetaXXX = &http.Header{}
		for k, v := range w.options.Metadata {
			opt.ObjectPutHeaderOptions.XCosMetaXXX.Set(fmt.Sprintf("x-cos-meta-%s", k), fmt.Sprintf("%v", v))
		}
	}

	_, err := w.client.Object.Put(w.ctx, w.path, bytes.NewReader(w.buffer.Bytes()), opt)
	return err
}

type cosReadWriter struct {
	ctx    context.Context
	client *cos.Client
	path   string
	buffer *bytes.Buffer
}

func newCosReadWriter(ctx context.Context, client *cos.Client, path string) *cosReadWriter {
	return &cosReadWriter{
		ctx:    ctx,
		client: client,
		path:   path,
		buffer: bytes.NewBuffer(nil),
	}
}

func (rw *cosReadWriter) Read(p []byte) (n int, err error) {
	if rw.buffer.Len() == 0 {
		resp, err := rw.client.Object.Get(rw.ctx, rw.path, nil)
		if err != nil {
			return 0, err
		}
		defer func() {
			_ = resp.Body.Close()
		}()

		_, err = io.Copy(rw.buffer, resp.Body)
		if err != nil {
			return 0, err
		}
	}
	return rw.buffer.Read(p)
}

func (rw *cosReadWriter) Write(p []byte) (n int, err error) {
	return rw.buffer.Write(p)
}

func (rw *cosReadWriter) Close() error {
	_, err := rw.client.Object.Put(rw.ctx, rw.path, bytes.NewReader(rw.buffer.Bytes()), nil)
	return err
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
