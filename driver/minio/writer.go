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
	client  *minio.Client
	bucket  string
	path    string
	buffer  *bytes.Buffer
	options fs.CreateOptions
}

func newMinioWriter(client *minio.Client, bucket, path string, options fs.CreateOptions) *minioWriter {
	return &minioWriter{
		client:  client,
		bucket:  bucket,
		path:    path,
		buffer:  bytes.NewBuffer(nil),
		options: options,
	}
}

func (w *minioWriter) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *minioWriter) Close() error {
	opts := minio.PutObjectOptions{}

	// 设置 ContentType
	if w.options.ContentType != "" {
		opts.ContentType = w.options.ContentType
	}

	// 处理metadata
	if w.options.Metadata != nil {
		// 将metadata转换为字符串map
		userMetadata := make(map[string]string)
		for k, v := range w.options.Metadata {
			userMetadata[k] = fmt.Sprintf("%v", v)
		}
		opts.UserMetadata = userMetadata
	}

	_, err := w.client.PutObject(
		context.Background(),
		w.bucket,
		w.path,
		bytes.NewReader(w.buffer.Bytes()),
		int64(w.buffer.Len()),
		opts,
	)
	return err
}

// minioReadWriter 实现 io.ReadWriteCloser 接口
type minioReadWriter struct {
	*minioWriter
	reader io.ReadCloser
}

func newMinioReadWriter(client *minio.Client, bucket, path string) *minioReadWriter {
	return &minioReadWriter{
		minioWriter: newMinioWriter(client, bucket, path, fs.CreateOptions{}),
	}
}

func (rw *minioReadWriter) Read(p []byte) (n int, err error) {
	if rw.reader == nil {
		var err error
		rw.reader, err = rw.client.GetObject(
			context.Background(),
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
		rw.reader.Close()
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

func (w *minioReadOnlyWrapper) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *minioReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
