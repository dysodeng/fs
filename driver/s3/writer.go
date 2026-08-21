package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/goairix/fs"
)

type s3Writer struct {
	ctx                context.Context
	client             *s3.Client
	bucket             string
	path               string
	buffer             *bytes.Buffer
	metadata           fs.Metadata
	contentType        string
	contentDisposition string
}

func newS3Writer(ctx context.Context, client *s3.Client, bucket, path string, opts ...fs.Option) *s3Writer {
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}

	writer := &s3Writer{
		ctx:    ctx,
		client: client,
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

func (w *s3Writer) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func buildPutObjectInput(bucket, path string, body io.Reader, contentType, contentDisposition string, metadata fs.Metadata) *s3.PutObjectInput {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   body,
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	if contentDisposition != "" {
		input.ContentDisposition = aws.String(contentDisposition)
	}
	if metadata != nil {
		input.Metadata = make(map[string]string, len(metadata))
		for key, value := range metadata {
			input.Metadata[key] = fmt.Sprintf("%v", value)
		}
	}
	return input
}

func (w *s3Writer) Close() error {
	input := buildPutObjectInput(
		w.bucket,
		w.path,
		bytes.NewReader(w.buffer.Bytes()),
		w.contentType,
		w.contentDisposition,
		w.metadata,
	)

	_, err := w.client.PutObject(w.ctx, input)
	return err
}

type s3ReadWriter struct {
	*s3Writer
	reader io.ReadCloser
}

func newS3ReadWriter(ctx context.Context, client *s3.Client, bucket, path string, opts ...fs.Option) *s3ReadWriter {
	return &s3ReadWriter{
		s3Writer: newS3Writer(ctx, client, bucket, path, opts...),
	}
}

func (rw *s3ReadWriter) Read(p []byte) (n int, err error) {
	if rw.reader == nil {
		output, err := rw.client.GetObject(rw.ctx, &s3.GetObjectInput{
			Bucket: aws.String(rw.bucket),
			Key:    aws.String(rw.path),
		})
		if err != nil {
			return 0, err
		}
		rw.reader = output.Body
	}
	return rw.reader.Read(p)
}

func (rw *s3ReadWriter) Close() error {
	if rw.reader != nil {
		_ = rw.reader.Close()
	}
	return rw.s3Writer.Close()
}

type s3ReadOnlyWrapper struct {
	reader io.ReadCloser
}

func newS3ReadOnlyWrapper(reader io.ReadCloser) *s3ReadOnlyWrapper {
	return &s3ReadOnlyWrapper{reader: reader}
}

func (w *s3ReadOnlyWrapper) Read(p []byte) (n int, err error) {
	return w.reader.Read(p)
}

func (w *s3ReadOnlyWrapper) Write(_ []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot write to read-only file")
}

func (w *s3ReadOnlyWrapper) Close() error {
	return w.reader.Close()
}
