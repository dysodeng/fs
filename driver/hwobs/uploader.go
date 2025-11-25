package hwobs

import (
	"context"
	"io"

	"github.com/dysodeng/fs"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

func (driver *obsFs) Uploader() fs.Uploader {
	return driver
}

func (driver *obsFs) Upload(ctx context.Context, path string, reader io.Reader, opts ...fs.Option) error {
	file, err := driver.Create(ctx, path, opts...)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

func (driver *obsFs) InitMultipartUpload(ctx context.Context, path string, opts ...fs.Option) (string, error) {
	path = driver.path(path)
	o := &fs.Options{}
	for _, opt := range opts {
		opt(o)
	}
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = driver.config.BucketName
	input.Key = path
	if o.ContentType != "" {
		input.ContentType = o.ContentType
	}

	output, err := driver.client.InitiateMultipartUpload(input)
	if err != nil {
		return "", err
	}

	return output.UploadId, nil
}

func (driver *obsFs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader, opts ...fs.Option) (string, error) {
	path = driver.path(path)
	input := &obs.UploadPartInput{
		Bucket:     driver.config.BucketName,
		Key:        path,
		PartNumber: partNumber,
		UploadId:   uploadID,
		Body:       data,
	}
	output, err := driver.client.UploadPart(input)
	if err != nil {
		return "", err
	}
	return output.ETag, nil
}

func (driver *obsFs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart, opts ...fs.Option) error {
	path = driver.path(path)
	obsParts := make([]obs.Part, len(parts))
	for i, part := range parts {
		obsParts[i] = obs.Part{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}
	input := &obs.CompleteMultipartUploadInput{
		Bucket:   driver.config.BucketName,
		Key:      path,
		UploadId: uploadID,
		Parts:    obsParts,
	}
	_, err := driver.client.CompleteMultipartUpload(input)
	return err
}

func (driver *obsFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string, opts ...fs.Option) error {
	path = driver.path(path)
	input := &obs.AbortMultipartUploadInput{
		Bucket:   driver.config.BucketName,
		Key:      path,
		UploadId: uploadID,
	}

	_, err := driver.client.AbortMultipartUpload(input)
	return err
}

func (driver *obsFs) ListMultipartUploads(ctx context.Context, opts ...fs.Option) ([]fs.MultipartUploadInfo, error) {
	input := &obs.ListMultipartUploadsInput{
		Bucket: driver.config.BucketName,
	}
	output, err := driver.client.ListMultipartUploads(input)
	if err != nil {
		return nil, err
	}

	uploads := make([]fs.MultipartUploadInfo, 0, len(output.Uploads))
	for _, upload := range output.Uploads {
		uploads = append(uploads, fs.MultipartUploadInfo{
			UploadID:   upload.UploadId,
			Path:       upload.Key,
			CreateTime: upload.Initiated,
		})
	}
	return uploads, nil
}

func (driver *obsFs) ListUploadedParts(ctx context.Context, path string, uploadID string, opts ...fs.Option) ([]fs.MultipartPart, error) {
	path = driver.path(path)
	input := &obs.ListPartsInput{
		Bucket:   driver.config.BucketName,
		Key:      path,
		UploadId: uploadID,
	}
	output, err := driver.client.ListParts(input)
	if err != nil {
		return nil, err
	}

	parts := make([]fs.MultipartPart, 0, len(output.Parts))
	for _, part := range output.Parts {
		parts = append(parts, fs.MultipartPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
			Size:       part.Size,
		})
	}
	return parts, nil
}
