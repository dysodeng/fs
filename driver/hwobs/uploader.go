package hwobs

import (
	"context"
	"io"

	"github.com/dysodeng/fs"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

func (o *obsFs) Uploader() fs.Uploader {
	return o
}

func (o *obsFs) Upload(ctx context.Context, path string, reader io.Reader) error {
	file, err := o.Create(ctx, path)
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

func (o *obsFs) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = o.config.BucketName
	input.Key = path

	output, err := o.client.InitiateMultipartUpload(input)
	if err != nil {
		return "", err
	}

	return output.UploadId, nil
}

func (o *obsFs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	input := &obs.UploadPartInput{
		Bucket:     o.config.BucketName,
		Key:        path,
		PartNumber: partNumber,
		UploadId:   uploadID,
		Body:       data,
	}
	output, err := o.client.UploadPart(input)
	if err != nil {
		return "", err
	}
	return output.ETag, nil
}

func (o *obsFs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	obsParts := make([]obs.Part, len(parts))
	for i, part := range parts {
		obsParts[i] = obs.Part{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}
	input := &obs.CompleteMultipartUploadInput{
		Bucket:   o.config.BucketName,
		Key:      path,
		UploadId: uploadID,
		Parts:    obsParts,
	}
	_, err := o.client.CompleteMultipartUpload(input)
	return err
}

func (o *obsFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	input := &obs.AbortMultipartUploadInput{
		Bucket:   o.config.BucketName,
		Key:      path,
		UploadId: uploadID,
	}

	_, err := o.client.AbortMultipartUpload(input)
	return err
}

func (o *obsFs) ListMultipartUploads(ctx context.Context) ([]fs.MultipartUploadInfo, error) {
	input := &obs.ListMultipartUploadsInput{
		Bucket: o.config.BucketName,
	}
	output, err := o.client.ListMultipartUploads(input)
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

func (o *obsFs) ListUploadedParts(ctx context.Context, path string, uploadID string) ([]fs.MultipartPart, error) {
	input := &obs.ListPartsInput{
		Bucket:   o.config.BucketName,
		Key:      path,
		UploadId: uploadID,
	}
	output, err := o.client.ListParts(input)
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
