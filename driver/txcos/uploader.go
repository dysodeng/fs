package txcos

import (
	"context"
	"io"
	"time"

	"github.com/dysodeng/fs"
	"github.com/tencentyun/cos-go-sdk-v5"
)

func (c *cosFs) Uploader() fs.Uploader {
	return c
}

func (c *cosFs) Upload(ctx context.Context, path string, reader io.Reader) error {
	file, err := c.Create(ctx, path)
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

func (c *cosFs) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	res, _, err := c.client.Object.InitiateMultipartUpload(ctx, path, nil)
	if err != nil {
		return "", err
	}
	return res.UploadID, nil
}

func (c *cosFs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	res, err := c.client.Object.UploadPart(ctx, path, uploadID, partNumber, data, nil)
	if err != nil {
		return "", err
	}
	return res.Header.Get("ETag"), nil
}

func (c *cosFs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	opt := &cos.CompleteMultipartUploadOptions{
		Parts: make([]cos.Object, len(parts)),
	}
	for i, part := range parts {
		opt.Parts[i] = cos.Object{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}
	_, _, err := c.client.Object.CompleteMultipartUpload(ctx, path, uploadID, opt)
	return err
}

func (c *cosFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	_, err := c.client.Object.AbortMultipartUpload(ctx, path, uploadID)
	return err
}

func (c *cosFs) ListMultipartUploads(ctx context.Context) ([]fs.MultipartUploadInfo, error) {
	opt := &cos.ListMultipartUploadsOptions{}
	v, _, err := c.client.Bucket.ListMultipartUploads(ctx, opt)
	if err != nil {
		return nil, err
	}

	result := make([]fs.MultipartUploadInfo, len(v.Uploads))
	for i, upload := range v.Uploads {
		createTime, _ := time.Parse(time.RFC3339, upload.Initiated)
		result[i] = fs.MultipartUploadInfo{
			UploadID:   upload.UploadID,
			Path:       upload.Key,
			CreateTime: createTime,
		}
	}
	return result, nil
}

func (c *cosFs) ListUploadedParts(ctx context.Context, path string, uploadID string) ([]fs.MultipartPart, error) {
	opt := &cos.ObjectListPartsOptions{}
	v, _, err := c.client.Object.ListParts(ctx, path, uploadID, opt)
	if err != nil {
		return nil, err
	}

	parts := make([]fs.MultipartPart, len(v.Parts))
	for i, part := range v.Parts {
		parts[i] = fs.MultipartPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
			Size:       part.Size,
		}
	}
	return parts, nil
}
