package txcos

import (
	"context"
	"io"

	"github.com/dysodeng/fs"
	"github.com/tencentyun/cos-go-sdk-v5"
)

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
