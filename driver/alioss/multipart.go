package alioss

import (
	"bytes"
	"context"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dysodeng/fs"
)

func (o *ossFs) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	initMultipartUploadResult, err := o.bucket.InitiateMultipartUpload(path, oss.WithContext(ctx))
	if err != nil {
		return "", err
	}
	return initMultipartUploadResult.UploadID, nil
}

func (o *ossFs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	initMultipartUploadResult := oss.InitiateMultipartUploadResult{
		Key:      path,
		UploadID: uploadID,
		Bucket:   o.bucket.BucketName,
	}

	// 获取数据大小
	var partSize int64
	if seeker, ok := data.(io.Seeker); ok {
		size, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return "", err
		}
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return "", err
		}
		partSize = size
	} else {
		// 如果无法获取大小，则先将数据读入内存
		buf := new(bytes.Buffer)
		size, err := io.Copy(buf, data)
		if err != nil {
			return "", err
		}
		data = buf
		partSize = size
	}

	part, err := o.bucket.UploadPart(initMultipartUploadResult, data, partSize, partNumber, oss.WithContext(ctx))
	if err != nil {
		return "", err
	}

	return part.ETag, nil
}

func (o *ossFs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	initMultipartUploadResult := oss.InitiateMultipartUploadResult{
		Key:      path,
		UploadID: uploadID,
		Bucket:   o.bucket.BucketName,
	}
	ossParts := make([]oss.UploadPart, len(parts))
	for i, part := range parts {
		ossParts[i] = oss.UploadPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}
	_, err := o.bucket.CompleteMultipartUpload(initMultipartUploadResult, ossParts, oss.WithContext(ctx))
	return err
}

func (o *ossFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	initMultipartUploadResult := oss.InitiateMultipartUploadResult{
		Key:      path,
		UploadID: uploadID,
		Bucket:   o.bucket.BucketName,
	}
	return o.bucket.AbortMultipartUpload(initMultipartUploadResult, oss.WithContext(ctx))
}

func (o *ossFs) ListMultipartUploads(ctx context.Context) ([]fs.MultipartUploadInfo, error) {
	initMultipartUploadResult, err := o.bucket.ListMultipartUploads(oss.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	uploads := make([]fs.MultipartUploadInfo, 0, len(initMultipartUploadResult.Uploads))
	for _, upload := range initMultipartUploadResult.Uploads {
		uploads = append(uploads, fs.MultipartUploadInfo{
			UploadID:   upload.UploadID,
			Path:       upload.Key,
			CreateTime: upload.Initiated,
		})
	}
	return uploads, nil
}

func (o *ossFs) ListUploadedParts(ctx context.Context, path string, uploadID string) ([]fs.MultipartPart, error) {
	initMultipartUploadResult := oss.InitiateMultipartUploadResult{
		Key:      path,
		UploadID: uploadID,
		Bucket:   o.bucket.BucketName,
	}

	// 列出已上传的分片
	lpr, err := o.bucket.ListUploadedParts(initMultipartUploadResult, oss.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	parts := make([]fs.MultipartPart, 0, len(lpr.UploadedParts))
	for _, part := range lpr.UploadedParts {
		parts = append(parts, fs.MultipartPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
			Size:       int64(part.Size),
		})
	}
	return parts, nil
}
