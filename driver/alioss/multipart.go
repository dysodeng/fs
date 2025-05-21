package alioss

import (
	"bytes"
	"context"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dysodeng/fs"
)

func (o *ossFs) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	initMultipartUpload, err := o.bucket.InitiateMultipartUpload(path, oss.WithContext(ctx))
	if err != nil {
		return "", err
	}
	return initMultipartUpload.UploadID, nil
}

func (o *ossFs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	initMultipartUpload := oss.InitiateMultipartUploadResult{
		UploadID: uploadID,
		Key:      path,
		Bucket:   o.bucket.BucketName,
	}

	// 获取reader的大小
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

	part, err := o.bucket.UploadPart(initMultipartUpload, data, partSize, partNumber, oss.WithContext(ctx))
	if err != nil {
		return "", err
	}

	return part.ETag, nil
}

func (o *ossFs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	initMultipartUpload := oss.InitiateMultipartUploadResult{
		UploadID: uploadID,
		Key:      path,
		Bucket:   o.bucket.BucketName,
	}
	ossParts := make([]oss.UploadPart, len(parts))
	for i, part := range parts {
		ossParts[i] = oss.UploadPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}
	_, err := o.bucket.CompleteMultipartUpload(initMultipartUpload, ossParts, oss.WithContext(ctx))
	return err
}

func (o *ossFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	initMultipartUpload := oss.InitiateMultipartUploadResult{
		UploadID: uploadID,
		Key:      path,
		Bucket:   o.bucket.BucketName,
	}
	return o.bucket.AbortMultipartUpload(initMultipartUpload, oss.WithContext(ctx))
}
