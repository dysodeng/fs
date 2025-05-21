package minio

import (
	"bytes"
	"context"
	"io"

	"github.com/dysodeng/fs"
	"github.com/minio/minio-go/v7"
)

func (m *minioFs) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	// 使用底层API创建分片上传
	opts := minio.PutObjectOptions{}
	uploadID, err := m.core.NewMultipartUpload(ctx, m.config.BucketName, path, opts)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

func (m *minioFs) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	// 计算数据大小
	var size int64
	if seeker, ok := data.(io.Seeker); ok {
		var err error
		size, err = seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return "", err
		}
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return "", err
		}
	} else {
		// 如果无法获取大小，则先将数据读入内存
		buf := new(bytes.Buffer)
		var err error
		size, err = io.Copy(buf, data)
		if err != nil {
			return "", err
		}
		data = buf
	}

	// 使用底层API上传分片
	part, err := m.core.PutObjectPart(ctx, m.config.BucketName, path, uploadID, partNumber, data, size, minio.PutObjectPartOptions{})
	if err != nil {
		return "", err
	}

	return part.ETag, nil
}

func (m *minioFs) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	// 转换分片信息格式
	completeParts := make([]minio.CompletePart, len(parts))
	for i, part := range parts {
		completeParts[i] = minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		}
	}

	// 使用底层API完成分片上传
	_, err := m.core.CompleteMultipartUpload(ctx, m.config.BucketName, path, uploadID, completeParts, minio.PutObjectOptions{})
	return err
}

func (m *minioFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	// 使用底层API取消分片上传
	return m.core.AbortMultipartUpload(ctx, m.config.BucketName, path, uploadID)
}
