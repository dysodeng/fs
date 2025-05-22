package minio

import (
	"bytes"
	"context"
	"io"

	"github.com/dysodeng/fs"
	"github.com/minio/minio-go/v7"
)

func (m *minioFs) Uploader() fs.Uploader {
	return m
}

func (m *minioFs) Upload(ctx context.Context, path string, reader io.Reader) error {
	file, err := m.Create(ctx, path)
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

	_, err := m.core.CompleteMultipartUpload(ctx, m.config.BucketName, path, uploadID, completeParts, minio.PutObjectOptions{})
	return err
}

func (m *minioFs) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	return m.core.AbortMultipartUpload(ctx, m.config.BucketName, path, uploadID)
}

func (m *minioFs) ListMultipartUploads(ctx context.Context) ([]fs.MultipartUploadInfo, error) {
	var uploads []fs.MultipartUploadInfo
	for multipart := range m.client.ListIncompleteUploads(ctx, m.config.BucketName, "", true) {
		if multipart.Err != nil {
			return nil, multipart.Err
		}
		uploads = append(uploads, fs.MultipartUploadInfo{
			UploadID:   multipart.UploadID,
			Path:       multipart.Key,
			CreateTime: multipart.Initiated,
		})
	}
	return uploads, nil
}

func (m *minioFs) ListUploadedParts(ctx context.Context, path string, uploadID string) ([]fs.MultipartPart, error) {
	var parts []fs.MultipartPart

	// 初始化参数
	partNumberMarker := 0
	maxParts := 1000 // 每次获取的最大分片数

	for {
		// 获取分片列表
		result, err := m.core.ListObjectParts(ctx, m.config.BucketName, path, uploadID, partNumberMarker, maxParts)
		if err != nil {
			return nil, err
		}

		// 添加分片信息
		for _, part := range result.ObjectParts {
			parts = append(parts, fs.MultipartPart{
				PartNumber: part.PartNumber,
				ETag:       part.ETag,
				Size:       part.Size,
			})
		}

		// 如果所有分片都已获取，退出循环
		if !result.IsTruncated {
			break
		}

		// 更新partNumberMarker，继续获取下一批分片
		partNumberMarker = result.NextPartNumberMarker
	}

	return parts, nil
}
