package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dysodeng/fs"
	"github.com/google/uuid"
)

type MultipartUpload struct {
	Path     string         `json:"path"`
	UploadID string         `json:"upload_id"`
	Parts    map[int]string `json:"parts"` // partNumber -> tempFilePath
}

func (localFs *local) InitMultipartUpload(ctx context.Context, path string) (string, error) {
	uploadID := uuid.New().String()
	upload := &MultipartUpload{
		Path:     path,
		UploadID: uploadID,
		Parts:    make(map[int]string),
	}
	if err := localFs.multipartStorage.Save(upload); err != nil {
		return "", err
	}
	return uploadID, nil
}

func (localFs *local) UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error) {
	upload, err := localFs.multipartStorage.Get(uploadID)
	if err != nil {
		return "", err
	}

	// 创建临时文件存储分片
	tempFile, err := os.CreateTemp("", fmt.Sprintf("part-%d-*", partNumber))
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	// 写入分片数据
	if _, err := io.Copy(tempFile, data); err != nil {
		_ = os.Remove(tempFile.Name())
		return "", err
	}

	upload.Parts[partNumber] = tempFile.Name()
	if err := localFs.multipartStorage.Save(upload); err != nil {
		_ = os.Remove(tempFile.Name())
		return "", err
	}
	return tempFile.Name(), nil
}

func (localFs *local) CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []fs.MultipartPart) error {
	upload, err := localFs.multipartStorage.Get(uploadID)
	if err != nil {
		return err
	}
	defer func() {
		_ = localFs.multipartStorage.Delete(uploadID)
	}()

	// 创建目标文件
	fullPath := localFs.fullPath(path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	destFile, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = destFile.Close()
	}()

	// 按顺序合并分片
	for _, part := range parts {
		tempPath, ok := upload.Parts[part.PartNumber]
		if !ok {
			return fmt.Errorf("part %d not found", part.PartNumber)
		}

		// 读取分片数据
		tempFile, err := os.Open(tempPath)
		if err != nil {
			return err
		}

		// 写入目标文件
		if _, err := io.Copy(destFile, tempFile); err != nil {
			_ = tempFile.Close()
			return err
		}
		tempFile.Close()

		// 删除临时文件
		_ = os.Remove(tempPath)
	}

	return nil
}

func (localFs *local) AbortMultipartUpload(ctx context.Context, path string, uploadID string) error {
	upload, err := localFs.multipartStorage.Get(uploadID)
	if err != nil {
		return nil
	}

	// 删除所有临时文件
	for _, tempPath := range upload.Parts {
		_ = os.Remove(tempPath)
	}

	return localFs.multipartStorage.Delete(uploadID)
}
