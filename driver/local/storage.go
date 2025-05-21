package local

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type MultipartStorage interface {
	// Save 保存分片上传状态
	Save(upload *MultipartUpload) error
	// Get 获取分片上传状态
	Get(uploadID string) (*MultipartUpload, error)
	// Delete 删除分片上传状态
	Delete(uploadID string) error
	// List 列出所有未完成的分片上传
	List() ([]*MultipartUpload, error)
}

// FileMultipartStorage 文件系统实现的状态存储
type FileMultipartStorage struct {
	storageDir string // 状态文件存储目录
}

func NewFileMultipartStorage(storageDir string) (*FileMultipartStorage, error) {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, err
	}
	return &FileMultipartStorage{storageDir: storageDir}, nil
}

func (s *FileMultipartStorage) getFilePath(uploadID string) string {
	return filepath.Join(s.storageDir, uploadID+".json")
}

func (s *FileMultipartStorage) Save(upload *MultipartUpload) error {
	data, err := json.Marshal(*upload)
	if err != nil {
		return err
	}
	return os.WriteFile(s.getFilePath(upload.UploadID), data, 0644)
}

func (s *FileMultipartStorage) Get(uploadID string) (*MultipartUpload, error) {
	data, err := os.ReadFile(s.getFilePath(uploadID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("upload ID not found")
		}
		return nil, err
	}

	upload := &MultipartUpload{}
	if err := json.Unmarshal(data, upload); err != nil {
		return nil, err
	}
	return upload, nil
}

func (s *FileMultipartStorage) Delete(uploadID string) error {
	return os.Remove(s.getFilePath(uploadID))
}

func (s *FileMultipartStorage) List() ([]*MultipartUpload, error) {
	files, err := os.ReadDir(s.storageDir)
	if err != nil {
		return nil, err
	}

	var uploads []*MultipartUpload
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			uploadID := strings.TrimSuffix(file.Name(), ".json")
			if upload, err := s.Get(uploadID); err == nil {
				uploads = append(uploads, upload)
			}
		}
	}
	return uploads, nil
}
