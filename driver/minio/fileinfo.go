package minio

import (
	"os"
	"time"

	"github.com/minio/minio-go/v7"
)

// minioFileInfo 实现 fs.FileInfo 接口
type minioFileInfo struct {
	info minio.ObjectInfo
}

func newMinioFileInfo(info minio.ObjectInfo) *minioFileInfo {
	return &minioFileInfo{info: info}
}

func (f *minioFileInfo) Name() string {
	return f.info.Key
}

func (f *minioFileInfo) Size() int64 {
	return f.info.Size
}

func (f *minioFileInfo) Mode() os.FileMode {
	return 0644 // MinIO不支持文件权限，返回默认值
}

func (f *minioFileInfo) ModTime() time.Time {
	return f.info.LastModified.Local()
}

func (f *minioFileInfo) IsDir() bool {
	return f.info.Key[len(f.info.Key)-1] == '/'
}

func (f *minioFileInfo) Sys() interface{} {
	return f.info
}
