package fs

import (
	"os"
	"time"
)

// FileInfo 文件信息，实现 os.FileInfo 接口
type FileInfo interface {
	Name() string
	Size() int64
	Mode() os.FileMode
	ModTime() time.Time
	IsDir() bool
	Sys() interface{}
}

// Metadata 文件元数据
type Metadata map[string]interface{}

// CreateOptions 文件创建选项
type CreateOptions struct {
	// ContentType 文件内容类型
	ContentType string
	// Metadata 文件元数据
	Metadata Metadata
}

// MultipartPart 分片信息
type MultipartPart struct {
	PartNumber int    // 分片号
	ETag       string // 分片ETag
}
