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
	ContentType string `json:"content_type"`
	// Metadata 文件元数据
	Metadata Metadata `json:"metadata"`
}

// MultipartPart 分片信息
type MultipartPart struct {
	PartNumber int    `json:"part_number"` // 分片号
	ETag       string `json:"etag"`        // 分片ETag
	Size       int64  `json:"size"`        // 分片大小
}

// MultipartUploadInfo 分片上传信息
type MultipartUploadInfo struct {
	UploadID   string          `json:"upload_id"`   // 上传ID
	Path       string          `json:"path"`        // 文件路径
	Parts      []MultipartPart `json:"parts"`       // 已上传的分片
	CreateTime time.Time       `json:"create_time"` // 创建时间
}
