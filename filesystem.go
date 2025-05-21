package fs

import (
	"context"
	"io"
	"os"
)

// FileSystem 文件系统接口
type FileSystem interface {
	// List 列出目录内容
	List(ctx context.Context, path string) ([]FileInfo, error)
	// MakeDir 创建目录
	MakeDir(ctx context.Context, path string, perm os.FileMode) error
	// RemoveDir 删除目录
	RemoveDir(ctx context.Context, path string) error

	// Create 创建文件并返回io.WriteCloser
	Create(ctx context.Context, path string) (io.WriteCloser, error)
	// CreateWithMetadata 创建文件并返回io.WriteCloser
	CreateWithMetadata(ctx context.Context, path string, metadata Metadata) (io.WriteCloser, error)
	// CreateWithOptions 创建文件并设置选项
	CreateWithOptions(ctx context.Context, path string, options CreateOptions) (io.WriteCloser, error)
	// Open 打开文件并返回io.ReadCloser
	Open(ctx context.Context, path string) (io.ReadCloser, error)
	// OpenFile 以指定模式打开文件
	OpenFile(ctx context.Context, path string, flag int, perm os.FileMode) (io.ReadWriteCloser, error)
	// Remove 删除文件
	Remove(ctx context.Context, path string) error
	// Copy 复制文件
	Copy(ctx context.Context, src, dst string) error
	// Move 移动文件
	Move(ctx context.Context, src, dst string) error
	// Rename 重命名文件或目录
	Rename(ctx context.Context, oldPath, newPath string) error

	// Stat 获取文件/目录信息
	Stat(ctx context.Context, path string) (FileInfo, error)
	// GetMimeType 获取文件的 MIME 类型
	GetMimeType(ctx context.Context, path string) (string, error)
	// SetMetadata 设置元数据
	SetMetadata(ctx context.Context, path string, metadata map[string]interface{}) error
	// GetMetadata 获取元数据
	GetMetadata(ctx context.Context, path string) (map[string]interface{}, error)

	// Exists 判断文件或目录是否存在
	Exists(ctx context.Context, path string) (bool, error)
	// IsDir 判断是否为目录
	IsDir(ctx context.Context, path string) (bool, error)
	// IsFile 判断是否为文件
	IsFile(ctx context.Context, path string) (bool, error)

	// InitMultipartUpload 初始化分片上传
	InitMultipartUpload(ctx context.Context, path string) (string, error)
	// UploadPart 上传分片
	UploadPart(ctx context.Context, path string, uploadID string, partNumber int, data io.Reader) (string, error)
	// CompleteMultipartUpload 完成分片上传
	CompleteMultipartUpload(ctx context.Context, path string, uploadID string, parts []MultipartPart) error
	// AbortMultipartUpload 取消分片上传
	AbortMultipartUpload(ctx context.Context, path string, uploadID string) error
	// ListMultipartUploads 列出所有未完成的分片上传
	ListMultipartUploads(ctx context.Context) ([]MultipartUploadInfo, error)
	// ListUploadedParts 列出已上传的分片
	ListUploadedParts(ctx context.Context, path string, uploadID string) ([]MultipartPart, error)
}
